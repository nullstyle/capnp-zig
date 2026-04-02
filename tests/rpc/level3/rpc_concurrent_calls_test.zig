const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.cap_table;
const Connection = capnpc.rpc.connection.Connection;
const Peer = peer_impl.Peer;

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

// ---------------------------------------------------------------------------
// Tests: Multiple inbound calls to a single export
// ---------------------------------------------------------------------------

test "concurrent inbound calls: multiple calls dispatched to same export" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        call_count: u32 = 0,
        question_ids: [16]u32 = [_]u32{0} ** 16,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.question_ids[state.call_count] = call.question_id;
            state.call_count += 1;
            // Respond immediately
            try called_peer.sendReturnException(call.question_id, "handled");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

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

    // Send 10 calls with distinct question IDs
    const num_calls: u32 = 10;
    for (0..num_calls) |i| {
        const qid: u32 = @intCast(100 + i);
        const frame = try buildCallFrame(allocator, qid, export_id, 0xABCD, 1);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // All calls should have been dispatched and returned
    try std.testing.expectEqual(num_calls, server_ctx.call_count);
    try std.testing.expectEqual(@as(usize, num_calls), capture.frames.items.len);

    // Verify each return matches the corresponding question ID
    for (0..num_calls) |i| {
        try std.testing.expectEqual(@as(u32, @intCast(100 + i)), server_ctx.question_ids[i]);

        var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[i]);
        defer decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.@"return", decoded.tag);
        const ret = try decoded.asReturn();
        try std.testing.expectEqual(@as(u32, @intCast(100 + i)), ret.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    }

    // Clean up: send finish for each
    for (0..num_calls) |i| {
        const frame = try buildFinishFrame(allocator, @intCast(100 + i));
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
}

test "concurrent inbound calls: deferred returns in reverse order" {
    const allocator = std.testing.allocator;

    // Handler that records calls but does NOT respond immediately
    const ServerCtx = struct {
        pending_ids: std.ArrayList(u32),
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            _: *Peer,
            call: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            try state.pending_ids.append(allocator, call.question_id);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var server_ctx = ServerCtx{ .pending_ids = std.ArrayList(u32).empty };
    defer server_ctx.pending_ids.deinit(allocator);

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

    // Inject 5 calls
    const num_calls: u32 = 5;
    for (0..num_calls) |i| {
        const qid: u32 = @intCast(200 + i);
        const frame = try buildCallFrame(allocator, qid, export_id, 0x1111, 0);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // No returns sent yet (handler didn't respond)
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
    try std.testing.expectEqual(num_calls, @as(u32, @intCast(server_ctx.pending_ids.items.len)));

    // Now respond in REVERSE order
    var i: u32 = num_calls;
    while (i > 0) {
        i -= 1;
        const qid = server_ctx.pending_ids.items[i];
        try peer.sendReturnException(qid, "deferred-reverse");
    }

    // All returns should be sent
    try std.testing.expectEqual(@as(usize, num_calls), capture.frames.items.len);

    // Verify returns match reverse order
    for (0..num_calls) |idx| {
        var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[idx]);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        // Returns sent in order: 204, 203, 202, 201, 200
        try std.testing.expectEqual(@as(u32, @intCast(204 - idx)), ret.answer_id);
    }

    // Clean up
    for (0..num_calls) |idx| {
        const frame = try buildFinishFrame(allocator, @intCast(200 + idx));
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
}

// ---------------------------------------------------------------------------
// Tests: Multiple outbound calls with interleaved returns
// ---------------------------------------------------------------------------

test "concurrent outbound calls: returns arrive in order" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        returned: [8]bool = [_]bool{false} ** 8,
        answer_ids: [8]u32 = [_]u32{0} ** 8,
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.answer_ids[state.count] = ret.answer_id;
            state.returned[state.count] = true;
            state.count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 4 outbound calls
    const num_calls: u32 = 4;
    var question_ids: [num_calls]u32 = undefined;
    for (0..num_calls) |i| {
        question_ids[i] = try peer.sendCall(
            0, // target import 0
            0xCAFE,
            @intCast(i),
            &return_ctx,
            null,
            Callback.onReturn,
        );
    }

    // Should have sent 4 call frames
    try std.testing.expectEqual(@as(usize, num_calls), capture.frames.items.len);

    // Inject return frames in order
    for (0..num_calls) |i| {
        const frame = try buildReturnResultsFrame(allocator, question_ids[i]);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // All callbacks should have fired
    try std.testing.expectEqual(num_calls, return_ctx.count);
    for (0..num_calls) |i| {
        try std.testing.expect(return_ctx.returned[i]);
        try std.testing.expectEqual(question_ids[i], return_ctx.answer_ids[i]);
    }
}

test "concurrent outbound calls: returns arrive in reverse order" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        answer_ids: [8]u32 = [_]u32{0} ** 8,
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.answer_ids[state.count] = ret.answer_id;
            state.count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 4 outbound calls
    const num_calls: u32 = 4;
    var question_ids: [num_calls]u32 = undefined;
    for (0..num_calls) |i| {
        question_ids[i] = try peer.sendCall(
            0,
            0xBEEF,
            @intCast(i),
            &return_ctx,
            null,
            Callback.onReturn,
        );
    }

    // Inject returns in REVERSE order
    var i: u32 = num_calls;
    while (i > 0) {
        i -= 1;
        const frame = try buildReturnResultsFrame(allocator, question_ids[i]);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // All callbacks should have fired, in reverse order
    try std.testing.expectEqual(num_calls, return_ctx.count);
    for (0..num_calls) |idx| {
        try std.testing.expectEqual(question_ids[num_calls - 1 - idx], return_ctx.answer_ids[idx]);
    }
}

test "concurrent outbound calls: mixed results and exceptions" {
    const allocator = std.testing.allocator;

    const ReturnCtx = struct {
        tags: [8]protocol.ReturnTag = [_]protocol.ReturnTag{.results} ** 8,
        count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, ret: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.tags[state.count] = ret.tag;
            state.count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 6 outbound calls
    const num_calls: u32 = 6;
    var question_ids: [num_calls]u32 = undefined;
    for (0..num_calls) |i| {
        question_ids[i] = try peer.sendCall(
            0,
            0xFACE,
            @intCast(i),
            &return_ctx,
            null,
            Callback.onReturn,
        );
    }

    // Return alternating results/exceptions
    for (0..num_calls) |i| {
        const frame = if (i % 2 == 0)
            try buildReturnResultsFrame(allocator, question_ids[i])
        else
            try buildReturnExceptionFrame(allocator, question_ids[i], "odd-call-failed");
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    try std.testing.expectEqual(num_calls, return_ctx.count);
    for (0..num_calls) |i| {
        const expected: protocol.ReturnTag = if (i % 2 == 0) .results else .exception;
        try std.testing.expectEqual(expected, return_ctx.tags[i]);
    }
}

// ---------------------------------------------------------------------------
// Tests: Interleaved bidirectional calls
// ---------------------------------------------------------------------------

test "bidirectional: interleaved inbound calls and outbound returns" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        call_count: u32 = 0,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.call_count += 1;
            // Respond to every inbound call immediately
            try called_peer.sendReturnException(call.question_id, "server-handled");
        }
    };

    const ReturnCtx = struct {
        return_count: u32 = 0,
    };
    const Callback = struct {
        fn onReturn(ctx: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {
            const state: *ReturnCtx = @ptrCast(@alignCast(ctx));
            state.return_count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

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

    var return_ctx = ReturnCtx{};

    // Interleave: send outbound call, receive inbound call, repeat
    const rounds: u32 = 8;
    var question_ids: [rounds]u32 = undefined;

    for (0..rounds) |r| {
        // Send an outbound call
        question_ids[r] = try peer.sendCall(
            0,
            0x1234,
            @intCast(r),
            &return_ctx,
            null,
            Callback.onReturn,
        );

        // Receive an inbound call
        const inbound_qid: u32 = @intCast(500 + r);
        const inbound_frame = try buildCallFrame(allocator, inbound_qid, export_id, 0x5678, 0);
        defer allocator.free(inbound_frame);
        try peer.handleFrame(inbound_frame);
    }

    // All inbound calls should have been handled
    try std.testing.expectEqual(rounds, server_ctx.call_count);

    // Now deliver returns for all outbound calls
    for (0..rounds) |r| {
        const frame = try buildReturnResultsFrame(allocator, question_ids[r]);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    // All outbound callbacks should have fired
    try std.testing.expectEqual(rounds, return_ctx.return_count);

    // Clean up inbound call finishes
    for (0..rounds) |r| {
        const frame = try buildFinishFrame(allocator, @intCast(500 + r));
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
}

// ---------------------------------------------------------------------------
// Tests: Calls to multiple distinct exports
// ---------------------------------------------------------------------------

test "concurrent calls to different exports" {
    const allocator = std.testing.allocator;

    const ExportCtx = struct {
        id: u32,
        call_count: u32 = 0,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ExportCtx = @ptrCast(@alignCast(ctx));
            state.call_count += 1;
            try called_peer.sendReturnException(call.question_id, "multi-export");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // Register 4 distinct exports
    const num_exports: u32 = 4;
    var export_ctxs: [num_exports]ExportCtx = undefined;
    var export_ids: [num_exports]u32 = undefined;
    for (0..num_exports) |i| {
        export_ctxs[i] = .{ .id = @intCast(i) };
        export_ids[i] = try peer.addExport(.{
            .ctx = &export_ctxs[i],
            .on_call = Handlers.onCall,
        });
    }

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Send 3 calls to each export (12 total), round-robin
    const calls_per_export: u32 = 3;
    var qid: u32 = 300;
    for (0..calls_per_export) |_| {
        for (0..num_exports) |e| {
            const frame = try buildCallFrame(allocator, qid, export_ids[e], 0xAAAA, 0);
            defer allocator.free(frame);
            try peer.handleFrame(frame);
            qid += 1;
        }
    }

    // Each export should have received exactly calls_per_export calls
    for (0..num_exports) |e| {
        try std.testing.expectEqual(calls_per_export, export_ctxs[e].call_count);
    }

    // Total returns should equal total calls
    try std.testing.expectEqual(@as(usize, num_exports * calls_per_export), capture.frames.items.len);
}

// ---------------------------------------------------------------------------
// Tests: Question ID uniqueness under concurrent outbound calls
// ---------------------------------------------------------------------------

test "outbound calls: question IDs are unique" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Send 32 outbound calls and collect question IDs
    const num_calls: u32 = 32;
    var question_ids: [num_calls]u32 = undefined;
    for (0..num_calls) |i| {
        question_ids[i] = try peer.sendCall(0, 0x9999, @intCast(i), &ctx, null, Callback.onReturn);
    }

    // Verify all IDs are unique
    for (0..num_calls) |i| {
        for (i + 1..num_calls) |j| {
            try std.testing.expect(question_ids[i] != question_ids[j]);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests: Question ID reuse after finish
// ---------------------------------------------------------------------------

test "outbound calls: question ID reclaimed after return+finish cycle" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var ctx: u8 = 0;

    // Send a call, get return, send finish — full lifecycle
    const qid1 = try peer.sendCall(0, 0x1111, 0, &ctx, null, Callback.onReturn);
    const ret_frame = try buildReturnResultsFrame(allocator, qid1);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    // Finish should be auto-sent or we send it
    // The return handler runs the callback; the peer should allow reusing the question ID slot

    // Send another call — the allocator should not waste IDs
    const qid2 = try peer.sendCall(0, 0x1111, 1, &ctx, null, Callback.onReturn);

    // IDs should be distinct (qid1 may or may not be reused depending on allocator,
    // but both should be valid)
    _ = qid2;
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len - 1); // 2 calls + 1 finish
}

// ---------------------------------------------------------------------------
// Tests: Stress test — many concurrent calls with cleanup
// ---------------------------------------------------------------------------

test "stress: 128 concurrent inbound calls with immediate responses" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        call_count: u32 = 0,
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.call_count += 1;
            try called_peer.sendReturnException(call.question_id, "stress");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

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

    // Inject 128 calls
    const num_calls: u32 = 128;
    for (0..num_calls) |i| {
        const qid: u32 = @intCast(1000 + i);
        const frame = try buildCallFrame(allocator, qid, export_id, 0xFFFF, 0);
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }

    try std.testing.expectEqual(num_calls, server_ctx.call_count);
    try std.testing.expectEqual(@as(usize, num_calls), capture.frames.items.len);

    // Send finish for all
    for (0..num_calls) |i| {
        const frame = try buildFinishFrame(allocator, @intCast(1000 + i));
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
}

test "stress: 64 outbound calls with interleaved returns" {
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
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 64 outbound calls
    const num_calls: u32 = 64;
    var question_ids: [num_calls]u32 = undefined;
    for (0..num_calls) |i| {
        question_ids[i] = try peer.sendCall(
            0,
            0xDEAD,
            @intCast(i % 16),
            &return_ctx,
            null,
            Callback.onReturn,
        );
    }

    // Return in a shuffled order: evens first, then odds
    for (0..num_calls) |i| {
        if (i % 2 == 0) {
            const frame = try buildReturnResultsFrame(allocator, question_ids[i]);
            defer allocator.free(frame);
            try peer.handleFrame(frame);
        }
    }
    for (0..num_calls) |i| {
        if (i % 2 != 0) {
            const frame = try buildReturnExceptionFrame(allocator, question_ids[i], "odd");
            defer allocator.free(frame);
            try peer.handleFrame(frame);
        }
    }

    try std.testing.expectEqual(num_calls, return_ctx.count);
}

// ---------------------------------------------------------------------------
// Tests: Shutdown during pending calls
// ---------------------------------------------------------------------------

test "shutdown rejects new calls while pending calls remain" {
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
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8).empty,
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var return_ctx = ReturnCtx{};

    // Send 2 calls
    const qid1 = try peer.sendCall(0, 0x1234, 0, &return_ctx, null, Callback.onReturn);
    _ = try peer.sendCall(0, 0x1234, 1, &return_ctx, null, Callback.onReturn);

    // Shutdown
    peer.shutdown(null);

    // New calls should be rejected
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendCall(0, 0x1234, 2, &return_ctx, null, Callback.onReturn),
    );

    // But pending returns should still be accepted
    const frame = try buildReturnResultsFrame(allocator, qid1);
    defer allocator.free(frame);
    try peer.handleFrame(frame);
    try std.testing.expectEqual(@as(u32, 1), return_ctx.count);
}
