const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const protocol = capnpc.rpc.protocol;
const peer_impl = capnpc.rpc.peer;
const cap_table = capnpc.rpc.cap_table;
const Connection = capnpc.rpc.connection.Connection;
const Peer = peer_impl.Peer;

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

const NoopHandler = struct {
    fn onCall(
        ctx: *anyopaque,
        called_peer: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = ctx;
        _ = called_peer;
        _ = call;
        _ = inbound_caps;
    }
};

// ---------------------------------------------------------------------------
// Release semantics tests at the Peer level
// ---------------------------------------------------------------------------

test "peer release message removes exported capability" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    // Simulate the remote side receiving the capability (bump ref_count).
    var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
    entry.value_ptr.ref_count = 1;

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Send a Release message for the export.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(export_id, 1);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // The export should be removed since ref_count was 1.
    try std.testing.expect(!peer.exports.contains(export_id));
}

test "peer release with ref_count > 1 only decrements, does not remove" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    // Set ref_count to 3.
    var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
    entry.value_ptr.ref_count = 3;

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Release 2 of the 3 refs.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(export_id, 2);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // The export should still exist with ref_count = 1.
    try std.testing.expect(peer.exports.contains(export_id));
    const updated = peer.exports.get(export_id) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 1), updated.ref_count);
}

test "peer release for unknown export id is handled gracefully" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Release an export that was never added. Should not crash or error.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(12345, 1);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // No abort or error frame should be sent.
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "peer release for already-released export is handled gracefully" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
    entry.value_ptr.ref_count = 1;

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // First release removes the export.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildRelease(export_id, 1);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    try std.testing.expect(!peer.exports.contains(export_id));

    // Second release for the same ID should be no-op.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildRelease(export_id, 1);
        const frame = try builder.finish();
        defer allocator.free(frame);
        try peer.handleFrame(frame);
    }
    // No error frames sent.
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
}

test "peer release with zero reference count is a no-op" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
    entry.value_ptr.ref_count = 3;

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(export_id, 0);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // Export should still be present with same ref_count.
    try std.testing.expect(peer.exports.contains(export_id));
    const updated = peer.exports.get(export_id) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 3), updated.ref_count);
}

test "peer bootstrap export is not removed by release" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.setBootstrap(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    // Simulate bootstrap being sent (ref_count goes up).
    var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
    entry.value_ptr.ref_count = 2;

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Release all refs for the bootstrap export.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(export_id, 2);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // The bootstrap export should NOT be removed (special case).
    try std.testing.expect(peer.exports.contains(export_id));
    const updated = peer.exports.get(export_id) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 0), updated.ref_count);
}

// ---------------------------------------------------------------------------
// Failure injection tests: call returns exception
// ---------------------------------------------------------------------------

test "peer call to exported capability returns exception" {
    const allocator = std.testing.allocator;

    const ExceptionHandler = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = inbound_caps;
            try called_peer.sendReturnException(call.question_id, "intentional failure");
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = ExceptionHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Send a call targeting the exported capability.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(1, 0xABCD, 0);
    try call.setTargetImportedCap(export_id);
    try call.setEmptyCapTable();
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // Should get a return with exception.
    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 1), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("intentional failure", ex.reason);
}

// ---------------------------------------------------------------------------
// Failure injection: handler returns error (not explicit exception)
// ---------------------------------------------------------------------------

test "peer call handler error is reported as exception return" {
    const allocator = std.testing.allocator;

    const ErrorHandler = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = inbound_caps;
            return error.TestExpectedError;
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = ErrorHandler.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(2, 0xBEEF, 0);
    try call.setTargetImportedCap(export_id);
    try call.setEmptyCapTable();
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // The error from the handler should be surfaced as a return exception.
    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
}

// ---------------------------------------------------------------------------
// Failure injection: malformed RPC messages
// ---------------------------------------------------------------------------

test "peer handleFrame with invalid message tag sends unimplemented" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Build a message with an invalid discriminant.
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xFFFF);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    try peer.handleFrame(bytes);

    // The peer should have responded with an unimplemented message.
    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.unimplemented, decoded.tag);
}

test "peer handleFrame with obsolete_save tag sends unimplemented" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Build a message with obsolete_save discriminant (7).
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, @intFromEnum(protocol.MessageTag.obsolete_save));
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    try peer.handleFrame(bytes);

    // The peer should respond with unimplemented.
    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.unimplemented, decoded.tag);
}

test "peer handleFrame abort updates state and returns error" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort("connection-failed");
    const frame = try builder.finish();
    defer allocator.free(frame);

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
    try std.testing.expectEqual(protocol.MessageTag.abort, peer.getLastInboundTag().?);
    try std.testing.expectEqualStrings("connection-failed", peer.getLastRemoteAbortReason().?);
}

test "peer repeated aborts update the reason string without leaking" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    // First abort.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAbort("first-error");
        const frame = try builder.finish();
        defer allocator.free(frame);
        try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
    }
    try std.testing.expectEqualStrings("first-error", peer.getLastRemoteAbortReason().?);

    // Second abort replaces the first.
    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();
        try builder.buildAbort("second-error");
        const frame = try builder.finish();
        defer allocator.free(frame);
        try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
    }
    try std.testing.expectEqualStrings("second-error", peer.getLastRemoteAbortReason().?);
}

// ---------------------------------------------------------------------------
// Failure injection: call to unknown export returns exception
// ---------------------------------------------------------------------------

test "peer call to unknown export returns exception" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    // Call a non-existent export.
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(3, 0x1111, 0);
    try call.setTargetImportedCap(99999);
    try call.setEmptyCapTable();
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    // Should get an exception return.
    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 3), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
}

// ---------------------------------------------------------------------------
// Failure injection: bootstrap not configured
// ---------------------------------------------------------------------------

test "peer bootstrap without configured bootstrap returns exception" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildBootstrap(1);
    const frame = try builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);

    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 1), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("bootstrap not configured", ex.reason);
}

// ---------------------------------------------------------------------------
// Failure injection: shutdown rejects new calls
// ---------------------------------------------------------------------------

test "peer shutdown rejects new outbound calls" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    peer.shutdown(null);

    var ctx: u8 = 0;
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendCallResolved(.{ .imported = .{ .id = 1 } }, 0x1234, 0, &ctx, null, Callback.onReturn),
    );
}

test "peer shutdown rejects new bootstrap requests" {
    const allocator = std.testing.allocator;

    const Callback = struct {
        fn onReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    peer.shutdown(null);

    var ctx: u8 = 0;
    try std.testing.expectError(
        error.PeerShuttingDown,
        peer.sendBootstrap(&ctx, Callback.onReturn),
    );
}

// ---------------------------------------------------------------------------
// Failure injection: finish for unknown question is handled gracefully
// ---------------------------------------------------------------------------

test "peer finish for never-sent question does not crash" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildFinish(99999, true, false);
    const frame = try builder.finish();
    defer allocator.free(frame);

    // Should not crash. The peer simply has no state to clean up.
    try peer.handleFrame(frame);
}

// ---------------------------------------------------------------------------
// Failure injection: return exception for unknown question
// ---------------------------------------------------------------------------

test "peer sendReturnException for answer creates and sends frame" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer capture.deinit();
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    try peer.sendReturnException(42, "deliberate-error");

    try std.testing.expect(capture.frames.items.len >= 1);
    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, decoded.tag);
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 42), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("deliberate-error", ex.reason);
}
