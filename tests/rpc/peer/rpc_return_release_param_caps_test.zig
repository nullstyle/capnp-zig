const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const peer_impl = capnpc.rpc.peer;
const Peer = peer_impl.Peer;

// ---------------------------------------------------------------------------
// Inbound Return.releaseParamCaps consumption (rpc.capnp defaults it to TRUE:
// the receiver of our call implicitly releases the wire refs our param caps
// took on our exports, instead of sending explicit Release messages).
//
// These tests drive a detached peer: outbound frames are swallowed by a
// send-frame override, inbound Return/Release frames are hand-built with
// protocol.MessageBuilder and injected via handleFrame.
// ---------------------------------------------------------------------------

fn noopSend(_: *anyopaque, _: []const u8) anyerror!void {}
fn noopReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}
fn noopCall(_: *anyopaque, _: *Peer, _: protocol.Call, _: *const cap_table.InboundCapTable) anyerror!void {}

/// Build fn placing one bare capability pointer (a local export) in params;
/// the outbound encoder classifies it as senderHosted and bumps the export's
/// wire ref via onOutboundCap.
const OneCapBuild = struct {
    export_id: u32,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const ctx: *const OneCapBuild = @ptrCast(@alignCast(ctx_ptr));
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        try any.setCapability(.{ .id = ctx.export_id });
    }
};

/// Build fn placing two capability pointers in one params payload.
const TwoCapBuild = struct {
    first_id: u32,
    second_id: u32,

    fn build(ctx_ptr: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
        const ctx: *const TwoCapBuild = @ptrCast(@alignCast(ctx_ptr));
        var payload = try call.payloadTyped();
        var any = try payload.initContent();
        const holder = try any.initStruct(0, 1);
        var caps_list = try holder.writePointerList(0, 2);
        try caps_list.setCapability(0, .{ .id = ctx.first_id });
        try caps_list.setCapability(1, .{ .id = ctx.second_id });
    }
};

fn buildResultsReturnFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    release_param_caps: bool,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(answer_id, .results);
    ret.setReleaseParamCaps(release_param_caps);
    var payload = try ret.payloadTyped();
    var any = try payload.initContent();
    _ = try any.initStruct(0, 0);
    _ = try ret.initCapTableTyped(0);
    return builder.finish();
}

fn buildReleaseFrame(allocator: std.mem.Allocator, export_id: u32, count: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildRelease(export_id, count);
    return builder.finish();
}

fn exportRefCount(peer: *const Peer, export_id: u32) !u32 {
    const entry = peer.exports.get(export_id) orelse return error.MissingExport;
    return entry.ref_count;
}

test "return with releaseParamCaps=true releases param-cap export refs to baseline" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    // Simulate a pre-existing wire ref held by the remote so the export
    // survives the release and we can observe the exact baseline.
    {
        var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
        entry.value_ptr.ref_count = 1;
    }

    var build_ctx = OneCapBuild{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);

    // Sending the export in params took one wire ref and recorded it.
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));
    try std.testing.expectEqual(@as(u32, 1), peer.question_param_export_refs.count());

    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    // The Return spent the recorded ref implicitly; record consumed.
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());
    try std.testing.expect(!peer.questions.contains(qid));
}

test "return with releaseParamCaps=true destroys export when both counts reach zero" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    // No baseline refs: the param-cap ref is the export's only wire ref and
    // it holds no answer refs, so spending it must destroy the export.
    try std.testing.expectEqual(@as(u32, 0), try exportRefCount(&peer, export_id));

    var build_ctx = OneCapBuild{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));

    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(!peer.exports.contains(export_id));
    try std.testing.expect(!peer.caps.hasExport(export_id));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());
}

test "return with releaseParamCaps=false retains refs until an explicit Release" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    {
        var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
        entry.value_ptr.ref_count = 1;
    }

    var build_ctx = OneCapBuild{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));

    const ret_frame = try buildResultsReturnFrame(allocator, qid, false);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    // releaseParamCaps=false: the remote keeps the refs alive and will send
    // an explicit Release. The record is dropped, the refs are untouched.
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());

    const release_frame = try buildReleaseFrame(allocator, export_id, 1);
    defer allocator.free(release_frame);
    try peer.handleFrame(release_frame);

    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));
}

test "return releaseParamCaps=true followed by explicit Release surfaces over-release protocol error" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    {
        var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
        entry.value_ptr.ref_count = 1;
    }

    var build_ctx = OneCapBuild{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));

    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));

    // A protocol-violating remote double-spends: releaseParamCaps=true AND
    // an explicit Release for more refs than remain. The existing
    // releaseExport guard rejects it without mutating the count — no
    // underflow, no panic.
    const over_release = try buildReleaseFrame(allocator, export_id, 2);
    defer allocator.free(over_release);
    try std.testing.expectError(error.ReleaseCountExceeded, peer.handleFrame(over_release));
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));

    // Spending the one genuinely remaining ref still works, destroying the
    // export...
    const legal_release = try buildReleaseFrame(allocator, export_id, 1);
    defer allocator.free(legal_release);
    try peer.handleFrame(legal_release);
    try std.testing.expect(!peer.exports.contains(export_id));

    // ...and a further Release for the now-unknown export is absorbed
    // gracefully (warn only), matching existing Release semantics.
    const stale_release = try buildReleaseFrame(allocator, export_id, 1);
    defer allocator.free(stale_release);
    try peer.handleFrame(stale_release);
}

test "same export twice in one call's params is deduped on the wire and settles to baseline" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    {
        var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
        entry.value_ptr.ref_count = 1;
    }

    // Two pointer occurrences of the SAME export: the outbound encoder
    // dedupes identical (tag, id) descriptors into one cap-table entry, so
    // exactly one wire ref is taken — and the record must mirror that (it
    // tracks refs actually taken, never pointer occurrences).
    var build_ctx = TwoCapBuild{ .first_id = export_id, .second_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, TwoCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));
    {
        const record = peer.question_param_export_refs.get(qid) orelse return error.MissingRecord;
        try std.testing.expectEqual(@as(usize, 1), record.items.len);
        try std.testing.expectEqual(export_id, record.items[0]);
    }

    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());
}

test "two distinct exports in one call are both released by one return" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_a: u8 = 0;
    var handler_b: u8 = 0;
    const export_a = try peer.addExport(.{ .ctx = &handler_a, .on_call = noopCall });
    const export_b = try peer.addExport(.{ .ctx = &handler_b, .on_call = noopCall });
    {
        var entry_a = peer.exports.getEntry(export_a) orelse return error.MissingExport;
        entry_a.value_ptr.ref_count = 1;
        var entry_b = peer.exports.getEntry(export_b) orelse return error.MissingExport;
        entry_b.value_ptr.ref_count = 1;
    }

    var build_ctx = TwoCapBuild{ .first_id = export_a, .second_id = export_b };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, TwoCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_a));
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_b));
    {
        const record = peer.question_param_export_refs.get(qid) orelse return error.MissingRecord;
        try std.testing.expectEqual(@as(usize, 2), record.items.len);
    }

    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_a));
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_b));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());
}

test "cancelled question absorbs late return and still releases param refs exactly once" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    {
        var entry = peer.exports.getEntry(export_id) orelse return error.MissingExport;
        entry.value_ptr.ref_count = 1;
    }

    var build_ctx = OneCapBuild{ .export_id = export_id };
    const qid = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));

    // Local cancel: the caller sees a synthetic exception, a Finish goes to
    // the remote, and the question entry stays behind (marked cancelled) to
    // absorb the remote's one guaranteed Return. The param-export record
    // must survive the cancel with it.
    try peer.cancelQuestion(qid, "test cancel");
    try std.testing.expect(peer.questions.contains(qid));
    try std.testing.expectEqual(@as(u32, 1), peer.question_param_export_refs.count());
    try std.testing.expectEqual(@as(u32, 2), try exportRefCount(&peer, export_id));

    // The late Return is absorbed silently — but its releaseParamCaps=true
    // still spends the recorded refs.
    const ret_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(!peer.questions.contains(qid));
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());

    // A duplicate Return for the dead question is a protocol violation and
    // must not touch the refs again.
    const dup_frame = try buildResultsReturnFrame(allocator, qid, true);
    defer allocator.free(dup_frame);
    try std.testing.expectError(error.UnknownQuestion, peer.handleFrame(dup_frame));
    try std.testing.expectEqual(@as(u32, 1), try exportRefCount(&peer, export_id));
}

test "deinit with an unanswered recorded question frees the record without spending" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });

    var build_ctx = OneCapBuild{ .export_id = export_id };
    _ = try peer.sendCall(7, 0xABCD, 0, &build_ctx, OneCapBuild.build, noopReturn);
    try std.testing.expectEqual(@as(u32, 1), peer.question_param_export_refs.count());
    // No Return ever arrives; the deferred deinit must free the record (the
    // testing allocator fails the test on any leak).
}

test "loopback call to a local export records no param-export entry" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var handler_state: u8 = 0;
    var param_handler_state: u8 = 0;
    const target_export = try peer.addExport(.{ .ctx = &handler_state, .on_call = noopCall });
    const param_export = try peer.addExport(.{ .ctx = &param_handler_state, .on_call = noopCall });

    // Loopback: the Call frame never crosses the wire (it is dispatched back
    // into this peer), so no record is taken — the local Return path cannot
    // legitimately spend wire refs the remote never saw.
    var build_ctx = OneCapBuild{ .export_id = param_export };
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = target_export } },
        0xABCD,
        0,
        &build_ctx,
        OneCapBuild.build,
        noopReturn,
    );
    try std.testing.expectEqual(@as(u32, 0), peer.question_param_export_refs.count());
}
