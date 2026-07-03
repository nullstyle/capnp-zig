const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.wire.protocol;
const cap_table = capnpc.rpc.caps.table;
const Peer = capnpc.rpc.peer.Peer;
const peer_test_hooks = Peer.test_hooks;

// Reusable allocation-failure injection over the fallible peer helpers. Each
// impl runs a self-contained operation; std.testing.checkAllAllocationFailures
// re-runs it with every allocation failing in turn and asserts the operation
// propagates error.OutOfMemory with no leak and no double-free. This guards the
// queue-ownership and question-rollback error paths systematically.
//
// (Peer.handleFrame deliberately swallows OOM as a non-fatal error, so it is
// not a checkAllAllocationFailures target; these exercise the helpers that do
// propagate — where the ownership/rollback bugs actually lived.)

fn noopSend(_: *anyopaque, _: []const u8) anyerror!void {}
fn noopReturn(_: *anyopaque, _: *Peer, _: protocol.Return, _: *const cap_table.InboundCapTable) anyerror!void {}

fn buildCallFrame(allocator: std.mem.Allocator, question_id: u32) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    var call = try builder.beginCall(question_id, 0xABCD, 0);
    try call.setTargetImportedCap(7);
    _ = try call.initCapTableTyped(0);
    return builder.finish();
}

fn queuePromisedCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    const frame = buildCallFrame(allocator, 100) catch |err| {
        inbound.deinit();
        return err;
    };
    defer allocator.free(frame);

    peer_test_hooks.queuePromisedCall(&peer, 5, frame, inbound) catch |err| {
        // Ownership contract: on failure the caller retains `inbound`.
        inbound.deinit();
        return err;
    };
    // On success the queue owns `inbound` and a copy of the frame; Peer.deinit
    // frees both, and the original `frame` is freed by the defer above.
}

fn sendBootstrapOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    peer.disableThreadAffinity();
    defer peer.deinit();

    var sink: u8 = 0;
    peer.setSendFrameOverride(&sink, noopSend);

    var ctx: u8 = 0;
    _ = try peer.sendBootstrap(&ctx, noopReturn);
    // On OOM sendBootstrap must roll the question back (no leak); on success the
    // question holds only a stack ctx (no deinit_ctx), freed with the map.
}

// Note: sendCall / caps.noteImport tolerate a best-effort (non-fatal)
// allocation on the outbound path, so they are not all-or-nothing
// checkAllAllocationFailures targets (the harness would flag the recovered
// allocation as a "swallowed" OOM even though there is no leak). The two
// targets below are the ones whose error paths actually carried ownership /
// rollback bugs this sprint fixed.

test "queuePromisedCall propagates OOM without leak or double-free" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromisedCallOomImpl, .{});
}

test "sendBootstrap rolls back cleanly under OOM injection" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sendBootstrapOomImpl, .{});
}
