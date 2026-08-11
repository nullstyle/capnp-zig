const std = @import("std");
const capnpc = @import("capnpc-zig");

const Peer = capnpc.rpc.peer.Peer;

pub fn expectImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(peer.caps.hasImport(import_id));
}

pub fn expectNoImport(peer: *const Peer, import_id: u32) !void {
    try std.testing.expect(!peer.caps.hasImport(import_id));
}

pub fn expectNoProvideState(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), peer.provides_by_key.count());
}

pub fn expectNoJoinState(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.pending_joins.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_questions.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_relays.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_accepts.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_join_result_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.cross_peer_join_relay_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), peer.join_accept_host_links.items.len);
}

pub fn expectNoEmbargoedAcceptState(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accept_embargo_by_question.count());
}

pub fn expectNoThirdPartyState(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
}

pub fn expectNoParkedCalls(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.pending_promises.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_export_promises.count());
}

pub fn expectNoL3L4TransientState(peer: *const Peer) !void {
    try expectNoProvideState(peer);
    try expectNoJoinState(peer);
    try expectNoEmbargoedAcceptState(peer);
    try expectNoThirdPartyState(peer);
    try expectNoParkedCalls(peer);
}

pub fn expectOutboundProvideCoupled(
    recipient_peer: *const Peer,
    provide_peer: *const Peer,
    vine_id: u32,
    provide_question_id: u32,
    provided_import_id: ?u32,
) !void {
    const outbound = recipient_peer.outbound_provides.get(vine_id) orelse return error.MissingOutboundProvide;
    try std.testing.expectEqual(provide_peer, outbound.provide_peer orelse return error.MissingProvidePeer);
    try std.testing.expectEqual(provide_question_id, outbound.provide_question_id);
    switch (outbound.forward_target) {
        .imported => |import_id| try std.testing.expectEqual(
            provided_import_id orelse return error.MissingProvidedImport,
            import_id,
        ),
        .promised => return error.UnexpectedPromisedAnswerTarget,
    }

    for (provide_peer.coupled_vines.items) |link| {
        if (link.recipient_peer == recipient_peer and link.vine_id == vine_id) return;
    }
    return error.MissingCoupledVineBacklink;
}

pub fn expectNoOutboundProvide(recipient_peer: *const Peer, vine_id: u32) !void {
    try std.testing.expect(!recipient_peer.outbound_provides.contains(vine_id));
    try std.testing.expect(recipient_peer.caps.getThirdPartyHosted(vine_id) == null);
}

pub fn expectNoCrossPeerProxyLinks(peer: *const Peer) !void {
    try std.testing.expectEqual(@as(usize, 0), peer.cross_peer_proxy_links.items.len);
    try std.testing.expectEqual(@as(usize, 0), peer.cross_peer_join_relay_links.items.len);
}

/// `std.testing.checkAllAllocationFailures`, but with a pristine backing
/// allocator per iteration.
///
/// The std version shares ONE backing allocator across every fail-index
/// iteration, which couples the runs together through residual heap state.
/// That is enough to make it report `NondeterministicMemoryUsage` for code
/// that is in fact perfectly deterministic: `FailingAllocator` bumps
/// `alloc_index` only in `alloc`, while `resize`/`remap` bump a separate
/// counter it never fails, so a buffer that needs a fresh `alloc` on a cold
/// heap can grow in place on a warm one. One allocation disappears, the final
/// fail index is never reached, and the run is reported as nondeterministic.
///
/// Measured on `crossPeerJoinRelayOomImpl` (ubuntu-latest, ReleaseSafe): the
/// reference run made 35 allocations; iteration 34 made 34 allocations and 6
/// resizes, with no failure induced. Every one of the 34 injected failures
/// rolled back correctly — the rollback code was never the problem. macOS and
/// linux-aarch64 did not reproduce it, because whether growth can happen in
/// place is allocator- and platform-dependent.
///
/// Giving each iteration its own allocator removes the coupling, and keeps
/// per-iteration leak detection.
pub fn checkAllAllocationFailuresIsolated(
    comptime test_fn: anytype,
    extra_args: anytype,
) !void {
    const needed = blk: {
        var dbg: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(dbg.deinit() == .ok);
        var fa = std.testing.FailingAllocator.init(dbg.allocator(), .{});
        try @call(.auto, test_fn, .{fa.allocator()} ++ extra_args);
        break :blk fa.alloc_index;
    };

    for (0..needed) |fail_index| {
        var dbg: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(dbg.deinit() == .ok);
        var fa = std.testing.FailingAllocator.init(dbg.allocator(), .{ .fail_index = fail_index });
        if (@call(.auto, test_fn, .{fa.allocator()} ++ extra_args)) |_| {
            // A fresh heap every iteration means the alloc sequence is the
            // same one the reference run took, so an unreached fail index is
            // a real signal rather than an artifact.
            if (fa.has_induced_failure) return error.SwallowedOutOfMemoryError;
            return error.NondeterministicMemoryUsage;
        } else |err| switch (err) {
            error.OutOfMemory => {
                if (fa.allocated_bytes != fa.freed_bytes) return error.MemoryLeakDetected;
            },
            else => |e| return e,
        }
    }
}
