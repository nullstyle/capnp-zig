const std = @import("std");
const message = @import("../../serialization/message.zig");

pub fn clearSendResultsToThirdPartyForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
) void {
    if (peer.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
        if (entry.value) |payload| peer.allocator.free(payload);
    }
}

/// Not for `rpc.peer.Peer`: the Peer drops a `yourself` mark only through
/// `ReturnSend.dropYourselfMark`, which also ends a tail call held on the
/// call (`Peer.held_tail_calls`).
pub fn clearSendResultsRoutingForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
) void {
    _ = peer.send_results_to_yourself.remove(answer_id);
    clear_send_results_to_third_party(peer, answer_id);
}

pub fn noteSendResultsToYourselfForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
) !void {
    clear_send_results_to_third_party(peer, answer_id);
    _ = try peer.send_results_to_yourself.getOrPut(answer_id);
}

/// Not for `rpc.peer.Peer`: the Peer drops a `yourself` mark only through
/// `ReturnSend.dropYourselfMark`, which also ends a tail call held on the
/// call (`Peer.held_tail_calls`).
pub fn noteSendResultsToThirdPartyForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    ptr: ?message.AnyPointerReader,
    capture_payload: *const fn (std.mem.Allocator, ?message.AnyPointerReader) anyerror!?[]u8,
) !void {
    _ = peer.send_results_to_yourself.remove(answer_id);

    const payload = try capture_payload(peer.allocator, ptr);
    errdefer if (payload) |bytes| peer.allocator.free(bytes);

    const entry = try peer.send_results_to_third_party.getOrPut(answer_id);
    if (entry.found_existing) {
        if (entry.value_ptr.*) |existing| peer.allocator.free(existing);
    }
    entry.value_ptr.* = payload;
}
