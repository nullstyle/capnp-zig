const std = @import("std");
const message = @import("../../../message.zig");
const protocol = @import("../../protocol.zig");

pub fn clearSendResultsToThirdPartyForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
) void {
    if (peer.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
        if (entry.value) |payload| peer.allocator.free(payload);
    }
}

pub fn clearSendResultsRoutingForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
) void {
    _ = peer.send_results_to_yourself.remove(answer_id);
    clear_send_results_to_third_party(peer, answer_id);
}

pub fn sendReturnFrameWithLoopbackForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    bytes: []const u8,
    deliver_loopback_return: *const fn (*PeerType, []const u8) anyerror!void,
    send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    if (peer.loopback_questions.remove(answer_id)) {
        try deliver_loopback_return(peer, bytes);
        return;
    }
    try send_frame(peer, bytes);
}

pub fn noteOutboundReturnCapRefsForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    ret: protocol.Return,
    note_export_ref: *const fn (*PeerType, u32) anyerror!void,
) !void {
    if (ret.tag != .results) return;
    const payload = ret.results orelse return error.InvalidReturnSemantics;
    const cap_table_list = payload.cap_table orelse return;

    var idx: u32 = 0;
    while (idx < cap_table_list.len()) : (idx += 1) {
        const reader = try cap_table_list.get(idx);
        const descriptor = try protocol.CapDescriptor.fromReader(reader);
        switch (descriptor.tag) {
            .sender_hosted, .sender_promise => {
                const id = descriptor.id orelse return error.MissingCapDescriptorId;
                try note_export_ref(peer, id);
            },
            else => {},
        }
    }
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
