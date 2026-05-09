const protocol = @import("../wire/protocol.zig");

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
            .senderHosted, .senderPromise => {
                const id = descriptor.id orelse return error.MissingCapDescriptorId;
                try note_export_ref(peer, id);
            },
            else => {},
        }
    }
}

pub fn rollbackOutboundReturnCapRefsForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    ret: protocol.Return,
    rollback_export_ref: *const fn (*PeerType, u32) void,
) !void {
    if (ret.tag != .results) return;
    const payload = ret.results orelse return error.InvalidReturnSemantics;
    const cap_table_list = payload.cap_table orelse return;

    var idx: u32 = 0;
    while (idx < cap_table_list.len()) : (idx += 1) {
        const reader = try cap_table_list.get(idx);
        const descriptor = try protocol.CapDescriptor.fromReader(reader);
        switch (descriptor.tag) {
            .senderHosted, .senderPromise => {
                const id = descriptor.id orelse return error.MissingCapDescriptorId;
                rollback_export_ref(peer, id);
            },
            else => {},
        }
    }
}
