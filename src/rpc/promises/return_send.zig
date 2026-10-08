const protocol = @import("../wire/protocol.zig");

/// Send a Return frame to the remote, or deliver it to this peer when
/// `answer_id` is a loopback answer (`peer.loopback_questions`): one this
/// peer asked of itself.
///
/// A loopback Return never falls back to the wire. Its marker stays until
/// the Return is delivered, so when delivery fails before the question saw
/// it (`question_open` still holds it, as after an out-of-memory decode),
/// the answer is still a loopback one: the error goes back to the sender,
/// and the exception Return a handler answers with is delivered here too.
/// When delivery fails after the question saw its Return (its callback ran
/// out of memory), the answer is settled: the marker goes, the error is
/// reported, and the sender gets no error that would make it answer twice.
pub fn sendReturnFrameWithLoopbackForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    answer_id: u32,
    bytes: []const u8,
    deliver_loopback_return: *const fn (*PeerType, []const u8) anyerror!void,
    send_frame: *const fn (*PeerType, []const u8) anyerror!void,
    question_open: *const fn (*PeerType, u32) bool,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) !void {
    if (!peer.loopback_questions.contains(answer_id)) {
        try send_frame(peer, bytes);
        return;
    }
    deliver_loopback_return(peer, bytes) catch |err| {
        if (question_open(peer, answer_id)) return err;
        _ = peer.loopback_questions.remove(answer_id);
        report_nonfatal_error(peer, err);
        return;
    };
    _ = peer.loopback_questions.remove(answer_id);
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
