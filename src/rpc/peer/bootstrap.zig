const std = @import("std");
const protocol = @import("../wire/protocol.zig");

pub fn handleUnimplemented(
    comptime PeerType: type,
    peer: *PeerType,
    unimplemented: protocol.Unimplemented,
    on_unimplemented_question: *const fn (*PeerType, u32) anyerror!void,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    const tag = unimplemented.message_tag orelse return;
    switch (tag) {
        .bootstrap, .call => {
            const question_id = unimplemented.question_id orelse return;
            try on_unimplemented_question(peer, question_id);
        },
        // Echoed Resolve is handled by the peer before it delegates here:
        // the resolution's export reference must be released or it leaks for
        // the connection lifetime (see Peer.handleUnimplementedResolve in
        // peer/mod.zig).
        .resolve => {},
        // Echoed Provide/Accept are unreachable in this two-party runtime: it
        // never originates Provide/Accept (Level-3 origination is a separate,
        // Experimental surface — see docs/supported-surface.md), so no peer can
        // legitimately echo one back. They stay silent no-ops; there is no
        // sender state to unwind and no live question to fail.
        .provide => {},
        .accept => {},
        // A peer that echoes our Disembargo as Unimplemented cannot uphold
        // e-order across the resolve this Disembargo was protecting: the
        // embargo can never be lifted by a receiverLoopback, so the target
        // import would stay flagged `embargoed` with a `pending_embargoes`
        // entry retained for the connection's life. That is a protocol
        // violation, not a benign no-op — surface it as a connection-level
        // error (abort + tear down) rather than a silent drop that strands the
        // embargo. See docs/supported-surface.md known limitation #1.
        .disembargo => {
            try send_abort(peer, "peer echoed Disembargo as Unimplemented");
            return error.EchoedDisembargoUnimplemented;
        },
        // Remaining tags (return/finish/release/abort/...) carry no sender
        // state that must be unwound on echo; ignore them.
        else => {},
    }
}

pub fn handleUnimplementedQuestion(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) !void {
    const ret = protocol.Return{
        .answer_id = question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .exception,
        .results = null,
        .exception = .{
            .reason = "unimplemented",
            .trace = "",
            // The remote echoed our question back as `Unimplemented`, which is
            // precisely what this exception type means. Reporting it as `failed`
            // told the caller nothing about retryability.
            .type_value = @intFromEnum(protocol.ExceptionType.unimplemented),
        },
        .take_from_other_question = null,
    };
    on_return(peer, &.{}, ret) catch |err| switch (err) {
        error.UnknownQuestion => {},
        else => return err,
    };
}

pub fn handleUnimplementedQuestionForPeerFn(
    comptime PeerType: type,
    comptime on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) *const fn (*PeerType, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, question_id: u32) anyerror!void {
            try handleUnimplementedQuestion(PeerType, peer, question_id, on_return);
        }
    }.call;
}

pub fn handleAbort(
    allocator: std.mem.Allocator,
    last_remote_abort_reason: *?[]u8,
    abort: protocol.Abort,
) !void {
    if (last_remote_abort_reason.*) |existing| {
        allocator.free(existing);
        last_remote_abort_reason.* = null;
    }
    const reason_copy = try allocator.alloc(u8, abort.exception.reason.len);
    std.mem.copyForwards(u8, reason_copy, abort.exception.reason);
    last_remote_abort_reason.* = reason_copy;
    return error.RemoteAbort;
}

pub fn buildBootstrapReturnFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    export_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(question_id, .results);
    var payload = try ret.payloadTyped();
    var any = try payload.initContent();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTableTyped(1);
    var entry = try cap_list.get(0);
    try entry.setSenderHosted(export_id);

    return builder.finish();
}

pub fn handleBootstrap(
    comptime PeerType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    bootstrap_msg: protocol.Bootstrap,
    bootstrap_export_id: ?u32,
    question_id_in_use: *const fn (*PeerType, u32) anyerror!bool,
    note_export_ref: *const fn (*PeerType, u32) anyerror!void,
    rollback_export_ref: *const fn (*PeerType, u32) void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    send_and_record_return: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    // Reject a Bootstrap whose question id is already live (spec violation),
    // mirroring handleCall's defense. Without this a remote could reuse an
    // active or resolved question id; we would emit a second Return on that id
    // and recordResolvedAnswer would silently replace the cached frame, so
    // pipelined PromisedAnswer references on the old answer would resolve
    // against the new one.
    if (try question_id_in_use(peer, bootstrap_msg.question_id)) {
        return error.DuplicateQuestionId;
    }

    const export_id = bootstrap_export_id orelse {
        try send_return_exception(peer, bootstrap_msg.question_id, "bootstrap not configured");
        return;
    };

    const bytes = try buildBootstrapReturnFrame(allocator, bootstrap_msg.question_id, export_id);
    defer allocator.free(bytes);

    try note_export_ref(peer, export_id);
    // If delivery fails the remote never received the cap descriptor and will
    // never Release it, so undo the ref-count bump to avoid overstating the
    // export's ref-count for the connection lifetime. send_and_record_return
    // reserves the recorded answer's resources BEFORE sending — recording
    // cannot fail once the frame is on the wire (matching sendReturnResults /
    // sendPrebuiltReturnFrame) — and afterward either commits the answer or,
    // when a Finish re-entered while the send was on the stack, commits it to
    // drain queued pipelined calls and immediately applies the Finish cleanup.
    // Every error it can return means the frame was not delivered.
    errdefer rollback_export_ref(peer, export_id);
    try send_and_record_return(peer, bootstrap_msg.question_id, bytes);
}
