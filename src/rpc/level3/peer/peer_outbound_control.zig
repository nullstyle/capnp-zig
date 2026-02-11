const std = @import("std");
const protocol = @import("../../level0/protocol.zig");

pub fn sendBuilder(
    comptime PeerType: type,
    peer: *PeerType,
    builder: *protocol.MessageBuilder,
    send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    const bytes = try builder.finish();
    defer peer.allocator.free(bytes);
    try send_frame(peer, bytes);
}

pub fn sendBuilderForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void {
    return struct {
        fn call(peer: *PeerType, builder: *protocol.MessageBuilder) anyerror!void {
            try sendBuilder(PeerType, peer, builder, send_frame);
        }
    }.call;
}

pub fn sendRelease(
    comptime PeerType: type,
    peer: *PeerType,
    import_id: u32,
    count: u32,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildRelease(import_id, count);
    try send_builder(peer, &builder);
}

pub fn sendReleaseViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    import_id: u32,
    count: u32,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendRelease(
        PeerType,
        peer,
        import_id,
        count,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendReleaseForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, u32, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, import_id: u32, count: u32) anyerror!void {
            try sendRelease(PeerType, peer, import_id, count, send_builder);
        }
    }.call;
}

pub fn sendReleaseViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, u32, u32) anyerror!void {
    return sendReleaseForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendFinishWithFlags(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    require_early_cancellation: bool,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildFinish(question_id, release_result_caps, require_early_cancellation);
    try send_builder(peer, &builder);
}

pub fn sendFinishWithFlagsViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    require_early_cancellation: bool,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendFinishWithFlags(
        PeerType,
        peer,
        question_id,
        release_result_caps,
        require_early_cancellation,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendFinishWithFlagsForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, u32, bool, bool) anyerror!void {
    return struct {
        fn call(
            peer: *PeerType,
            question_id: u32,
            release_result_caps: bool,
            require_early_cancellation: bool,
        ) anyerror!void {
            try sendFinishWithFlags(
                PeerType,
                peer,
                question_id,
                release_result_caps,
                require_early_cancellation,
                send_builder,
            );
        }
    }.call;
}

pub fn sendFinishWithFlagsViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, u32, bool, bool) anyerror!void {
    return sendFinishWithFlagsForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendFinish(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    send_finish_with_flags: *const fn (*PeerType, u32, bool, bool) anyerror!void,
) !void {
    try send_finish_with_flags(peer, question_id, release_result_caps, false);
}

pub fn sendFinishForPeerFn(
    comptime PeerType: type,
    comptime send_finish_with_flags: *const fn (*PeerType, u32, bool, bool) anyerror!void,
) *const fn (*PeerType, u32, bool) anyerror!void {
    return struct {
        fn call(peer: *PeerType, question_id: u32, release_result_caps: bool) anyerror!void {
            try sendFinish(PeerType, peer, question_id, release_result_caps, send_finish_with_flags);
        }
    }.call;
}

pub fn sendFinishViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, u32, bool) anyerror!void {
    return sendFinishForPeerFn(PeerType, sendFinishWithFlagsViaSendFrameForPeerFn(PeerType, send_frame));
}

pub fn sendResolveCap(
    comptime PeerType: type,
    peer: *PeerType,
    promise_id: u32,
    descriptor: protocol.CapDescriptor,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildResolveCap(promise_id, descriptor);
    try send_builder(peer, &builder);
}

pub fn sendResolveCapViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    promise_id: u32,
    descriptor: protocol.CapDescriptor,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendResolveCap(
        PeerType,
        peer,
        promise_id,
        descriptor,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendResolveCapForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, u32, protocol.CapDescriptor) anyerror!void {
    return struct {
        fn call(peer: *PeerType, promise_id: u32, descriptor: protocol.CapDescriptor) anyerror!void {
            try sendResolveCap(PeerType, peer, promise_id, descriptor, send_builder);
        }
    }.call;
}

pub fn sendResolveCapViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, u32, protocol.CapDescriptor) anyerror!void {
    return sendResolveCapForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendResolveException(
    comptime PeerType: type,
    peer: *PeerType,
    promise_id: u32,
    reason: []const u8,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildResolveException(promise_id, reason);
    try send_builder(peer, &builder);
}

pub fn sendResolveExceptionViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    promise_id: u32,
    reason: []const u8,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendResolveException(
        PeerType,
        peer,
        promise_id,
        reason,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendResolveExceptionForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, u32, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, promise_id: u32, reason: []const u8) anyerror!void {
            try sendResolveException(PeerType, peer, promise_id, reason, send_builder);
        }
    }.call;
}

pub fn sendResolveExceptionViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, u32, []const u8) anyerror!void {
    return sendResolveExceptionForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendAbort(
    comptime PeerType: type,
    peer: *PeerType,
    reason: []const u8,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildAbort(reason);
    try send_builder(peer, &builder);
}

pub fn sendAbortViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    reason: []const u8,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendAbort(
        PeerType,
        peer,
        reason,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendAbortForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, reason: []const u8) anyerror!void {
            try sendAbort(PeerType, peer, reason, send_builder);
        }
    }.call;
}

pub fn sendAbortViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, []const u8) anyerror!void {
    return sendAbortForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendDisembargoSenderLoopback(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    embargo_id: u32,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildDisembargoSenderLoopback(target, embargo_id);
    try send_builder(peer, &builder);
}

pub fn sendDisembargoSenderLoopbackViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    embargo_id: u32,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendDisembargoSenderLoopback(
        PeerType,
        peer,
        target,
        embargo_id,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendDisembargoSenderLoopbackForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, target: protocol.MessageTarget, embargo_id: u32) anyerror!void {
            try sendDisembargoSenderLoopback(PeerType, peer, target, embargo_id, send_builder);
        }
    }.call;
}

pub fn sendDisembargoSenderLoopbackViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void {
    return sendDisembargoSenderLoopbackForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

pub fn sendDisembargoReceiverLoopback(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    embargo_id: u32,
    send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) !void {
    var builder = protocol.MessageBuilder.init(peer.allocator);
    defer builder.deinit();
    try builder.buildDisembargoReceiverLoopback(target, embargo_id);
    try send_builder(peer, &builder);
}

pub fn sendDisembargoReceiverLoopbackViaSendFrame(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    embargo_id: u32,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    try sendDisembargoReceiverLoopback(
        PeerType,
        peer,
        target,
        embargo_id,
        sendBuilderForPeerFn(PeerType, send_frame),
    );
}

pub fn sendDisembargoReceiverLoopbackForPeerFn(
    comptime PeerType: type,
    comptime send_builder: *const fn (*PeerType, *protocol.MessageBuilder) anyerror!void,
) *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, target: protocol.MessageTarget, embargo_id: u32) anyerror!void {
            try sendDisembargoReceiverLoopback(PeerType, peer, target, embargo_id, send_builder);
        }
    }.call;
}

pub fn sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(
    comptime PeerType: type,
    comptime send_frame: *const fn (*PeerType, []const u8) anyerror!void,
) *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void {
    return sendDisembargoReceiverLoopbackForPeerFn(PeerType, sendBuilderForPeerFn(PeerType, send_frame));
}

test "peer_outbound_control sendReleaseForPeerFn builds release message" {
    const State = struct {
        allocator: std.mem.Allocator,
        calls: usize = 0,
        release_id: u32 = 0,
        release_count: u32 = 0,
    };

    const Hooks = struct {
        fn sendFrame(state: *State, frame: []const u8) !void {
            state.calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            try std.testing.expectEqual(protocol.MessageTag.release, decoded.tag);
            const release = try decoded.asRelease();
            state.release_id = release.id;
            state.release_count = release.reference_count;
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };
    const send_builder = sendBuilderForPeerFn(State, Hooks.sendFrame);
    const send_release = sendReleaseForPeerFn(State, send_builder);
    try send_release(&state, 77, 3);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(u32, 77), state.release_id);
    try std.testing.expectEqual(@as(u32, 3), state.release_count);
}

test "peer_outbound_control sendFinishForPeerFn clears early-cancel flag" {
    const State = struct {
        allocator: std.mem.Allocator,
        calls: usize = 0,
        question_id: u32 = 0,
        release_result_caps: bool = false,
        require_early_cancellation: bool = true,
    };

    const Hooks = struct {
        fn sendFrame(state: *State, frame: []const u8) !void {
            state.calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            try std.testing.expectEqual(protocol.MessageTag.finish, decoded.tag);
            const finish = try decoded.asFinish();
            state.question_id = finish.question_id;
            state.release_result_caps = finish.release_result_caps;
            state.require_early_cancellation = finish.require_early_cancellation;
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };
    const send_builder = sendBuilderForPeerFn(State, Hooks.sendFrame);
    const send_finish_with_flags = sendFinishWithFlagsForPeerFn(State, send_builder);
    const send_finish = sendFinishForPeerFn(State, send_finish_with_flags);
    try send_finish(&state, 91, true);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(u32, 91), state.question_id);
    try std.testing.expect(state.release_result_caps);
    try std.testing.expect(!state.require_early_cancellation);
}

test "peer_outbound_control sendAbortForPeerFn builds abort message" {
    const State = struct {
        allocator: std.mem.Allocator,
        calls: usize = 0,
        reason: []const u8 = "",
    };

    const Hooks = struct {
        fn sendFrame(state: *State, frame: []const u8) !void {
            state.calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
            const abort = try decoded.asAbort();
            state.reason = abort.exception.reason;
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };
    const send_builder = sendBuilderForPeerFn(State, Hooks.sendFrame);
    const send_abort = sendAbortForPeerFn(State, send_builder);
    try send_abort(&state, "fatal");

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqualStrings("fatal", state.reason);
}

test "peer_outbound_control sendFinishViaSendFrameForPeerFn composes finish callbacks" {
    const State = struct {
        allocator: std.mem.Allocator,
        calls: usize = 0,
        require_early_cancellation: bool = true,
    };

    const Hooks = struct {
        fn sendFrame(state: *State, frame: []const u8) !void {
            state.calls += 1;
            var decoded = try protocol.DecodedMessage.init(state.allocator, frame);
            defer decoded.deinit();
            const finish = try decoded.asFinish();
            state.require_early_cancellation = finish.require_early_cancellation;
        }
    };

    var state = State{
        .allocator = std.testing.allocator,
    };
    const send_finish = sendFinishViaSendFrameForPeerFn(State, Hooks.sendFrame);
    try send_finish(&state, 3, false);

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expect(!state.require_early_cancellation);
}
