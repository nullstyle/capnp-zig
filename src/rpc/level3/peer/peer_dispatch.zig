const std = @import("std");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../level0/protocol.zig");

pub const InboundRoute = enum {
    unimplemented,
    abort,
    bootstrap,
    call,
    return_,
    finish,
    release,
    resolve,
    disembargo,
    provide,
    accept,
    join,
    third_party_answer,
    unknown,
};

pub fn route(tag: protocol.MessageTag) InboundRoute {
    return switch (tag) {
        .unimplemented => .unimplemented,
        .abort => .abort,
        .bootstrap => .bootstrap,
        .call => .call,
        .return_ => .return_,
        .finish => .finish,
        .release => .release,
        .resolve => .resolve,
        .disembargo => .disembargo,
        .provide => .provide,
        .accept => .accept,
        .join => .join,
        .third_party_answer => .third_party_answer,
        else => .unknown,
    };
}

pub fn dispatchDecodedForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    decoded: *protocol.DecodedMessage,
    handle_unimplemented: *const fn (*PeerType, protocol.Unimplemented) anyerror!void,
    handle_abort: *const fn (*PeerType, protocol.Abort) anyerror!void,
    handle_bootstrap: *const fn (*PeerType, protocol.Bootstrap) anyerror!void,
    handle_call: *const fn (*PeerType, []const u8, protocol.Call) anyerror!void,
    handle_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
    handle_finish: *const fn (*PeerType, protocol.Finish) anyerror!void,
    handle_release: *const fn (*PeerType, protocol.Release) anyerror!void,
    handle_resolve: *const fn (*PeerType, protocol.Resolve) anyerror!void,
    handle_disembargo: *const fn (*PeerType, protocol.Disembargo) anyerror!void,
    handle_provide: *const fn (*PeerType, protocol.Provide) anyerror!void,
    handle_accept: *const fn (*PeerType, protocol.Accept) anyerror!void,
    handle_join: *const fn (*PeerType, protocol.Join) anyerror!void,
    handle_third_party_answer: *const fn (*PeerType, protocol.ThirdPartyAnswer) anyerror!void,
    send_unimplemented: *const fn (*PeerType, message.AnyPointerReader) anyerror!void,
) !void {
    switch (route(decoded.tag)) {
        .unimplemented => try handle_unimplemented(peer, try decoded.asUnimplemented()),
        .abort => try handle_abort(peer, try decoded.asAbort()),
        .bootstrap => try handle_bootstrap(peer, try decoded.asBootstrap()),
        .call => try handle_call(peer, frame, try decoded.asCall()),
        .return_ => try handle_return(peer, frame, try decoded.asReturn()),
        .finish => try handle_finish(peer, try decoded.asFinish()),
        .release => try handle_release(peer, try decoded.asRelease()),
        .resolve => try handle_resolve(peer, try decoded.asResolve()),
        .disembargo => try handle_disembargo(peer, try decoded.asDisembargo()),
        .provide => try handle_provide(peer, try decoded.asProvide()),
        .accept => try handle_accept(peer, try decoded.asAccept()),
        .join => try handle_join(peer, try decoded.asJoin()),
        .third_party_answer => try handle_third_party_answer(peer, try decoded.asThirdPartyAnswer()),
        .unknown => {
            const root = try decoded.msg.getRootAnyPointer();
            try send_unimplemented(peer, root);
        },
    }
}

test "peer_dispatch dispatchDecodedForPeer routes call messages with original frame" {
    const State = struct {
        calls: usize = 0,
        question_id: u32 = 0,
        frame_len: usize = 0,
    };

    const Hooks = struct {
        fn unexpectedUnimplemented(_: *State, _: protocol.Unimplemented) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedAbort(_: *State, _: protocol.Abort) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedBootstrap(_: *State, _: protocol.Bootstrap) !void {
            return error.TestUnexpectedResult;
        }
        fn onCall(state: *State, frame: []const u8, call: protocol.Call) !void {
            state.calls += 1;
            state.question_id = call.question_id;
            state.frame_len = frame.len;
        }
        fn unexpectedReturn(_: *State, _: []const u8, _: protocol.Return) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedFinish(_: *State, _: protocol.Finish) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedRelease(_: *State, _: protocol.Release) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedResolve(_: *State, _: protocol.Resolve) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedDisembargo(_: *State, _: protocol.Disembargo) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedProvide(_: *State, _: protocol.Provide) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedAccept(_: *State, _: protocol.Accept) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedJoin(_: *State, _: protocol.Join) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedThirdPartyAnswer(_: *State, _: protocol.ThirdPartyAnswer) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedSendUnimplemented(_: *State, _: message.AnyPointerReader) !void {
            return error.TestUnexpectedResult;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var call = try builder.beginCall(88, 0xABCD, 5);
    try call.setTargetImportedCap(7);
    try call.setEmptyCapTable();
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();

    var state = State{};
    try dispatchDecodedForPeer(
        State,
        &state,
        frame,
        &decoded,
        Hooks.unexpectedUnimplemented,
        Hooks.unexpectedAbort,
        Hooks.unexpectedBootstrap,
        Hooks.onCall,
        Hooks.unexpectedReturn,
        Hooks.unexpectedFinish,
        Hooks.unexpectedRelease,
        Hooks.unexpectedResolve,
        Hooks.unexpectedDisembargo,
        Hooks.unexpectedProvide,
        Hooks.unexpectedAccept,
        Hooks.unexpectedJoin,
        Hooks.unexpectedThirdPartyAnswer,
        Hooks.unexpectedSendUnimplemented,
    );

    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(u32, 88), state.question_id);
    try std.testing.expectEqual(frame.len, state.frame_len);
}

test "peer_dispatch dispatchDecodedForPeer routes unknown tags to sendUnimplemented" {
    const State = struct {
        send_unimplemented_calls: usize = 0,
        saw_null_root: bool = false,
    };

    const Hooks = struct {
        fn unexpectedUnimplemented(_: *State, _: protocol.Unimplemented) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedAbort(_: *State, _: protocol.Abort) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedBootstrap(_: *State, _: protocol.Bootstrap) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedCall(_: *State, _: []const u8, _: protocol.Call) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedReturn(_: *State, _: []const u8, _: protocol.Return) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedFinish(_: *State, _: protocol.Finish) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedRelease(_: *State, _: protocol.Release) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedResolve(_: *State, _: protocol.Resolve) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedDisembargo(_: *State, _: protocol.Disembargo) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedProvide(_: *State, _: protocol.Provide) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedAccept(_: *State, _: protocol.Accept) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedJoin(_: *State, _: protocol.Join) !void {
            return error.TestUnexpectedResult;
        }
        fn unexpectedThirdPartyAnswer(_: *State, _: protocol.ThirdPartyAnswer) !void {
            return error.TestUnexpectedResult;
        }
        fn onSendUnimplemented(state: *State, root: message.AnyPointerReader) !void {
            state.send_unimplemented_calls += 1;
            state.saw_null_root = root.isNull();
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    try builder.buildAbort("abort");
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    decoded.tag = .obsolete_save;

    var state = State{};
    try dispatchDecodedForPeer(
        State,
        &state,
        frame,
        &decoded,
        Hooks.unexpectedUnimplemented,
        Hooks.unexpectedAbort,
        Hooks.unexpectedBootstrap,
        Hooks.unexpectedCall,
        Hooks.unexpectedReturn,
        Hooks.unexpectedFinish,
        Hooks.unexpectedRelease,
        Hooks.unexpectedResolve,
        Hooks.unexpectedDisembargo,
        Hooks.unexpectedProvide,
        Hooks.unexpectedAccept,
        Hooks.unexpectedJoin,
        Hooks.unexpectedThirdPartyAnswer,
        Hooks.onSendUnimplemented,
    );

    try std.testing.expectEqual(@as(usize, 1), state.send_unimplemented_calls);
    try std.testing.expect(!state.saw_null_root);
}
