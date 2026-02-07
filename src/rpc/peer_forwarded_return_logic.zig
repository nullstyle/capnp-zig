const std = @import("std");
const message = @import("../message.zig");
const protocol = @import("protocol.zig");

pub const ForwardedReturnMode = enum {
    translate_to_caller,
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

pub fn handleForwardedReturn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    mode: ForwardedReturnMode,
    answer_id: u32,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    send_return_results: *const fn (*PeerType, u32, protocol.Payload, *const InboundCapsType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    send_return_tag: *const fn (*PeerType, u32, protocol.ReturnTag) anyerror!void,
    lookup_forwarded_question: *const fn (*PeerType, u32) ?u32,
    send_take_from_other_question: *const fn (*PeerType, u32, u32) anyerror!void,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_accept_from_third_party: *const fn (*PeerType, u32, ?[]const u8) anyerror!void,
    context_third_party_payload: ?[]const u8,
) !void {
    switch (mode) {
        .translate_to_caller => switch (ret.tag) {
            .results => {
                const payload = ret.results orelse {
                    try send_return_exception(peer, answer_id, "forwarded return missing payload");
                    return;
                };
                try send_return_results(peer, answer_id, payload, inbound_caps);
            },
            .exception => {
                const ex = ret.exception orelse {
                    try send_return_exception(peer, answer_id, "forwarded return missing exception");
                    return;
                };
                try send_return_exception(peer, answer_id, ex.reason);
            },
            .canceled => try send_return_tag(peer, answer_id, .canceled),
            .results_sent_elsewhere => {
                try send_return_exception(peer, answer_id, "forwarded resultsSentElsewhere unsupported");
            },
            .take_from_other_question => {
                const other_local_id = ret.take_from_other_question orelse return error.MissingQuestionId;
                const translated = lookup_forwarded_question(peer, other_local_id) orelse {
                    try send_return_exception(peer, answer_id, "forwarded takeFromOtherQuestion missing mapping");
                    return;
                };
                try send_take_from_other_question(peer, answer_id, translated);
            },
            .accept_from_third_party => {
                const await_payload = try capture_payload(peer, ret.accept_from_third_party);
                defer if (await_payload) |payload| free_payload(peer, payload);
                try send_accept_from_third_party(peer, answer_id, await_payload);
            },
        },
        .sent_elsewhere => switch (ret.tag) {
            .results_sent_elsewhere, .canceled => {},
            else => return error.UnexpectedForwardedTailReturn,
        },
        .propagate_results_sent_elsewhere => switch (ret.tag) {
            .results_sent_elsewhere => try send_return_tag(peer, answer_id, .results_sent_elsewhere),
            .canceled => try send_return_tag(peer, answer_id, .canceled),
            .exception => {
                const ex = ret.exception orelse {
                    try send_return_exception(peer, answer_id, "forwarded return missing exception");
                    return;
                };
                try send_return_exception(peer, answer_id, ex.reason);
            },
            .take_from_other_question => {
                try send_return_exception(peer, answer_id, "forwarded takeFromOtherQuestion unsupported");
            },
            .accept_from_third_party, .results => {
                try send_return_tag(peer, answer_id, .results_sent_elsewhere);
            },
        },
        .propagate_accept_from_third_party => switch (ret.tag) {
            .results_sent_elsewhere => {
                try send_accept_from_third_party(peer, answer_id, context_third_party_payload);
            },
            .accept_from_third_party => {
                const await_payload = try capture_payload(peer, ret.accept_from_third_party);
                defer if (await_payload) |payload| free_payload(peer, payload);
                try send_accept_from_third_party(peer, answer_id, await_payload);
            },
            .canceled => try send_return_tag(peer, answer_id, .canceled),
            .exception => {
                const ex = ret.exception orelse {
                    try send_return_exception(peer, answer_id, "forwarded return missing exception");
                    return;
                };
                try send_return_exception(peer, answer_id, ex.reason);
            },
            .take_from_other_question => {
                try send_return_exception(peer, answer_id, "forwarded takeFromOtherQuestion unsupported");
            },
            .results => {
                const payload = ret.results orelse {
                    try send_return_exception(peer, answer_id, "forwarded return missing payload");
                    return;
                };
                try send_return_results(peer, answer_id, payload, inbound_caps);
            },
        },
    }
}

test "peer_forwarded_return_logic translate mode missing results payload sends exception callback" {
    const InboundCaps = struct {};
    const State = struct {
        exception_calls: usize = 0,
        reason: ?[]const u8 = null,
    };

    const Hooks = struct {
        fn sendResults(state: *State, answer_id: u32, payload: protocol.Payload, inbound: *const InboundCaps) !void {
            _ = state;
            _ = answer_id;
            _ = payload;
            _ = inbound;
            return error.TestUnexpectedResult;
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            _ = answer_id;
            state.exception_calls += 1;
            state.reason = reason;
        }

        fn sendTag(state: *State, answer_id: u32, tag: protocol.ReturnTag) !void {
            _ = state;
            _ = answer_id;
            _ = tag;
            return error.TestUnexpectedResult;
        }

        fn lookup(state: *State, local_question_id: u32) ?u32 {
            _ = state;
            _ = local_question_id;
            return null;
        }

        fn sendTake(state: *State, answer_id: u32, other_question_id: u32) !void {
            _ = state;
            _ = answer_id;
            _ = other_question_id;
            return error.TestUnexpectedResult;
        }

        fn capture(state: *State, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = state;
            _ = ptr;
            return null;
        }

        fn free(state: *State, payload: []u8) void {
            _ = state;
            _ = payload;
        }

        fn sendAccept(state: *State, answer_id: u32, payload: ?[]const u8) !void {
            _ = state;
            _ = answer_id;
            _ = payload;
            return error.TestUnexpectedResult;
        }
    };

    var state = State{};
    const inbound = InboundCaps{};
    const ret = protocol.Return{
        .answer_id = 1,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try handleForwardedReturn(
        State,
        InboundCaps,
        &state,
        .translate_to_caller,
        7,
        ret,
        &inbound,
        Hooks.sendResults,
        Hooks.sendException,
        Hooks.sendTag,
        Hooks.lookup,
        Hooks.sendTake,
        Hooks.capture,
        Hooks.free,
        Hooks.sendAccept,
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), state.exception_calls);
    try std.testing.expectEqualStrings("forwarded return missing payload", state.reason orelse "");
}

test "peer_forwarded_return_logic sentElsewhere mode rejects unexpected return tag" {
    const InboundCaps = struct {};
    const State = struct {};
    const Hooks = struct {
        fn sendResults(state: *State, answer_id: u32, payload: protocol.Payload, inbound: *const InboundCaps) !void {
            _ = state;
            _ = answer_id;
            _ = payload;
            _ = inbound;
            return error.TestUnexpectedResult;
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            _ = state;
            _ = answer_id;
            _ = reason;
            return error.TestUnexpectedResult;
        }

        fn sendTag(state: *State, answer_id: u32, tag: protocol.ReturnTag) !void {
            _ = state;
            _ = answer_id;
            _ = tag;
            return error.TestUnexpectedResult;
        }

        fn lookup(state: *State, local_question_id: u32) ?u32 {
            _ = state;
            _ = local_question_id;
            return null;
        }

        fn sendTake(state: *State, answer_id: u32, other_question_id: u32) !void {
            _ = state;
            _ = answer_id;
            _ = other_question_id;
            return error.TestUnexpectedResult;
        }

        fn capture(state: *State, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = state;
            _ = ptr;
            return null;
        }

        fn free(state: *State, payload: []u8) void {
            _ = state;
            _ = payload;
        }

        fn sendAccept(state: *State, answer_id: u32, payload: ?[]const u8) !void {
            _ = state;
            _ = answer_id;
            _ = payload;
            return error.TestUnexpectedResult;
        }
    };

    var state = State{};
    const inbound = InboundCaps{};
    const ret = protocol.Return{
        .answer_id = 2,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = .{
            .content = undefined,
            .cap_table = null,
        },
        .exception = null,
        .take_from_other_question = null,
    };

    try std.testing.expectError(
        error.UnexpectedForwardedTailReturn,
        handleForwardedReturn(
            State,
            InboundCaps,
            &state,
            .sent_elsewhere,
            8,
            ret,
            &inbound,
            Hooks.sendResults,
            Hooks.sendException,
            Hooks.sendTag,
            Hooks.lookup,
            Hooks.sendTake,
            Hooks.capture,
            Hooks.free,
            Hooks.sendAccept,
            null,
        ),
    );
}
