const std = @import("std");
const message = @import("../../serialization/message.zig");
const peer_forwarded_return_logic = @import("./forward/peer_forwarded_return_logic.zig");
const protocol = @import("../wire/protocol.zig");

pub const adoption = @import("./third_party/peer_third_party_adoption.zig");
pub const pending = @import("./third_party/peer_third_party_pending.zig");
pub const returns = @import("./third_party/peer_third_party_returns.zig");

pub const adoptThirdPartyAnswer = adoption.adoptThirdPartyAnswer;
pub const handleThirdPartyAnswer = adoption.handleThirdPartyAnswer;
pub const captureThirdPartyCompletionForPeer = adoption.captureThirdPartyCompletionForPeer;
pub const captureThirdPartyCompletionForPeerFn = adoption.captureThirdPartyCompletionForPeerFn;
pub const adoptPendingAwaitEntryForPeer = adoption.adoptPendingAwaitEntryForPeer;
pub const adoptPendingAwaitEntryForPeerFn = adoption.adoptPendingAwaitEntryForPeerFn;
pub const handleReturnAcceptFromThirdParty = adoption.handleReturnAcceptFromThirdParty;
pub const handleReturnAcceptFromThirdPartyForPeerFn = adoption.handleReturnAcceptFromThirdPartyForPeerFn;

pub const adoptPendingAwait = pending.adoptPendingAwait;
pub const takePendingAnswerId = pending.takePendingAnswerId;
pub const putPendingAnswer = pending.putPendingAnswer;
pub const putPendingAwait = pending.putPendingAwait;

pub const hasPendingReturn = returns.hasPendingReturn;
pub const hasPendingReturnForPeer = returns.hasPendingReturnForPeer;
pub const hasPendingReturnForPeerFn = returns.hasPendingReturnForPeerFn;
pub const bufferPendingReturn = returns.bufferPendingReturn;
pub const bufferPendingReturnForPeer = returns.bufferPendingReturnForPeer;
pub const bufferPendingReturnForPeerFn = returns.bufferPendingReturnForPeerFn;
pub const takePendingReturnFrame = returns.takePendingReturnFrame;
pub const handlePendingReturnFrame = returns.handlePendingReturnFrame;
pub const handlePendingReturnFrameForPeer = returns.handlePendingReturnFrameForPeer;
pub const handlePendingReturnFrameForPeerFn = returns.handlePendingReturnFrameForPeerFn;

pub const isThirdPartyAnswerId = adoption.isThirdPartyAnswerId;
pub const ForwardedReturnMode = peer_forwarded_return_logic.ForwardedReturnMode;

pub fn noteCallSendResults(
    comptime PeerType: type,
    peer: *PeerType,
    call: protocol.Call,
    note_send_results_to_yourself: *const fn (*PeerType, u32) anyerror!void,
    note_send_results_to_third_party: *const fn (*PeerType, u32, ?message.AnyPointerReader) anyerror!void,
) !void {
    switch (call.send_results_to.tag) {
        .caller => {},
        .yourself => {
            try note_send_results_to_yourself(peer, call.question_id);
        },
        .thirdParty => {
            try note_send_results_to_third_party(peer, call.question_id, call.send_results_to.third_party);
        },
    }
}

pub fn noteCallSendResultsForPeerFn(
    comptime PeerType: type,
    comptime note_send_results_to_yourself: *const fn (*PeerType, u32) anyerror!void,
    comptime note_send_results_to_third_party: *const fn (*PeerType, u32, ?message.AnyPointerReader) anyerror!void,
) *const fn (*PeerType, protocol.Call) anyerror!void {
    return struct {
        fn call(peer: *PeerType, rpc_call: protocol.Call) anyerror!void {
            try noteCallSendResults(
                PeerType,
                peer,
                rpc_call,
                note_send_results_to_yourself,
                note_send_results_to_third_party,
            );
        }
    }.call;
}

pub const ForwardedCallDestination = union(enum) {
    caller,
    yourself,
    third_party: ?[]u8,

    pub fn sendResultsToTag(self: ForwardedCallDestination) protocol.SendResultsToTag {
        return switch (self) {
            .caller => .caller,
            .yourself => .yourself,
            .third_party => .thirdParty,
        };
    }

    pub fn thirdPartyPayload(self: ForwardedCallDestination) ?[]u8 {
        return switch (self) {
            .third_party => |payload| payload,
            else => null,
        };
    }
};

pub fn buildForwardedCallDestination(
    comptime PeerType: type,
    peer: *PeerType,
    mode: ForwardedReturnMode,
    third_party: ?message.AnyPointerReader,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !ForwardedCallDestination {
    return switch (mode) {
        .translate_to_caller => .caller,
        .sent_elsewhere, .propagate_results_sent_elsewhere => .yourself,
        .propagate_accept_from_third_party => .{
            .third_party = try capture_payload(peer, third_party),
        },
    };
}

pub fn applyForwardedCallSendResults(
    comptime PeerType: type,
    peer: *PeerType,
    call_builder: *protocol.CallBuilder,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]const u8,
    set_third_party_from_payload: *const fn (*PeerType, *protocol.CallBuilder, []const u8) anyerror!void,
) !void {
    switch (send_results_to) {
        .caller => call_builder.setSendResultsToCaller(),
        .yourself => call_builder.setSendResultsToYourself(),
        .thirdParty => {
            if (send_results_to_third_party_payload) |payload| {
                try set_third_party_from_payload(peer, call_builder, payload);
            } else {
                try call_builder.setSendResultsToThirdPartyNull();
            }
        },
    }
}

pub fn setForwardedCallThirdPartyFromPayloadForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    call_builder: *protocol.CallBuilder,
    payload: []const u8,
) !void {
    var msg = try message.Message.initUnvalidated(peer.allocator, payload);
    defer msg.deinit();
    const third_party = try msg.getRootAnyPointer();
    try call_builder.setSendResultsToThirdParty(third_party);
}

pub fn setForwardedCallThirdPartyFromPayloadForPeerFn(
    comptime PeerType: type,
) *const fn (*PeerType, *protocol.CallBuilder, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, call_builder: *protocol.CallBuilder, payload: []const u8) anyerror!void {
            try setForwardedCallThirdPartyFromPayloadForPeer(
                PeerType,
                peer,
                call_builder,
                payload,
            );
        }
    }.call;
}

pub fn captureAnyPointerPayloadForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    ptr: ?message.AnyPointerReader,
    capture_payload: *const fn (std.mem.Allocator, ?message.AnyPointerReader) anyerror!?[]u8,
) !?[]u8 {
    return capture_payload(peer.allocator, ptr);
}

pub fn captureAnyPointerPayloadForPeerFn(
    comptime PeerType: type,
    comptime capture_payload: *const fn (std.mem.Allocator, ?message.AnyPointerReader) anyerror!?[]u8,
) *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8 {
    return struct {
        fn call(peer: *PeerType, ptr: ?message.AnyPointerReader) anyerror!?[]u8 {
            return try captureAnyPointerPayloadForPeer(
                PeerType,
                peer,
                ptr,
                capture_payload,
            );
        }
    }.call;
}

pub fn handleMissingReturnQuestion(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    answer_id: u32,
    is_third_party_answer_id: *const fn (u32) bool,
    has_pending_third_party_return: *const fn (*PeerType, u32) bool,
    buffer_pending_third_party_return: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    if (is_third_party_answer_id(answer_id)) {
        if (has_pending_third_party_return(peer, answer_id)) {
            return error.DuplicateThirdPartyReturn;
        }
        try buffer_pending_third_party_return(peer, answer_id, frame);
        return;
    }
    return error.UnknownQuestion;
}
