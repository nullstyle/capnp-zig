const std = @import("std");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");

/// Every builder here takes `release_param_caps` EXPLICITLY rather than
/// leaning on the rpc.capnp default (`true`).
///
/// `Return.releaseParamCaps = true` tells the caller its param capabilities
/// are released by this Return and forbids the callee from ever sending
/// separate `Release` messages for them. A vat that also releases those
/// imports explicitly — which this stack does, both from the post-dispatch
/// auto-release and from an application that kept a param cap and drops it
/// later — would spend the reference twice and a compliant peer aborts the
/// connection. So the flag is a property of the ANSWER, decided by the peer
/// (`Peer.returnReleasesParamCaps`), never a default that silently drifts out
/// of step with the Release frames.
pub fn buildReturnTagFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    tag: protocol.ReturnTag,
    release_param_caps: bool,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, tag);
    ret.setReleaseParamCaps(release_param_caps);
    return builder.finish();
}

pub fn buildReturnExceptionFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    reason: []const u8,
    ex_type: protocol.ExceptionType,
    release_param_caps: bool,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .exception);
    ret.setReleaseParamCaps(release_param_caps);
    try ret.setExceptionTyped(reason, ex_type);
    return builder.finish();
}

pub fn buildReturnTakeFromOtherQuestionFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    other_question_id: u32,
    release_param_caps: bool,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .takeFromOtherQuestion);
    ret.setReleaseParamCaps(release_param_caps);
    try ret.setTakeFromOtherQuestion(other_question_id);
    return builder.finish();
}

pub fn buildReturnAcceptFromThirdPartyFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    await_payload: ?[]const u8,
    release_param_caps: bool,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .awaitFromThirdParty);
    ret.setReleaseParamCaps(release_param_caps);
    if (await_payload) |payload| {
        var await_msg = try message.Message.initUnvalidated(allocator, payload);
        defer await_msg.deinit();
        const await_ptr = try await_msg.getRootAnyPointer();
        try ret.setAcceptFromThirdParty(await_ptr);
    } else {
        try ret.setAcceptFromThirdPartyNull();
    }

    return builder.finish();
}

test "peer_return_frames buildReturnTagFrame encodes requested tag" {
    const frame = try buildReturnTagFrame(std.testing.allocator, 70, .canceled, false);
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 70), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag);
    try std.testing.expectEqual(false, ret.release_param_caps);

    // The schema default (true) must still be reachable: an answer that took
    // no param-cap wire refs asks the caller to drop them implicitly.
    const implicit = try buildReturnTagFrame(std.testing.allocator, 70, .canceled, true);
    defer std.testing.allocator.free(implicit);
    var decoded_implicit = try protocol.DecodedMessage.init(std.testing.allocator, implicit);
    defer decoded_implicit.deinit();
    try std.testing.expectEqual(true, (try decoded_implicit.asReturn()).release_param_caps);
}

test "peer_return_frames buildReturnExceptionFrame encodes reason" {
    // `ex_type` is deliberately NOT `.failed` here: `failed` is ordinal 0, which
    // is also what an unwritten `Exception.type` field reads back as, so
    // asserting on it could not tell "the caller's type was encoded" from "the
    // field was never touched". `.overloaded` (ordinal 1) makes the round trip
    // observable while leaving the reason/tag/answer-id checks — this test's
    // original subject — exactly as they were.
    const frame = try buildReturnExceptionFrame(std.testing.allocator, 71, "bad", .overloaded, false);
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 71), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("bad", ex.reason);
    try std.testing.expectEqual(protocol.ExceptionType.overloaded, ex.kind());
    // An exception Return carries releaseParamCaps too: a failed call still
    // has to settle the caller's param grants exactly once.
    try std.testing.expectEqual(false, ret.release_param_caps);

    const implicit = try buildReturnExceptionFrame(std.testing.allocator, 71, "bad", .overloaded, true);
    defer std.testing.allocator.free(implicit);
    var decoded_implicit = try protocol.DecodedMessage.init(std.testing.allocator, implicit);
    defer decoded_implicit.deinit();
    try std.testing.expectEqual(true, (try decoded_implicit.asReturn()).release_param_caps);
}

test "peer_return_frames buildReturnTakeFromOtherQuestionFrame encodes referenced question id" {
    const frame = try buildReturnTakeFromOtherQuestionFrame(std.testing.allocator, 72, 900, false);
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 72), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.takeFromOtherQuestion, ret.tag);
    try std.testing.expectEqual(@as(u32, 900), ret.take_from_other_question orelse return error.MissingQuestionId);
    try std.testing.expectEqual(false, ret.release_param_caps);
}

test "peer_return_frames buildReturnAcceptFromThirdPartyFrame supports null and non-null payload" {
    const null_frame = try buildReturnAcceptFromThirdPartyFrame(std.testing.allocator, 73, null, false);
    defer std.testing.allocator.free(null_frame);

    var decoded_null = try protocol.DecodedMessage.init(std.testing.allocator, null_frame);
    defer decoded_null.deinit();
    const ret_null = try decoded_null.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.awaitFromThirdParty, ret_null.tag);
    try std.testing.expectEqual(false, ret_null.release_param_caps);
    const await_null = ret_null.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expect(await_null.isNull());

    var await_builder = message.MessageBuilder.init(std.testing.allocator);
    defer await_builder.deinit();
    const await_root = try await_builder.initRootAnyPointer();
    try await_root.setText("await-destination");
    const await_payload = try await_builder.toBytes();
    defer std.testing.allocator.free(await_payload);

    const non_null_frame = try buildReturnAcceptFromThirdPartyFrame(std.testing.allocator, 74, await_payload, false);
    defer std.testing.allocator.free(non_null_frame);

    var decoded_non_null = try protocol.DecodedMessage.init(std.testing.allocator, non_null_frame);
    defer decoded_non_null.deinit();
    const ret_non_null = try decoded_non_null.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.awaitFromThirdParty, ret_non_null.tag);
    const await_ptr = ret_non_null.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("await-destination", try await_ptr.getText());
}
