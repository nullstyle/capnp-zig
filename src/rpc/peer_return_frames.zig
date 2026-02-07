const std = @import("std");
const message = @import("../message.zig");
const protocol = @import("protocol.zig");

pub fn buildReturnTagFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    tag: protocol.ReturnTag,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.beginReturn(answer_id, tag);
    return builder.finish();
}

pub fn buildReturnExceptionFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    reason: []const u8,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .exception);
    try ret.setException(reason);
    return builder.finish();
}

pub fn buildReturnTakeFromOtherQuestionFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    other_question_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .take_from_other_question);
    try ret.setTakeFromOtherQuestion(other_question_id);
    return builder.finish();
}

pub fn buildReturnAcceptFromThirdPartyFrame(
    allocator: std.mem.Allocator,
    answer_id: u32,
    await_payload: ?[]const u8,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(answer_id, .accept_from_third_party);
    if (await_payload) |payload| {
        var await_msg = try message.Message.init(allocator, payload);
        defer await_msg.deinit();
        const await_ptr = try await_msg.getRootAnyPointer();
        try ret.setAcceptFromThirdParty(await_ptr);
    } else {
        try ret.setAcceptFromThirdPartyNull();
    }

    return builder.finish();
}

test "peer_return_frames buildReturnTagFrame encodes requested tag" {
    const frame = try buildReturnTagFrame(std.testing.allocator, 70, .canceled);
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 70), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag);
}

test "peer_return_frames buildReturnExceptionFrame encodes reason" {
    const frame = try buildReturnExceptionFrame(std.testing.allocator, 71, "bad");
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 71), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("bad", ex.reason);
}

test "peer_return_frames buildReturnTakeFromOtherQuestionFrame encodes referenced question id" {
    const frame = try buildReturnTakeFromOtherQuestionFrame(std.testing.allocator, 72, 900);
    defer std.testing.allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(std.testing.allocator, frame);
    defer decoded.deinit();
    const ret = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 72), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, ret.tag);
    try std.testing.expectEqual(@as(u32, 900), ret.take_from_other_question orelse return error.MissingQuestionId);
}

test "peer_return_frames buildReturnAcceptFromThirdPartyFrame supports null and non-null payload" {
    const null_frame = try buildReturnAcceptFromThirdPartyFrame(std.testing.allocator, 73, null);
    defer std.testing.allocator.free(null_frame);

    var decoded_null = try protocol.DecodedMessage.init(std.testing.allocator, null_frame);
    defer decoded_null.deinit();
    const ret_null = try decoded_null.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret_null.tag);
    const await_null = ret_null.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expect(await_null.isNull());

    var await_builder = message.MessageBuilder.init(std.testing.allocator);
    defer await_builder.deinit();
    const await_root = try await_builder.initRootAnyPointer();
    try await_root.setText("await-destination");
    const await_payload = try await_builder.toBytes();
    defer std.testing.allocator.free(await_payload);

    const non_null_frame = try buildReturnAcceptFromThirdPartyFrame(std.testing.allocator, 74, await_payload);
    defer std.testing.allocator.free(non_null_frame);

    var decoded_non_null = try protocol.DecodedMessage.init(std.testing.allocator, non_null_frame);
    defer decoded_non_null.deinit();
    const ret_non_null = try decoded_non_null.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret_non_null.tag);
    const await_ptr = ret_non_null.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("await-destination", try await_ptr.getText());
}
