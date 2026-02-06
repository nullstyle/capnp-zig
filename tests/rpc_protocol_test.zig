const std = @import("std");
const capnpc = @import("capnpc-zig");

const protocol = capnpc.rpc.protocol;
const cap_table = capnpc.rpc.cap_table;
const message = capnpc.message;

test "RPC bootstrap return encodes cap table" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(123, .results);
    var any = try ret.getResultsAnyPointer();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTable(1);
    const entry = try cap_list.get(0);
    protocol.CapDescriptor.writeSenderHosted(entry, 42);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const ret_decoded = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 123), ret_decoded.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret_decoded.tag);

    const payload = ret_decoded.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, desc.tag);
    try std.testing.expectEqual(@as(u32, 42), desc.id.?);
}

test "InboundCapTable resolves senderHosted and receiverHosted" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(7, .results);
    var any = try ret.getResultsAnyPointer();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTable(2);
    const entry0 = try cap_list.get(0);
    protocol.CapDescriptor.writeSenderHosted(entry0, 5);
    const entry1 = try cap_list.get(1);
    protocol.CapDescriptor.writeReceiverHosted(entry1, 9);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, payload.cap_table, &caps);
    defer inbound.deinit();

    const resolved0 = try inbound.get(0);
    switch (resolved0) {
        .imported => |cap| try std.testing.expectEqual(@as(u32, 5), cap.id),
        else => return error.UnexpectedCapType,
    }

    const resolved1 = try inbound.get(1);
    switch (resolved1) {
        .exported => |cap| try std.testing.expectEqual(@as(u32, 9), cap.id),
        else => return error.UnexpectedCapType,
    }

    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());
}

test "RPC call promised answer encodes transform" {
    const allocator = std.testing.allocator;

    const ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .noop, .pointer_index = 0 },
        .{ .tag = .get_pointer_field, .pointer_index = 2 },
    };

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(9, 0x1234, 2);
    try call.setTargetPromisedAnswerWithOps(77, &ops);
    try call.setEmptyCapTable();

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    try std.testing.expectEqual(protocol.MessageTargetTag.promised_answer, call_decoded.target.tag);

    const promised = call_decoded.target.promised_answer orelse return error.MissingPromisedAnswer;
    try std.testing.expectEqual(@as(u32, 77), promised.question_id);
    try std.testing.expectEqual(@as(u32, ops.len), promised.transform.len());

    const op0 = try promised.transform.get(0);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.noop, op0.tag);
    const op1 = try promised.transform.get(1);
    try std.testing.expectEqual(protocol.PromisedAnswerOpTag.get_pointer_field, op1.tag);
    try std.testing.expectEqual(@as(u16, 2), op1.pointer_index);
}

test "RPC call sendResultsTo.yourself encodes and decodes" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(9, 0x1234, 2);
    try call.setTargetImportedCap(77);
    call.setSendResultsToYourself();
    try call.setEmptyCapTable();

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.yourself, call_decoded.send_results_to.tag);
}

test "RPC call sendResultsTo.thirdParty encodes and decodes" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(9, 0x1234, 2);
    try call.setTargetImportedCap(77);
    try call.setSendResultsToThirdPartyNull();
    try call.setEmptyCapTable();

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.third_party, call_decoded.send_results_to.tag);
}

test "RPC call sendResultsTo.thirdParty clones pointer payload" {
    const allocator = std.testing.allocator;

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("vat-hint");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);

    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var call = try builder.beginCall(9, 0xAA, 2);
    try call.setTargetImportedCap(3);
    try call.setSendResultsToThirdParty(third_ptr);
    try call.setEmptyCapTable();

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const call_decoded = try decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.third_party, call_decoded.send_results_to.tag);
    const payload = call_decoded.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
    const text = try payload.getText();
    try std.testing.expectEqualStrings("vat-hint", text);
}

test "RPC call sendResultsTo conformance covers caller yourself and thirdParty" {
    const allocator = std.testing.allocator;

    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();

        var call = try builder.beginCall(15, 0x33, 1);
        try call.setTargetImportedCap(2);
        call.setSendResultsToCaller();
        try call.setEmptyCapTable();

        const bytes = try builder.finish();
        defer allocator.free(bytes);
        var decoded = try protocol.DecodedMessage.init(allocator, bytes);
        defer decoded.deinit();
        const call_decoded = try decoded.asCall();
        try std.testing.expectEqual(protocol.SendResultsToTag.caller, call_decoded.send_results_to.tag);
    }

    {
        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();

        var call = try builder.beginCall(16, 0x33, 1);
        try call.setTargetImportedCap(2);
        call.setSendResultsToYourself();
        try call.setEmptyCapTable();

        const bytes = try builder.finish();
        defer allocator.free(bytes);
        var decoded = try protocol.DecodedMessage.init(allocator, bytes);
        defer decoded.deinit();
        const call_decoded = try decoded.asCall();
        try std.testing.expectEqual(protocol.SendResultsToTag.yourself, call_decoded.send_results_to.tag);
    }

    {
        var third_builder = message.MessageBuilder.init(allocator);
        defer third_builder.deinit();
        const third_root = try third_builder.initRootAnyPointer();
        try third_root.setText("conformance-third-party");
        const third_bytes = try third_builder.toBytes();
        defer allocator.free(third_bytes);
        var third_msg = try message.Message.init(allocator, third_bytes);
        defer third_msg.deinit();
        const third_ptr = try third_msg.getRootAnyPointer();

        var builder = protocol.MessageBuilder.init(allocator);
        defer builder.deinit();

        var call = try builder.beginCall(17, 0x33, 1);
        try call.setTargetImportedCap(2);
        try call.setSendResultsToThirdParty(third_ptr);
        try call.setEmptyCapTable();

        const bytes = try builder.finish();
        defer allocator.free(bytes);
        var decoded = try protocol.DecodedMessage.init(allocator, bytes);
        defer decoded.deinit();
        const call_decoded = try decoded.asCall();
        try std.testing.expectEqual(protocol.SendResultsToTag.third_party, call_decoded.send_results_to.tag);
        const payload = call_decoded.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
        try std.testing.expectEqualStrings("conformance-third-party", try payload.getText());
    }
}

test "InboundCapTable resolves receiverAnswer" {
    const allocator = std.testing.allocator;

    const ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .get_pointer_field, .pointer_index = 1 },
    };

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(7, .results);
    var any = try ret.getResultsAnyPointer();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTable(1);
    const entry0 = try cap_list.get(0);
    try protocol.CapDescriptor.writeReceiverAnswer(entry0, 55, &ops);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, payload.cap_table, &caps);
    defer inbound.deinit();

    const resolved = try inbound.get(0);
    switch (resolved) {
        .promised => |pa| {
            try std.testing.expectEqual(@as(u32, 55), pa.question_id);
            try std.testing.expectEqual(@as(u32, 1), pa.transform.len());
            const op = try pa.transform.get(0);
            try std.testing.expectEqual(protocol.PromisedAnswerOpTag.get_pointer_field, op.tag);
            try std.testing.expectEqual(@as(u16, 1), op.pointer_index);
        },
        else => return error.UnexpectedCapType,
    }

    try std.testing.expectEqual(@as(usize, 0), caps.imports.count());
}

test "InboundCapTable resolves thirdPartyHosted to vine import" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(8, .results);
    var any = try ret.getResultsAnyPointer();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTable(1);
    const entry0 = try cap_list.get(0);
    try protocol.CapDescriptor.writeThirdPartyHostedNull(entry0, 77);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const ret_decoded = try decoded.asReturn();
    const payload = ret_decoded.results orelse return error.MissingPayload;

    var caps = cap_table.CapTable.init(allocator);
    defer caps.deinit();

    var inbound = try cap_table.InboundCapTable.init(allocator, payload.cap_table, &caps);
    defer inbound.deinit();

    const resolved = try inbound.get(0);
    switch (resolved) {
        .imported => |cap| try std.testing.expectEqual(@as(u32, 77), cap.id),
        else => return error.UnexpectedCapType,
    }

    try std.testing.expectEqual(@as(usize, 1), caps.imports.count());
}

test "RPC resolve encodes cap descriptor" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    const descriptor = protocol.CapDescriptor{
        .tag = .sender_hosted,
        .id = 42,
    };

    try builder.buildResolveCap(9, descriptor);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const resolve = try decoded.asResolve();
    try std.testing.expectEqual(@as(u32, 9), resolve.promise_id);
    try std.testing.expectEqual(protocol.ResolveTag.cap, resolve.tag);

    const cap = resolve.cap orelse return error.MissingResolveCap;
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, cap.tag);
    try std.testing.expectEqual(@as(u32, 42), cap.id.?);
}

test "RPC resolve encodes thirdPartyHosted cap descriptor" {
    const allocator = std.testing.allocator;

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("contact-info");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);

    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    const descriptor = protocol.CapDescriptor{
        .tag = .third_party_hosted,
        .third_party = .{
            .id = third_ptr,
            .vine_id = 21,
        },
    };

    try builder.buildResolveCap(10, descriptor);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const resolve = try decoded.asResolve();
    try std.testing.expectEqual(@as(u32, 10), resolve.promise_id);
    try std.testing.expectEqual(protocol.ResolveTag.cap, resolve.tag);

    const cap = resolve.cap orelse return error.MissingResolveCap;
    try std.testing.expectEqual(protocol.CapDescriptorTag.third_party_hosted, cap.tag);
    const third = cap.third_party orelse return error.MissingThirdPartyCapDescriptor;
    try std.testing.expectEqual(@as(u32, 21), third.vine_id);
    const id_ptr = third.id orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("contact-info", try id_ptr.getText());
}

test "RPC resolve encodes exception" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    try builder.buildResolveException(11, "broken");

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const resolve = try decoded.asResolve();
    try std.testing.expectEqual(@as(u32, 11), resolve.promise_id);
    try std.testing.expectEqual(protocol.ResolveTag.exception, resolve.tag);

    const ex = resolve.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("broken", ex.reason);
}

test "RPC finish encodes releaseResultCaps and requireEarlyCancellation bits" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    try builder.buildFinish(29, true, false);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const finish = try decoded.asFinish();
    try std.testing.expectEqual(@as(u32, 29), finish.question_id);
    try std.testing.expect(finish.release_result_caps);
    try std.testing.expect(!finish.require_early_cancellation);
}

test "RPC return takeFromOtherQuestion encodes question id" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(17, .take_from_other_question);
    try ret.setTakeFromOtherQuestion(5);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const parsed = try decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 17), parsed.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, parsed.tag);
    try std.testing.expectEqual(@as(u32, 5), parsed.take_from_other_question.?);
}

test "RPC return acceptFromThirdParty clones pointer payload" {
    const allocator = std.testing.allocator;

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("await-vat");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);

    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(41, .accept_from_third_party);
    try ret.setAcceptFromThirdParty(third_ptr);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const parsed = try decoded.asReturn();
    try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, parsed.tag);

    const await_ptr = parsed.accept_from_third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("await-vat", try await_ptr.getText());
}

test "RPC disembargo sender loopback encodes target" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    const target = protocol.MessageTarget{
        .tag = .imported_cap,
        .imported_cap = 12,
        .promised_answer = null,
    };

    try builder.buildDisembargoSenderLoopback(target, 77);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const disembargo = try decoded.asDisembargo();
    try std.testing.expectEqual(protocol.DisembargoContextTag.sender_loopback, disembargo.context_tag);
    try std.testing.expectEqual(@as(u32, 77), disembargo.embargo_id.?);
    try std.testing.expectEqual(protocol.MessageTargetTag.imported_cap, disembargo.target.tag);
    try std.testing.expectEqual(@as(u32, 12), disembargo.target.imported_cap.?);
}

test "RPC disembargo sender loopback supports promisedAnswer target" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    const target = protocol.MessageTarget{
        .tag = .promised_answer,
        .imported_cap = null,
        .promised_answer = .{
            .question_id = 33,
            .transform = .{ .list = null },
        },
    };

    try builder.buildDisembargoSenderLoopback(target, 88);

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const disembargo = try decoded.asDisembargo();
    try std.testing.expectEqual(protocol.DisembargoContextTag.sender_loopback, disembargo.context_tag);
    try std.testing.expectEqual(@as(u32, 88), disembargo.embargo_id.?);
    try std.testing.expectEqual(protocol.MessageTargetTag.promised_answer, disembargo.target.tag);
    const promised = disembargo.target.promised_answer orelse return error.MissingPromisedAnswer;
    try std.testing.expectEqual(@as(u32, 33), promised.question_id);
    try std.testing.expectEqual(@as(u32, 0), promised.transform.len());
}

test "RPC disembargo accept encodes accept token" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    const target = protocol.MessageTarget{
        .tag = .imported_cap,
        .imported_cap = 21,
        .promised_answer = null,
    };

    try builder.buildDisembargoAccept(target, "accept-token");

    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    const disembargo = try decoded.asDisembargo();
    try std.testing.expectEqual(protocol.DisembargoContextTag.accept, disembargo.context_tag);
    try std.testing.expectEqualStrings("accept-token", disembargo.accept.?);
    try std.testing.expectEqual(protocol.MessageTargetTag.imported_cap, disembargo.target.tag);
    try std.testing.expectEqual(@as(u32, 21), disembargo.target.imported_cap.?);
}

test "RPC provide encodes and decodes" {
    const allocator = std.testing.allocator;

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);
    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildProvide(
        60,
        .{
            .tag = .imported_cap,
            .imported_cap = 11,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.provide, decoded.tag);

    const provide = try decoded.asProvide();
    try std.testing.expectEqual(@as(u32, 60), provide.question_id);
    try std.testing.expectEqual(protocol.MessageTargetTag.imported_cap, provide.target.tag);
    try std.testing.expectEqual(@as(u32, 11), provide.target.imported_cap.?);
    const recipient = provide.recipient orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("recipient", try recipient.getText());
}

test "RPC accept encodes and decodes" {
    const allocator = std.testing.allocator;

    var provision_builder = message.MessageBuilder.init(allocator);
    defer provision_builder.deinit();
    const provision_root = try provision_builder.initRootAnyPointer();
    try provision_root.setText("provision");
    const provision_bytes = try provision_builder.toBytes();
    defer allocator.free(provision_bytes);
    var provision_msg = try message.Message.init(allocator, provision_bytes);
    defer provision_msg.deinit();
    const provision_ptr = try provision_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAccept(61, provision_ptr, "embargo-id");
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.accept, decoded.tag);

    const accept = try decoded.asAccept();
    try std.testing.expectEqual(@as(u32, 61), accept.question_id);
    const provision = accept.provision orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("provision", try provision.getText());
    try std.testing.expectEqualStrings("embargo-id", accept.embargo.?);
}

test "RPC thirdPartyAnswer encodes and decodes" {
    const allocator = std.testing.allocator;

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildThirdPartyAnswer(62, completion_ptr);
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.third_party_answer, decoded.tag);

    const answer = try decoded.asThirdPartyAnswer();
    try std.testing.expectEqual(@as(u32, 62), answer.answer_id);
    const completion = answer.completion orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("completion", try completion.getText());
}

test "RPC join encodes and decodes" {
    const allocator = std.testing.allocator;

    var key_part_builder = message.MessageBuilder.init(allocator);
    defer key_part_builder.deinit();
    const key_part_root = try key_part_builder.initRootAnyPointer();
    try key_part_root.setText("join-key-part");
    const key_part_bytes = try key_part_builder.toBytes();
    defer allocator.free(key_part_bytes);
    var key_part_msg = try message.Message.init(allocator, key_part_bytes);
    defer key_part_msg.deinit();
    const key_part_ptr = try key_part_msg.getRootAnyPointer();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildJoin(
        63,
        .{
            .tag = .imported_cap,
            .imported_cap = 7,
            .promised_answer = null,
        },
        key_part_ptr,
    );
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.join, decoded.tag);

    const join = try decoded.asJoin();
    try std.testing.expectEqual(@as(u32, 63), join.question_id);
    try std.testing.expectEqual(protocol.MessageTargetTag.imported_cap, join.target.tag);
    try std.testing.expectEqual(@as(u32, 7), join.target.imported_cap.?);
    const key_part = join.key_part orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("join-key-part", try key_part.getText());
}

test "RPC message tag ordinals match Cap'n Proto rpc.capnp" {
    try std.testing.expectEqual(@as(u16, 0), @intFromEnum(protocol.MessageTag.unimplemented));
    try std.testing.expectEqual(@as(u16, 1), @intFromEnum(protocol.MessageTag.abort));
    try std.testing.expectEqual(@as(u16, 2), @intFromEnum(protocol.MessageTag.call));
    try std.testing.expectEqual(@as(u16, 3), @intFromEnum(protocol.MessageTag.return_));
    try std.testing.expectEqual(@as(u16, 4), @intFromEnum(protocol.MessageTag.finish));
    try std.testing.expectEqual(@as(u16, 5), @intFromEnum(protocol.MessageTag.resolve));
    try std.testing.expectEqual(@as(u16, 6), @intFromEnum(protocol.MessageTag.release));
    try std.testing.expectEqual(@as(u16, 7), @intFromEnum(protocol.MessageTag.obsolete_save));
    try std.testing.expectEqual(@as(u16, 8), @intFromEnum(protocol.MessageTag.bootstrap));
    try std.testing.expectEqual(@as(u16, 9), @intFromEnum(protocol.MessageTag.obsolete_delete));
    try std.testing.expectEqual(@as(u16, 10), @intFromEnum(protocol.MessageTag.provide));
    try std.testing.expectEqual(@as(u16, 11), @intFromEnum(protocol.MessageTag.accept));
    try std.testing.expectEqual(@as(u16, 12), @intFromEnum(protocol.MessageTag.join));
    try std.testing.expectEqual(@as(u16, 13), @intFromEnum(protocol.MessageTag.disembargo));
    try std.testing.expectEqual(@as(u16, 14), @intFromEnum(protocol.MessageTag.third_party_answer));
}

test "RPC decoded message recognizes every defined message tag discriminant" {
    const allocator = std.testing.allocator;

    const tags = [_]protocol.MessageTag{
        .unimplemented,
        .abort,
        .call,
        .return_,
        .finish,
        .resolve,
        .release,
        .obsolete_save,
        .bootstrap,
        .obsolete_delete,
        .provide,
        .accept,
        .join,
        .disembargo,
        .third_party_answer,
    };

    for (tags) |tag| {
        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();
        var root = try builder.allocateStruct(1, 1);
        root.writeUnionDiscriminant(0, @intFromEnum(tag));
        const bytes = try builder.toBytes();
        defer allocator.free(bytes);

        var decoded = try protocol.DecodedMessage.init(allocator, bytes);
        defer decoded.deinit();
        try std.testing.expectEqual(tag, decoded.tag);
    }
}

test "RPC abort encodes and decodes exception" {
    const allocator = std.testing.allocator;

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    try builder.buildAbort("remote-failed");
    const bytes = try builder.finish();
    defer allocator.free(bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();

    try std.testing.expectEqual(protocol.MessageTag.abort, decoded.tag);
    const abort_msg = try decoded.asAbort();
    try std.testing.expectEqualStrings("remote-failed", abort_msg.exception.reason);
}

test "RPC unimplemented wraps nested message" {
    const allocator = std.testing.allocator;

    var inner_builder = protocol.MessageBuilder.init(allocator);
    defer inner_builder.deinit();
    var inner_call = try inner_builder.beginCall(44, 0xAA, 9);
    try inner_call.setTargetImportedCap(123);
    try inner_call.setEmptyCapTable();
    const inner_bytes = try inner_builder.finish();
    defer allocator.free(inner_bytes);

    var inner_msg = try message.Message.init(allocator, inner_bytes);
    defer inner_msg.deinit();
    const inner_root = try inner_msg.getRootAnyPointer();

    var outer_builder = protocol.MessageBuilder.init(allocator);
    defer outer_builder.deinit();
    try outer_builder.buildUnimplementedFromAnyPointer(inner_root);
    const outer_bytes = try outer_builder.finish();
    defer allocator.free(outer_bytes);

    var decoded = try protocol.DecodedMessage.init(allocator, outer_bytes);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.unimplemented, decoded.tag);

    const unimplemented = try decoded.asUnimplemented();
    try std.testing.expectEqual(protocol.MessageTag.call, unimplemented.message_tag.?);
    try std.testing.expectEqual(@as(u32, 44), unimplemented.question_id.?);
}

test "RPC DecodedMessage rejects invalid message tag discriminant" {
    const allocator = std.testing.allocator;

    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();
    var root = try builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xffff);
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    try std.testing.expectError(error.InvalidMessageTag, protocol.DecodedMessage.init(allocator, bytes));
}

test "RPC protocol fuzz malformed frames does not crash decode" {
    const allocator = std.testing.allocator;

    var prng = std.Random.DefaultPrng.init(0xE4D8_1187_9A4F_2231);
    const random = prng.random();

    var i: usize = 0;
    while (i < 1024) : (i += 1) {
        const len = random.uintLessThan(usize, 220);
        const frame = try allocator.alloc(u8, len);
        defer allocator.free(frame);
        random.bytes(frame);

        var decoded = protocol.DecodedMessage.init(allocator, frame) catch continue;
        defer decoded.deinit();

        switch (decoded.tag) {
            .unimplemented => _ = decoded.asUnimplemented() catch {},
            .abort => _ = decoded.asAbort() catch {},
            .call => _ = decoded.asCall() catch {},
            .return_ => _ = decoded.asReturn() catch {},
            .finish => _ = decoded.asFinish() catch {},
            .resolve => _ = decoded.asResolve() catch {},
            .release => _ = decoded.asRelease() catch {},
            .bootstrap => _ = decoded.asBootstrap() catch {},
            .provide => _ = decoded.asProvide() catch {},
            .accept => _ = decoded.asAccept() catch {},
            .join => _ = decoded.asJoin() catch {},
            .disembargo => _ = decoded.asDisembargo() catch {},
            .third_party_answer => _ = decoded.asThirdPartyAnswer() catch {},
            .obsolete_save, .obsolete_delete => {},
        }
    }
}
