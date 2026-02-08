const std = @import("std");
const message = @import("../message.zig");

pub const MessageTag = enum(u16) {
    unimplemented = 0,
    abort = 1,
    call = 2,
    return_ = 3,
    finish = 4,
    resolve = 5,
    release = 6,
    obsolete_save = 7,
    bootstrap = 8,
    obsolete_delete = 9,
    provide = 10,
    accept = 11,
    join = 12,
    disembargo = 13,
    third_party_answer = 14,
};

pub const ReturnTag = enum(u16) {
    results = 0,
    exception = 1,
    canceled = 2,
    results_sent_elsewhere = 3,
    take_from_other_question = 4,
    accept_from_third_party = 5,
};

pub const MessageTargetTag = enum(u16) {
    imported_cap = 0,
    promised_answer = 1,
};

pub const PromisedAnswerOpTag = enum(u16) {
    noop = 0,
    get_pointer_field = 1,
};

pub const CapDescriptorTag = enum(u16) {
    none = 0,
    sender_hosted = 1,
    sender_promise = 2,
    receiver_hosted = 3,
    receiver_answer = 4,
    third_party_hosted = 5,
};

pub const SendResultsToTag = enum(u16) {
    caller = 0,
    yourself = 1,
    third_party = 2,
};

pub const ResolveTag = enum(u16) {
    cap = 0,
    exception = 1,
};

pub const DisembargoContextTag = enum(u16) {
    sender_loopback = 0,
    receiver_loopback = 1,
    accept = 2,
};

pub const CapDescriptor = struct {
    tag: CapDescriptorTag,
    id: ?u32 = null,
    promised_answer: ?PromisedAnswer = null,
    third_party: ?ThirdPartyCapDescriptor = null,
    attached_fd: ?u8 = null,

    pub fn fromReader(reader: message.StructReader) !CapDescriptor {
        const tag_value = reader.readUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES);
        const tag: CapDescriptorTag = @enumFromInt(tag_value);
        var id: ?u32 = null;
        var promised_answer: ?PromisedAnswer = null;
        var third_party: ?ThirdPartyCapDescriptor = null;
        var attached_fd: ?u8 = null;
        switch (tag) {
            .sender_hosted, .sender_promise, .receiver_hosted => {
                id = reader.readU32(byteOffsetU32(CAP_DESCRIPTOR_ID_OFFSET));
            },
            .receiver_answer => {
                const pa_reader = try reader.readStruct(CAP_DESCRIPTOR_PTR);
                promised_answer = try PromisedAnswer.fromReader(pa_reader);
            },
            .third_party_hosted => {
                const third_reader = try reader.readStruct(CAP_DESCRIPTOR_PTR);
                third_party = try ThirdPartyCapDescriptor.fromReader(third_reader);
            },
            .none => {},
        }
        const fd_value = reader.readU8(byteOffsetU16(CAP_DESCRIPTOR_ATTACHED_FD_OFFSET));
        if (fd_value != 0) {
            attached_fd = fd_value;
        }
        return .{
            .tag = tag,
            .id = id,
            .promised_answer = promised_answer,
            .third_party = third_party,
            .attached_fd = attached_fd,
        };
    }

    pub fn writeSenderHosted(builder: message.StructBuilder, id: u32) void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.sender_hosted));
        builder.writeU32(byteOffsetU32(CAP_DESCRIPTOR_ID_OFFSET), id);
    }

    pub fn writeSenderPromise(builder: message.StructBuilder, id: u32) void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.sender_promise));
        builder.writeU32(byteOffsetU32(CAP_DESCRIPTOR_ID_OFFSET), id);
    }

    pub fn writeReceiverHosted(builder: message.StructBuilder, id: u32) void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.receiver_hosted));
        builder.writeU32(byteOffsetU32(CAP_DESCRIPTOR_ID_OFFSET), id);
    }

    pub fn writeReceiverAnswer(builder: message.StructBuilder, question_id: u32, ops: []const PromisedAnswerOp) !void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.receiver_answer));
        const promised = try builder.initStruct(CAP_DESCRIPTOR_PTR, PROMISED_ANSWER_DATA_WORDS, PROMISED_ANSWER_POINTER_WORDS);
        try writePromisedAnswerOps(promised, question_id, ops);
    }

    pub fn writeThirdPartyHostedNull(builder: message.StructBuilder, vine_id: u32) !void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.third_party_hosted));
        const third = try builder.initStruct(
            CAP_DESCRIPTOR_PTR,
            THIRD_PARTY_CAP_DESCRIPTOR_DATA_WORDS,
            THIRD_PARTY_CAP_DESCRIPTOR_POINTER_WORDS,
        );
        third.writeU32(byteOffsetU32(THIRD_PARTY_CAP_DESCRIPTOR_VINE_ID_OFFSET), vine_id);
        var id_any = try third.getAnyPointer(THIRD_PARTY_CAP_DESCRIPTOR_ID_PTR);
        try id_any.setNull();
    }

    pub fn writeThirdPartyHosted(
        builder: message.StructBuilder,
        third_party_id: message.AnyPointerReader,
        vine_id: u32,
    ) !void {
        builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.third_party_hosted));
        const third = try builder.initStruct(
            CAP_DESCRIPTOR_PTR,
            THIRD_PARTY_CAP_DESCRIPTOR_DATA_WORDS,
            THIRD_PARTY_CAP_DESCRIPTOR_POINTER_WORDS,
        );
        third.writeU32(byteOffsetU32(THIRD_PARTY_CAP_DESCRIPTOR_VINE_ID_OFFSET), vine_id);
        const id_any = try third.getAnyPointer(THIRD_PARTY_CAP_DESCRIPTOR_ID_PTR);
        try message.cloneAnyPointer(third_party_id, id_any);
    }
};

pub const ThirdPartyCapDescriptor = struct {
    id: ?message.AnyPointerReader,
    vine_id: u32,

    fn fromReader(reader: message.StructReader) !ThirdPartyCapDescriptor {
        const id_ptr = reader.readAnyPointer(THIRD_PARTY_CAP_DESCRIPTOR_ID_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        return .{
            .id = id_ptr,
            .vine_id = reader.readU32(byteOffsetU32(THIRD_PARTY_CAP_DESCRIPTOR_VINE_ID_OFFSET)),
        };
    }
};

const MESSAGE_DATA_WORDS: u16 = 1;
const MESSAGE_POINTER_WORDS: u16 = 1;
const MESSAGE_DISCRIMINANT_OFFSET_BYTES: usize = 0;

const BOOTSTRAP_DATA_WORDS: u16 = 1;
const BOOTSTRAP_POINTER_WORDS: u16 = 1;
const BOOTSTRAP_QUESTION_ID_OFFSET: u32 = 0;
const BOOTSTRAP_DEPRECATED_OBJECT_PTR: usize = 0;

const CALL_DATA_WORDS: u16 = 3;
const CALL_POINTER_WORDS: u16 = 3;
const CALL_QUESTION_ID_OFFSET: u32 = 0;
const CALL_INTERFACE_ID_OFFSET: u32 = 1;
const CALL_METHOD_ID_OFFSET: u32 = 2;
const CALL_ALLOW_THIRD_PARTY_BIT: u32 = 128;
const CALL_NO_PROMISE_BIT: u32 = 129;
const CALL_ONLY_PROMISE_BIT: u32 = 130;
const CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES: usize = 6;
const CALL_TARGET_PTR: usize = 0;
const CALL_PARAMS_PTR: usize = 1;
const CALL_SEND_RESULTS_TO_THIRD_PARTY_PTR: usize = 2;

const RETURN_DATA_WORDS: u16 = 2;
const RETURN_POINTER_WORDS: u16 = 1;
const RETURN_DISCRIMINANT_OFFSET_BYTES: usize = 6; // discriminant_offset=3 words
const RETURN_ANSWER_ID_OFFSET: u32 = 0;
const RETURN_RELEASE_PARAM_CAPS_BIT: u32 = 32;
const RETURN_NO_FINISH_BIT: u32 = 33;
const RETURN_TAKE_FROM_OTHER_Q_OFFSET: u32 = 2;
const RETURN_RESULTS_PTR: usize = 0;

const FINISH_DATA_WORDS: u16 = 1;
const FINISH_POINTER_WORDS: u16 = 0;
const FINISH_QUESTION_ID_OFFSET: u32 = 0;
const FINISH_RELEASE_RESULT_CAPS_BIT: u32 = 32;
const FINISH_REQUIRE_EARLY_CANCEL_BIT: u32 = 33;

const MESSAGE_TARGET_DATA_WORDS: u16 = 1;
const MESSAGE_TARGET_POINTER_WORDS: u16 = 1;
const MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES: usize = 4; // discriminant_offset=2 words
const MESSAGE_TARGET_IMPORTED_CAP_OFFSET: u32 = 0;
const MESSAGE_TARGET_PROMISED_ANSWER_PTR: usize = 0;

const PROMISED_ANSWER_DATA_WORDS: u16 = 1;
const PROMISED_ANSWER_POINTER_WORDS: u16 = 1;
const PROMISED_ANSWER_QUESTION_ID_OFFSET: u32 = 0;
const PROMISED_ANSWER_TRANSFORM_PTR: usize = 0;

const PROMISED_ANSWER_OP_DATA_WORDS: u16 = 1;
const PROMISED_ANSWER_OP_POINTER_WORDS: u16 = 0;
const PROMISED_ANSWER_OP_DISCRIMINANT_OFFSET_BYTES: usize = 0;
const PROMISED_ANSWER_OP_GET_POINTER_FIELD_OFFSET_BYTES: usize = 2;

const RELEASE_DATA_WORDS: u16 = 1;
const RELEASE_POINTER_WORDS: u16 = 0;
const RELEASE_ID_OFFSET: u32 = 0;
const RELEASE_REFERENCE_COUNT_OFFSET: u32 = 1;

const RESOLVE_DATA_WORDS: u16 = 1;
const RESOLVE_POINTER_WORDS: u16 = 1;
const RESOLVE_DISCRIMINANT_OFFSET_BYTES: usize = 4;
const RESOLVE_PROMISE_ID_OFFSET: u32 = 0;
const RESOLVE_CAP_PTR: usize = 0;

const DISEMBARGO_DATA_WORDS: u16 = 1;
const DISEMBARGO_POINTER_WORDS: u16 = 2;
const DISEMBARGO_DISCRIMINANT_OFFSET_BYTES: usize = 4;
const DISEMBARGO_EMBARGO_ID_OFFSET: u32 = 0;
const DISEMBARGO_TARGET_PTR: usize = 0;
const DISEMBARGO_ACCEPT_PTR: usize = 1;

const PROVIDE_DATA_WORDS: u16 = 1;
const PROVIDE_POINTER_WORDS: u16 = 2;
const PROVIDE_QUESTION_ID_OFFSET: u32 = 0;
const PROVIDE_TARGET_PTR: usize = 0;
const PROVIDE_RECIPIENT_PTR: usize = 1;

const ACCEPT_DATA_WORDS: u16 = 1;
const ACCEPT_POINTER_WORDS: u16 = 2;
const ACCEPT_QUESTION_ID_OFFSET: u32 = 0;
const ACCEPT_PROVISION_PTR: usize = 0;
const ACCEPT_EMBARGO_PTR: usize = 1;

const THIRD_PARTY_ANSWER_DATA_WORDS: u16 = 1;
const THIRD_PARTY_ANSWER_POINTER_WORDS: u16 = 1;
const THIRD_PARTY_ANSWER_COMPLETION_PTR: usize = 0;
const THIRD_PARTY_ANSWER_ANSWER_ID_OFFSET: u32 = 0;

const JOIN_DATA_WORDS: u16 = 1;
const JOIN_POINTER_WORDS: u16 = 2;
const JOIN_QUESTION_ID_OFFSET: u32 = 0;
const JOIN_TARGET_PTR: usize = 0;
const JOIN_KEY_PART_PTR: usize = 1;

pub const PAYLOAD_DATA_WORDS: u16 = 0;
pub const PAYLOAD_POINTER_WORDS: u16 = 2;
pub const PAYLOAD_CONTENT_PTR: usize = 0;
pub const PAYLOAD_CAP_TABLE_PTR: usize = 1;

pub const CAP_DESCRIPTOR_DATA_WORDS: u16 = 1;
pub const CAP_DESCRIPTOR_POINTER_WORDS: u16 = 1;
const CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES: usize = 0;
const CAP_DESCRIPTOR_ID_OFFSET: u32 = 1;
const CAP_DESCRIPTOR_PTR: usize = 0;
const CAP_DESCRIPTOR_ATTACHED_FD_OFFSET: u32 = 2;

const THIRD_PARTY_CAP_DESCRIPTOR_DATA_WORDS: u16 = 1;
const THIRD_PARTY_CAP_DESCRIPTOR_POINTER_WORDS: u16 = 1;
const THIRD_PARTY_CAP_DESCRIPTOR_VINE_ID_OFFSET: u32 = 0;
const THIRD_PARTY_CAP_DESCRIPTOR_ID_PTR: usize = 0;

const EXCEPTION_DATA_WORDS: u16 = 1;
const EXCEPTION_POINTER_WORDS: u16 = 2;
const EXCEPTION_REASON_PTR: usize = 0;
const EXCEPTION_TRACE_PTR: usize = 1;
const EXCEPTION_TYPE_OFFSET: u32 = 2;

fn byteOffsetU16(offset: u32) usize {
    return @as(usize, @intCast(offset)) * 2;
}

fn byteOffsetU32(offset: u32) usize {
    return @as(usize, @intCast(offset)) * 4;
}

fn byteOffsetU64(offset: u32) usize {
    return @as(usize, @intCast(offset)) * 8;
}

fn byteOffsetBool(bit_offset: u32) struct { byte: usize, bit: u3 } {
    return .{
        .byte = @as(usize, @intCast(bit_offset / 8)),
        .bit = @as(u3, @truncate(bit_offset % 8)),
    };
}

pub const DecodedMessage = struct {
    msg: message.Message,
    tag: MessageTag,

    pub fn init(allocator: std.mem.Allocator, frame: []const u8) !DecodedMessage {
        var msg = try message.Message.init(allocator, frame);
        errdefer msg.deinit();
        const root = try msg.getRootStruct();
        const disc = root.readUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES);
        const tag = std.meta.intToEnum(MessageTag, disc) catch return error.InvalidMessageTag;
        return .{ .msg = msg, .tag = tag };
    }

    pub fn deinit(self: *DecodedMessage) void {
        self.msg.deinit();
    }

    pub fn asBootstrap(self: *DecodedMessage) !Bootstrap {
        if (self.tag != .bootstrap) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Bootstrap.fromReader(reader);
    }

    pub fn asUnimplemented(self: *DecodedMessage) !Unimplemented {
        if (self.tag != .unimplemented) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Unimplemented.fromReader(reader);
    }

    pub fn asAbort(self: *DecodedMessage) !Abort {
        if (self.tag != .abort) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Abort.fromReader(reader);
    }

    pub fn asCall(self: *DecodedMessage) !Call {
        if (self.tag != .call) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Call.fromReader(reader);
    }

    pub fn asReturn(self: *DecodedMessage) !Return {
        if (self.tag != .return_) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Return.fromReader(reader);
    }

    pub fn asFinish(self: *DecodedMessage) !Finish {
        if (self.tag != .finish) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Finish.fromReader(reader);
    }

    pub fn asRelease(self: *DecodedMessage) !Release {
        if (self.tag != .release) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Release.fromReader(reader);
    }

    pub fn asResolve(self: *DecodedMessage) !Resolve {
        if (self.tag != .resolve) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Resolve.fromReader(reader);
    }

    pub fn asDisembargo(self: *DecodedMessage) !Disembargo {
        if (self.tag != .disembargo) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Disembargo.fromReader(reader);
    }

    pub fn asProvide(self: *DecodedMessage) !Provide {
        if (self.tag != .provide) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Provide.fromReader(reader);
    }

    pub fn asAccept(self: *DecodedMessage) !Accept {
        if (self.tag != .accept) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Accept.fromReader(reader);
    }

    pub fn asThirdPartyAnswer(self: *DecodedMessage) !ThirdPartyAnswer {
        if (self.tag != .third_party_answer) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return ThirdPartyAnswer.fromReader(reader);
    }

    pub fn asJoin(self: *DecodedMessage) !Join {
        if (self.tag != .join) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Join.fromReader(reader);
    }
};

pub const Bootstrap = struct {
    question_id: u32,
    deprecated_object: ?message.AnyPointerReader,

    fn fromReader(reader: message.StructReader) !Bootstrap {
        const question_id = reader.readU32(byteOffsetU32(BOOTSTRAP_QUESTION_ID_OFFSET));
        const object_ptr = reader.readAnyPointer(BOOTSTRAP_DEPRECATED_OBJECT_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        return .{ .question_id = question_id, .deprecated_object = object_ptr };
    }
};

pub const Unimplemented = struct {
    message_tag: ?MessageTag,
    question_id: ?u32,

    fn fromReader(reader: message.StructReader) !Unimplemented {
        const nested_disc = reader.readUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES);
        const message_tag = std.meta.intToEnum(MessageTag, nested_disc) catch null;
        var question_id: ?u32 = null;

        if (message_tag == .bootstrap or message_tag == .call) {
            const nested = reader.readStruct(0) catch |err| switch (err) {
                error.OutOfBounds, error.InvalidPointer => null,
                else => return err,
            };
            if (nested) |nested_struct| {
                question_id = switch (message_tag.?) {
                    .bootstrap => nested_struct.readU32(byteOffsetU32(BOOTSTRAP_QUESTION_ID_OFFSET)),
                    .call => nested_struct.readU32(byteOffsetU32(CALL_QUESTION_ID_OFFSET)),
                    else => null,
                };
            }
        }

        return .{
            .message_tag = message_tag,
            .question_id = question_id,
        };
    }
};

pub const Abort = struct {
    exception: Exception,

    fn fromReader(reader: message.StructReader) !Abort {
        return .{
            .exception = try Exception.fromReader(reader),
        };
    }
};

pub const Call = struct {
    question_id: u32,
    interface_id: u64,
    method_id: u16,
    allow_third_party_tail: bool,
    no_promise_pipelining: bool,
    only_promise_pipeline: bool,
    send_results_to: SendResultsTo,
    target: MessageTarget,
    params: Payload,

    fn fromReader(reader: message.StructReader) !Call {
        const question_id = reader.readU32(byteOffsetU32(CALL_QUESTION_ID_OFFSET));
        const interface_id = reader.readU64(byteOffsetU64(CALL_INTERFACE_ID_OFFSET));
        const method_id = reader.readU16(byteOffsetU16(CALL_METHOD_ID_OFFSET));

        const allow_bits = byteOffsetBool(CALL_ALLOW_THIRD_PARTY_BIT);
        const no_promise_bits = byteOffsetBool(CALL_NO_PROMISE_BIT);
        const only_promise_bits = byteOffsetBool(CALL_ONLY_PROMISE_BIT);

        const allow_third_party_tail = reader.readBool(allow_bits.byte, allow_bits.bit);
        const no_promise_pipelining = reader.readBool(no_promise_bits.byte, no_promise_bits.bit);
        const only_promise_pipeline = reader.readBool(only_promise_bits.byte, only_promise_bits.bit);
        const send_results_to = try SendResultsTo.fromReader(reader);

        const target_reader = try reader.readStruct(CALL_TARGET_PTR);
        const params_reader = try reader.readStruct(CALL_PARAMS_PTR);

        return .{
            .question_id = question_id,
            .interface_id = interface_id,
            .method_id = method_id,
            .allow_third_party_tail = allow_third_party_tail,
            .no_promise_pipelining = no_promise_pipelining,
            .only_promise_pipeline = only_promise_pipeline,
            .send_results_to = send_results_to,
            .target = try MessageTarget.fromReader(target_reader),
            .params = try Payload.fromReader(params_reader),
        };
    }
};

pub const SendResultsTo = struct {
    tag: SendResultsToTag,
    third_party: ?message.AnyPointerReader = null,

    fn fromReader(reader: message.StructReader) !SendResultsTo {
        const tag_value = reader.readUnionDiscriminant(CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES);
        const tag: SendResultsToTag = @enumFromInt(tag_value);

        var third_party: ?message.AnyPointerReader = null;
        if (tag == .third_party) {
            third_party = reader.readAnyPointer(CALL_SEND_RESULTS_TO_THIRD_PARTY_PTR) catch |err| switch (err) {
                error.OutOfBounds => null,
                else => return err,
            };
        }

        return .{
            .tag = tag,
            .third_party = third_party,
        };
    }
};

pub const Return = struct {
    answer_id: u32,
    release_param_caps: bool,
    no_finish_needed: bool,
    tag: ReturnTag,
    results: ?Payload,
    exception: ?Exception,
    take_from_other_question: ?u32,
    accept_from_third_party: ?message.AnyPointerReader = null,

    fn fromReader(reader: message.StructReader) !Return {
        const answer_id = reader.readU32(byteOffsetU32(RETURN_ANSWER_ID_OFFSET));
        const release_bits = byteOffsetBool(RETURN_RELEASE_PARAM_CAPS_BIT);
        const no_finish_bits = byteOffsetBool(RETURN_NO_FINISH_BIT);
        const release_param_caps = !reader.readBool(release_bits.byte, release_bits.bit);
        const no_finish_needed = reader.readBool(no_finish_bits.byte, no_finish_bits.bit);

        const tag_value = reader.readUnionDiscriminant(RETURN_DISCRIMINANT_OFFSET_BYTES);
        const tag: ReturnTag = @enumFromInt(tag_value);

        var results: ?Payload = null;
        var exception: ?Exception = null;
        var take_from_other_question: ?u32 = null;
        var accept_from_third_party: ?message.AnyPointerReader = null;

        switch (tag) {
            .results => {
                const payload_reader = try reader.readStruct(RETURN_RESULTS_PTR);
                results = try Payload.fromReader(payload_reader);
            },
            .exception => {
                const ex_reader = try reader.readStruct(RETURN_RESULTS_PTR);
                exception = try Exception.fromReader(ex_reader);
            },
            .take_from_other_question => {
                take_from_other_question = reader.readU32(byteOffsetU32(RETURN_TAKE_FROM_OTHER_Q_OFFSET));
            },
            .accept_from_third_party => {
                accept_from_third_party = reader.readAnyPointer(RETURN_RESULTS_PTR) catch |err| switch (err) {
                    error.OutOfBounds => null,
                    else => return err,
                };
            },
            else => {},
        }

        return .{
            .answer_id = answer_id,
            .release_param_caps = release_param_caps,
            .no_finish_needed = no_finish_needed,
            .tag = tag,
            .results = results,
            .exception = exception,
            .take_from_other_question = take_from_other_question,
            .accept_from_third_party = accept_from_third_party,
        };
    }
};

pub const Finish = struct {
    question_id: u32,
    release_result_caps: bool,
    require_early_cancellation: bool,

    fn fromReader(reader: message.StructReader) !Finish {
        const question_id = reader.readU32(byteOffsetU32(FINISH_QUESTION_ID_OFFSET));
        const release_bits = byteOffsetBool(FINISH_RELEASE_RESULT_CAPS_BIT);
        const require_bits = byteOffsetBool(FINISH_REQUIRE_EARLY_CANCEL_BIT);
        return .{
            .question_id = question_id,
            .release_result_caps = !reader.readBool(release_bits.byte, release_bits.bit),
            .require_early_cancellation = !reader.readBool(require_bits.byte, require_bits.bit),
        };
    }
};

pub const MessageTarget = struct {
    tag: MessageTargetTag,
    imported_cap: ?u32,
    promised_answer: ?PromisedAnswer,

    fn fromReader(reader: message.StructReader) !MessageTarget {
        const tag_value = reader.readUnionDiscriminant(MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES);
        const tag: MessageTargetTag = @enumFromInt(tag_value);
        var imported_cap: ?u32 = null;
        var promised_answer: ?PromisedAnswer = null;
        switch (tag) {
            .imported_cap => {
                imported_cap = reader.readU32(byteOffsetU32(MESSAGE_TARGET_IMPORTED_CAP_OFFSET));
            },
            .promised_answer => {
                const pa_reader = try reader.readStruct(MESSAGE_TARGET_PROMISED_ANSWER_PTR);
                promised_answer = try PromisedAnswer.fromReader(pa_reader);
            },
        }
        return .{ .tag = tag, .imported_cap = imported_cap, .promised_answer = promised_answer };
    }
};

pub const PromisedAnswer = struct {
    question_id: u32,
    transform: PromisedAnswerTransform,

    fn fromReader(reader: message.StructReader) !PromisedAnswer {
        const question_id = reader.readU32(byteOffsetU32(PROMISED_ANSWER_QUESTION_ID_OFFSET));
        const transform = reader.readStructList(PROMISED_ANSWER_TRANSFORM_PTR) catch |err| switch (err) {
            error.InvalidPointer => null,
            else => return err,
        };
        return .{ .question_id = question_id, .transform = .{ .list = transform } };
    }
};

pub const PromisedAnswerTransform = struct {
    list: ?message.StructListReader,

    pub fn len(self: PromisedAnswerTransform) u32 {
        return if (self.list) |list| list.len() else 0;
    }

    pub fn get(self: PromisedAnswerTransform, index: u32) !PromisedAnswerOp {
        const list = self.list orelse return error.OutOfBounds;
        const reader = try list.get(index);
        return PromisedAnswerOp.fromReader(reader);
    }
};

pub const PromisedAnswerOp = struct {
    tag: PromisedAnswerOpTag,
    pointer_index: u16,

    fn fromReader(reader: message.StructReader) PromisedAnswerOp {
        const disc = reader.readUnionDiscriminant(PROMISED_ANSWER_OP_DISCRIMINANT_OFFSET_BYTES);
        const tag: PromisedAnswerOpTag = @enumFromInt(disc);
        const pointer_index = switch (tag) {
            .get_pointer_field => reader.readU16(PROMISED_ANSWER_OP_GET_POINTER_FIELD_OFFSET_BYTES),
            else => 0,
        };
        return .{ .tag = tag, .pointer_index = pointer_index };
    }
};

fn writePromisedAnswerFrom(builder: message.StructBuilder, promised: PromisedAnswer) !void {
    builder.writeU32(byteOffsetU32(PROMISED_ANSWER_QUESTION_ID_OFFSET), promised.question_id);
    const op_count = promised.transform.len();
    if (op_count == 0) return;
    var list = try builder.writeStructList(
        PROMISED_ANSWER_TRANSFORM_PTR,
        op_count,
        PROMISED_ANSWER_OP_DATA_WORDS,
        PROMISED_ANSWER_OP_POINTER_WORDS,
    );
    var idx: u32 = 0;
    while (idx < op_count) : (idx += 1) {
        const op = try promised.transform.get(idx);
        const elem = try list.get(idx);
        elem.writeUnionDiscriminant(PROMISED_ANSWER_OP_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(op.tag));
        if (op.tag == .get_pointer_field) {
            elem.writeU16(PROMISED_ANSWER_OP_GET_POINTER_FIELD_OFFSET_BYTES, op.pointer_index);
        }
    }
}

fn writePromisedAnswerOps(builder: message.StructBuilder, question_id: u32, ops: []const PromisedAnswerOp) !void {
    builder.writeU32(byteOffsetU32(PROMISED_ANSWER_QUESTION_ID_OFFSET), question_id);
    if (ops.len == 0) return;
    var list = try builder.writeStructList(
        PROMISED_ANSWER_TRANSFORM_PTR,
        @intCast(ops.len),
        PROMISED_ANSWER_OP_DATA_WORDS,
        PROMISED_ANSWER_OP_POINTER_WORDS,
    );
    for (ops, 0..) |op, idx| {
        const elem = try list.get(@intCast(idx));
        elem.writeUnionDiscriminant(PROMISED_ANSWER_OP_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(op.tag));
        if (op.tag == .get_pointer_field) {
            elem.writeU16(PROMISED_ANSWER_OP_GET_POINTER_FIELD_OFFSET_BYTES, op.pointer_index);
        }
    }
}

fn writeMessageTarget(builder: message.StructBuilder, target: MessageTarget) !void {
    builder.writeUnionDiscriminant(MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(target.tag));
    switch (target.tag) {
        .imported_cap => {
            const id = target.imported_cap orelse return error.MissingCallTarget;
            builder.writeU32(byteOffsetU32(MESSAGE_TARGET_IMPORTED_CAP_OFFSET), id);
        },
        .promised_answer => {
            const promised_answer = target.promised_answer orelse return error.MissingPromisedAnswer;
            const promised = try builder.initStruct(MESSAGE_TARGET_PROMISED_ANSWER_PTR, PROMISED_ANSWER_DATA_WORDS, PROMISED_ANSWER_POINTER_WORDS);
            try writePromisedAnswerFrom(promised, promised_answer);
        },
    }
}

fn writeCapDescriptor(builder: message.StructBuilder, descriptor: CapDescriptor) !void {
    switch (descriptor.tag) {
        .none => {
            builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.none));
        },
        .sender_hosted => CapDescriptor.writeSenderHosted(builder, descriptor.id orelse return error.MissingCapDescriptorId),
        .sender_promise => CapDescriptor.writeSenderPromise(builder, descriptor.id orelse return error.MissingCapDescriptorId),
        .receiver_hosted => CapDescriptor.writeReceiverHosted(builder, descriptor.id orelse return error.MissingCapDescriptorId),
        .receiver_answer => {
            const promised_answer = descriptor.promised_answer orelse return error.MissingPromisedAnswer;
            builder.writeUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(CapDescriptorTag.receiver_answer));
            const promised = try builder.initStruct(CAP_DESCRIPTOR_PTR, PROMISED_ANSWER_DATA_WORDS, PROMISED_ANSWER_POINTER_WORDS);
            try writePromisedAnswerFrom(promised, promised_answer);
        },
        .third_party_hosted => {
            const third = descriptor.third_party orelse return error.MissingThirdPartyCapDescriptor;
            if (third.id) |id| {
                try CapDescriptor.writeThirdPartyHosted(builder, id, third.vine_id);
            } else {
                try CapDescriptor.writeThirdPartyHostedNull(builder, third.vine_id);
            }
        },
    }

    if (descriptor.attached_fd) |fd| {
        builder.writeU8(byteOffsetU16(CAP_DESCRIPTOR_ATTACHED_FD_OFFSET), fd);
    }
}

pub const Payload = struct {
    content: message.AnyPointerReader,
    cap_table: ?message.StructListReader,

    fn fromReader(reader: message.StructReader) !Payload {
        const content = try reader.readAnyPointer(PAYLOAD_CONTENT_PTR);
        const cap_table = reader.readStructList(PAYLOAD_CAP_TABLE_PTR) catch |err| switch (err) {
            error.InvalidPointer => null,
            else => return err,
        };
        return .{ .content = content, .cap_table = cap_table };
    }
};

pub const Release = struct {
    id: u32,
    reference_count: u32,

    fn fromReader(reader: message.StructReader) !Release {
        const id = reader.readU32(byteOffsetU32(RELEASE_ID_OFFSET));
        const reference_count = reader.readU32(byteOffsetU32(RELEASE_REFERENCE_COUNT_OFFSET));
        return .{ .id = id, .reference_count = reference_count };
    }
};

pub const Resolve = struct {
    promise_id: u32,
    tag: ResolveTag,
    cap: ?CapDescriptor,
    exception: ?Exception,

    fn fromReader(reader: message.StructReader) !Resolve {
        const promise_id = reader.readU32(byteOffsetU32(RESOLVE_PROMISE_ID_OFFSET));
        const tag_value = reader.readUnionDiscriminant(RESOLVE_DISCRIMINANT_OFFSET_BYTES);
        const tag: ResolveTag = @enumFromInt(tag_value);
        var cap: ?CapDescriptor = null;
        var exception: ?Exception = null;

        switch (tag) {
            .cap => {
                const cap_reader = try reader.readStruct(RESOLVE_CAP_PTR);
                cap = try CapDescriptor.fromReader(cap_reader);
            },
            .exception => {
                const ex_reader = try reader.readStruct(RESOLVE_CAP_PTR);
                exception = try Exception.fromReader(ex_reader);
            },
        }

        return .{
            .promise_id = promise_id,
            .tag = tag,
            .cap = cap,
            .exception = exception,
        };
    }
};

pub const Disembargo = struct {
    target: MessageTarget,
    context_tag: DisembargoContextTag,
    embargo_id: ?u32,
    accept: ?[]const u8,

    fn fromReader(reader: message.StructReader) !Disembargo {
        const target_reader = try reader.readStruct(DISEMBARGO_TARGET_PTR);
        const target = try MessageTarget.fromReader(target_reader);

        const tag_value = reader.readUnionDiscriminant(DISEMBARGO_DISCRIMINANT_OFFSET_BYTES);
        const context_tag: DisembargoContextTag = @enumFromInt(tag_value);
        var embargo_id: ?u32 = null;
        var accept: ?[]const u8 = null;

        switch (context_tag) {
            .sender_loopback, .receiver_loopback => {
                embargo_id = reader.readU32(byteOffsetU32(DISEMBARGO_EMBARGO_ID_OFFSET));
            },
            .accept => {
                accept = reader.readData(DISEMBARGO_ACCEPT_PTR) catch |err| switch (err) {
                    error.InvalidPointer => null,
                    else => return err,
                };
            },
        }

        return .{
            .target = target,
            .context_tag = context_tag,
            .embargo_id = embargo_id,
            .accept = accept,
        };
    }
};

pub const Provide = struct {
    question_id: u32,
    target: MessageTarget,
    recipient: ?message.AnyPointerReader,

    fn fromReader(reader: message.StructReader) !Provide {
        const target_reader = try reader.readStruct(PROVIDE_TARGET_PTR);
        const recipient = reader.readAnyPointer(PROVIDE_RECIPIENT_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        return .{
            .question_id = reader.readU32(byteOffsetU32(PROVIDE_QUESTION_ID_OFFSET)),
            .target = try MessageTarget.fromReader(target_reader),
            .recipient = recipient,
        };
    }
};

pub const Accept = struct {
    question_id: u32,
    provision: ?message.AnyPointerReader,
    embargo: ?[]const u8,

    fn fromReader(reader: message.StructReader) !Accept {
        const provision = reader.readAnyPointer(ACCEPT_PROVISION_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        const embargo = reader.readData(ACCEPT_EMBARGO_PTR) catch |err| switch (err) {
            error.InvalidPointer => null,
            else => return err,
        };
        return .{
            .question_id = reader.readU32(byteOffsetU32(ACCEPT_QUESTION_ID_OFFSET)),
            .provision = provision,
            .embargo = embargo,
        };
    }
};

pub const ThirdPartyAnswer = struct {
    completion: ?message.AnyPointerReader,
    answer_id: u32,

    fn fromReader(reader: message.StructReader) !ThirdPartyAnswer {
        const completion = reader.readAnyPointer(THIRD_PARTY_ANSWER_COMPLETION_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        return .{
            .completion = completion,
            .answer_id = reader.readU32(byteOffsetU32(THIRD_PARTY_ANSWER_ANSWER_ID_OFFSET)),
        };
    }
};

pub const Join = struct {
    question_id: u32,
    target: MessageTarget,
    key_part: ?message.AnyPointerReader,

    fn fromReader(reader: message.StructReader) !Join {
        const target_reader = try reader.readStruct(JOIN_TARGET_PTR);
        const key_part = reader.readAnyPointer(JOIN_KEY_PART_PTR) catch |err| switch (err) {
            error.OutOfBounds => null,
            else => return err,
        };
        return .{
            .question_id = reader.readU32(byteOffsetU32(JOIN_QUESTION_ID_OFFSET)),
            .target = try MessageTarget.fromReader(target_reader),
            .key_part = key_part,
        };
    }
};

pub const Exception = struct {
    reason: []const u8,
    trace: []const u8,
    type_value: u16,

    fn fromReader(reader: message.StructReader) !Exception {
        const reason = try reader.readText(EXCEPTION_REASON_PTR);
        const trace = reader.readText(EXCEPTION_TRACE_PTR) catch |err| switch (err) {
            error.InvalidPointer => "",
            else => return err,
        };
        const type_value = reader.readU16(byteOffsetU16(EXCEPTION_TYPE_OFFSET));
        return .{ .reason = reason, .trace = trace, .type_value = type_value };
    }
};

pub const MessageBuilder = struct {
    builder: message.MessageBuilder,

    pub fn init(allocator: std.mem.Allocator) MessageBuilder {
        return .{ .builder = message.MessageBuilder.init(allocator) };
    }

    pub fn deinit(self: *MessageBuilder) void {
        self.builder.deinit();
    }

    pub fn finish(self: *MessageBuilder) ![]const u8 {
        return self.builder.toBytes();
    }

    pub fn buildBootstrap(self: *MessageBuilder, question_id: u32) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.bootstrap));

        var bootstrap = try root.initStruct(0, BOOTSTRAP_DATA_WORDS, BOOTSTRAP_POINTER_WORDS);
        bootstrap.writeU32(byteOffsetU32(BOOTSTRAP_QUESTION_ID_OFFSET), question_id);
    }

    pub fn buildUnimplementedFromAnyPointer(self: *MessageBuilder, original: message.AnyPointerReader) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.unimplemented));
        const payload = try root.getAnyPointer(0);
        try message.cloneAnyPointer(original, payload);
    }

    pub fn buildAbort(self: *MessageBuilder, reason: []const u8) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.abort));

        var ex = try root.initStruct(0, EXCEPTION_DATA_WORDS, EXCEPTION_POINTER_WORDS);
        try ex.writeText(EXCEPTION_REASON_PTR, reason);
    }

    pub fn buildRelease(self: *MessageBuilder, id: u32, reference_count: u32) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.release));

        var release = try root.initStruct(0, RELEASE_DATA_WORDS, RELEASE_POINTER_WORDS);
        release.writeU32(byteOffsetU32(RELEASE_ID_OFFSET), id);
        release.writeU32(byteOffsetU32(RELEASE_REFERENCE_COUNT_OFFSET), reference_count);
    }

    pub fn buildFinish(
        self: *MessageBuilder,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.finish));

        var finish_struct = try root.initStruct(0, FINISH_DATA_WORDS, FINISH_POINTER_WORDS);
        finish_struct.writeU32(byteOffsetU32(FINISH_QUESTION_ID_OFFSET), question_id);
        const release_bits = byteOffsetBool(FINISH_RELEASE_RESULT_CAPS_BIT);
        finish_struct.writeBool(release_bits.byte, release_bits.bit, !release_result_caps);
        const require_bits = byteOffsetBool(FINISH_REQUIRE_EARLY_CANCEL_BIT);
        finish_struct.writeBool(require_bits.byte, require_bits.bit, !require_early_cancellation);
    }

    pub fn buildResolveCap(self: *MessageBuilder, promise_id: u32, descriptor: CapDescriptor) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.resolve));

        var resolve = try root.initStruct(0, RESOLVE_DATA_WORDS, RESOLVE_POINTER_WORDS);
        resolve.writeU32(byteOffsetU32(RESOLVE_PROMISE_ID_OFFSET), promise_id);
        resolve.writeUnionDiscriminant(RESOLVE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(ResolveTag.cap));

        const cap_builder = try resolve.initStruct(RESOLVE_CAP_PTR, CAP_DESCRIPTOR_DATA_WORDS, CAP_DESCRIPTOR_POINTER_WORDS);
        try writeCapDescriptor(cap_builder, descriptor);
    }

    pub fn buildResolveException(self: *MessageBuilder, promise_id: u32, reason: []const u8) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.resolve));

        var resolve = try root.initStruct(0, RESOLVE_DATA_WORDS, RESOLVE_POINTER_WORDS);
        resolve.writeU32(byteOffsetU32(RESOLVE_PROMISE_ID_OFFSET), promise_id);
        resolve.writeUnionDiscriminant(RESOLVE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(ResolveTag.exception));

        var ex = try resolve.initStruct(RESOLVE_CAP_PTR, EXCEPTION_DATA_WORDS, EXCEPTION_POINTER_WORDS);
        try ex.writeText(EXCEPTION_REASON_PTR, reason);
    }

    pub fn buildDisembargoSenderLoopback(self: *MessageBuilder, target: MessageTarget, embargo_id: u32) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.disembargo));

        var disembargo = try root.initStruct(0, DISEMBARGO_DATA_WORDS, DISEMBARGO_POINTER_WORDS);
        const target_builder = try disembargo.initStruct(DISEMBARGO_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        try writeMessageTarget(target_builder, target);
        disembargo.writeUnionDiscriminant(DISEMBARGO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(DisembargoContextTag.sender_loopback));
        disembargo.writeU32(byteOffsetU32(DISEMBARGO_EMBARGO_ID_OFFSET), embargo_id);
    }

    pub fn buildDisembargoReceiverLoopback(self: *MessageBuilder, target: MessageTarget, embargo_id: u32) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.disembargo));

        var disembargo = try root.initStruct(0, DISEMBARGO_DATA_WORDS, DISEMBARGO_POINTER_WORDS);
        const target_builder = try disembargo.initStruct(DISEMBARGO_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        try writeMessageTarget(target_builder, target);
        disembargo.writeUnionDiscriminant(DISEMBARGO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(DisembargoContextTag.receiver_loopback));
        disembargo.writeU32(byteOffsetU32(DISEMBARGO_EMBARGO_ID_OFFSET), embargo_id);
    }

    pub fn buildDisembargoAccept(
        self: *MessageBuilder,
        target: MessageTarget,
        accept_embargo: []const u8,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.disembargo));

        var disembargo = try root.initStruct(0, DISEMBARGO_DATA_WORDS, DISEMBARGO_POINTER_WORDS);
        const target_builder = try disembargo.initStruct(DISEMBARGO_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        try writeMessageTarget(target_builder, target);
        disembargo.writeUnionDiscriminant(DISEMBARGO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(DisembargoContextTag.accept));
        try disembargo.writeData(DISEMBARGO_ACCEPT_PTR, accept_embargo);
    }

    pub fn buildProvide(
        self: *MessageBuilder,
        question_id: u32,
        target: MessageTarget,
        recipient: ?message.AnyPointerReader,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.provide));

        var provide = try root.initStruct(0, PROVIDE_DATA_WORDS, PROVIDE_POINTER_WORDS);
        provide.writeU32(byteOffsetU32(PROVIDE_QUESTION_ID_OFFSET), question_id);
        const target_builder = try provide.initStruct(PROVIDE_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        try writeMessageTarget(target_builder, target);

        const recipient_any = try provide.getAnyPointer(PROVIDE_RECIPIENT_PTR);
        if (recipient) |recipient_ptr| {
            try message.cloneAnyPointer(recipient_ptr, recipient_any);
        } else {
            try recipient_any.setNull();
        }
    }

    pub fn buildAccept(
        self: *MessageBuilder,
        question_id: u32,
        provision: ?message.AnyPointerReader,
        embargo: ?[]const u8,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.accept));

        var accept = try root.initStruct(0, ACCEPT_DATA_WORDS, ACCEPT_POINTER_WORDS);
        accept.writeU32(byteOffsetU32(ACCEPT_QUESTION_ID_OFFSET), question_id);

        const provision_any = try accept.getAnyPointer(ACCEPT_PROVISION_PTR);
        if (provision) |provision_ptr| {
            try message.cloneAnyPointer(provision_ptr, provision_any);
        } else {
            try provision_any.setNull();
        }

        if (embargo) |embargo_bytes| {
            try accept.writeData(ACCEPT_EMBARGO_PTR, embargo_bytes);
        }
    }

    pub fn buildThirdPartyAnswer(
        self: *MessageBuilder,
        answer_id: u32,
        completion: ?message.AnyPointerReader,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.third_party_answer));

        var third_party_answer = try root.initStruct(
            0,
            THIRD_PARTY_ANSWER_DATA_WORDS,
            THIRD_PARTY_ANSWER_POINTER_WORDS,
        );
        third_party_answer.writeU32(byteOffsetU32(THIRD_PARTY_ANSWER_ANSWER_ID_OFFSET), answer_id);

        const completion_any = try third_party_answer.getAnyPointer(THIRD_PARTY_ANSWER_COMPLETION_PTR);
        if (completion) |completion_ptr| {
            try message.cloneAnyPointer(completion_ptr, completion_any);
        } else {
            try completion_any.setNull();
        }
    }

    pub fn buildJoin(
        self: *MessageBuilder,
        question_id: u32,
        target: MessageTarget,
        key_part: ?message.AnyPointerReader,
    ) !void {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.join));

        var join = try root.initStruct(0, JOIN_DATA_WORDS, JOIN_POINTER_WORDS);
        join.writeU32(byteOffsetU32(JOIN_QUESTION_ID_OFFSET), question_id);
        const target_builder = try join.initStruct(JOIN_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        try writeMessageTarget(target_builder, target);

        const key_part_any = try join.getAnyPointer(JOIN_KEY_PART_PTR);
        if (key_part) |key_part_ptr| {
            try message.cloneAnyPointer(key_part_ptr, key_part_any);
        } else {
            try key_part_any.setNull();
        }
    }

    pub fn beginCall(self: *MessageBuilder, question_id: u32, interface_id: u64, method_id: u16) !CallBuilder {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.call));

        var call = try root.initStruct(0, CALL_DATA_WORDS, CALL_POINTER_WORDS);
        call.writeU32(byteOffsetU32(CALL_QUESTION_ID_OFFSET), question_id);
        call.writeU64(byteOffsetU64(CALL_INTERFACE_ID_OFFSET), interface_id);
        call.writeU16(byteOffsetU16(CALL_METHOD_ID_OFFSET), method_id);

        return CallBuilder{ .call = call };
    }

    pub fn beginReturn(self: *MessageBuilder, answer_id: u32, tag: ReturnTag) !ReturnBuilder {
        const root = try self.builder.allocateStruct(MESSAGE_DATA_WORDS, MESSAGE_POINTER_WORDS);
        root.writeUnionDiscriminant(MESSAGE_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTag.return_));

        var ret = try root.initStruct(0, RETURN_DATA_WORDS, RETURN_POINTER_WORDS);
        ret.writeU32(byteOffsetU32(RETURN_ANSWER_ID_OFFSET), answer_id);
        ret.writeUnionDiscriminant(RETURN_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(tag));

        return ReturnBuilder{ .ret = ret, .tag = tag };
    }
};

pub const CallBuilder = struct {
    call: message.StructBuilder,
    payload: ?message.StructBuilder = null,

    pub fn setTargetImportedCap(self: *CallBuilder, cap_id: u32) !void {
        var target = try self.call.initStruct(CALL_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        target.writeUnionDiscriminant(MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTargetTag.imported_cap));
        target.writeU32(byteOffsetU32(MESSAGE_TARGET_IMPORTED_CAP_OFFSET), cap_id);
    }

    pub fn setTargetPromisedAnswer(self: *CallBuilder, question_id: u32) !void {
        try self.setTargetPromisedAnswerWithOps(question_id, &[_]PromisedAnswerOp{});
    }

    pub fn setTargetPromisedAnswerWithOps(self: *CallBuilder, question_id: u32, ops: []const PromisedAnswerOp) !void {
        var target = try self.call.initStruct(CALL_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        target.writeUnionDiscriminant(MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTargetTag.promised_answer));
        const promised = try target.initStruct(MESSAGE_TARGET_PROMISED_ANSWER_PTR, PROMISED_ANSWER_DATA_WORDS, PROMISED_ANSWER_POINTER_WORDS);
        try writePromisedAnswerOps(promised, question_id, ops);
    }

    pub fn setTargetPromisedAnswerFrom(self: *CallBuilder, promised_answer: PromisedAnswer) !void {
        var target = try self.call.initStruct(CALL_TARGET_PTR, MESSAGE_TARGET_DATA_WORDS, MESSAGE_TARGET_POINTER_WORDS);
        target.writeUnionDiscriminant(MESSAGE_TARGET_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(MessageTargetTag.promised_answer));
        const promised = try target.initStruct(MESSAGE_TARGET_PROMISED_ANSWER_PTR, PROMISED_ANSWER_DATA_WORDS, PROMISED_ANSWER_POINTER_WORDS);
        try writePromisedAnswerFrom(promised, promised_answer);
    }

    pub fn setSendResultsToCaller(self: *CallBuilder) void {
        self.call.writeUnionDiscriminant(CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(SendResultsToTag.caller));
    }

    pub fn setSendResultsToYourself(self: *CallBuilder) void {
        self.call.writeUnionDiscriminant(CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(SendResultsToTag.yourself));
    }

    pub fn setSendResultsToThirdPartyNull(self: *CallBuilder) !void {
        self.call.writeUnionDiscriminant(CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(SendResultsToTag.third_party));
        var ptr = try self.call.getAnyPointer(CALL_SEND_RESULTS_TO_THIRD_PARTY_PTR);
        try ptr.setNull();
    }

    pub fn setSendResultsToThirdParty(self: *CallBuilder, third_party: message.AnyPointerReader) !void {
        self.call.writeUnionDiscriminant(CALL_SEND_RESULTS_TO_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(SendResultsToTag.third_party));
        const ptr = try self.call.getAnyPointer(CALL_SEND_RESULTS_TO_THIRD_PARTY_PTR);
        try message.cloneAnyPointer(third_party, ptr);
    }

    pub fn initParamsStruct(self: *CallBuilder, data_words: u16, pointer_words: u16) !message.StructBuilder {
        var payload = try self.ensurePayload();
        const any = try payload.getAnyPointer(PAYLOAD_CONTENT_PTR);
        return any.initStruct(data_words, pointer_words);
    }

    pub fn payloadBuilder(self: *CallBuilder) !message.StructBuilder {
        return self.ensurePayload();
    }

    pub fn getParamsAnyPointer(self: *CallBuilder) !message.AnyPointerBuilder {
        var payload = try self.ensurePayload();
        return payload.getAnyPointer(PAYLOAD_CONTENT_PTR);
    }

    pub fn initCapTable(self: *CallBuilder, count: u32) !message.StructListBuilder {
        var payload = try self.ensurePayload();
        return payload.writeStructList(PAYLOAD_CAP_TABLE_PTR, count, CAP_DESCRIPTOR_DATA_WORDS, CAP_DESCRIPTOR_POINTER_WORDS);
    }

    pub fn setEmptyCapTable(self: *CallBuilder) !void {
        var payload = try self.ensurePayload();
        _ = try payload.writeStructList(PAYLOAD_CAP_TABLE_PTR, 0, CAP_DESCRIPTOR_DATA_WORDS, CAP_DESCRIPTOR_POINTER_WORDS);
    }

    fn ensurePayload(self: *CallBuilder) !message.StructBuilder {
        if (self.payload) |payload| return payload;
        const payload = try self.call.initStruct(CALL_PARAMS_PTR, PAYLOAD_DATA_WORDS, PAYLOAD_POINTER_WORDS);
        self.payload = payload;
        return payload;
    }
};

pub const ReturnBuilder = struct {
    ret: message.StructBuilder,
    tag: ReturnTag,
    payload: ?message.StructBuilder = null,

    pub fn setReleaseParamCaps(self: *ReturnBuilder, release_param_caps: bool) void {
        const release_bits = byteOffsetBool(RETURN_RELEASE_PARAM_CAPS_BIT);
        self.ret.writeBool(release_bits.byte, release_bits.bit, !release_param_caps);
    }

    pub fn setNoFinishNeeded(self: *ReturnBuilder, no_finish_needed: bool) void {
        const no_finish_bits = byteOffsetBool(RETURN_NO_FINISH_BIT);
        self.ret.writeBool(no_finish_bits.byte, no_finish_bits.bit, no_finish_needed);
    }

    pub fn initResultsStruct(self: *ReturnBuilder, data_words: u16, pointer_words: u16) !message.StructBuilder {
        if (self.tag != .results) return error.InvalidReturnTag;
        var payload = try self.ensurePayload();
        const any = try payload.getAnyPointer(PAYLOAD_CONTENT_PTR);
        return any.initStruct(data_words, pointer_words);
    }

    pub fn payloadBuilder(self: *ReturnBuilder) !message.StructBuilder {
        if (self.tag != .results) return error.InvalidReturnTag;
        return self.ensurePayload();
    }

    pub fn getResultsAnyPointer(self: *ReturnBuilder) !message.AnyPointerBuilder {
        if (self.tag != .results) return error.InvalidReturnTag;
        var payload = try self.ensurePayload();
        return payload.getAnyPointer(PAYLOAD_CONTENT_PTR);
    }

    pub fn initCapTable(self: *ReturnBuilder, count: u32) !message.StructListBuilder {
        if (self.tag != .results) return error.InvalidReturnTag;
        var payload = try self.ensurePayload();
        return payload.writeStructList(PAYLOAD_CAP_TABLE_PTR, count, CAP_DESCRIPTOR_DATA_WORDS, CAP_DESCRIPTOR_POINTER_WORDS);
    }

    pub fn setEmptyCapTable(self: *ReturnBuilder) !void {
        if (self.tag != .results) return error.InvalidReturnTag;
        var payload = try self.ensurePayload();
        _ = try payload.writeStructList(PAYLOAD_CAP_TABLE_PTR, 0, CAP_DESCRIPTOR_DATA_WORDS, CAP_DESCRIPTOR_POINTER_WORDS);
    }

    pub fn setException(self: *ReturnBuilder, reason: []const u8) !void {
        if (self.tag != .exception) return error.InvalidReturnTag;
        var ex = try self.ret.initStruct(RETURN_RESULTS_PTR, EXCEPTION_DATA_WORDS, EXCEPTION_POINTER_WORDS);
        try ex.writeText(EXCEPTION_REASON_PTR, reason);
    }

    pub fn setCanceled(self: *ReturnBuilder) void {
        _ = self;
    }

    pub fn setTakeFromOtherQuestion(self: *ReturnBuilder, question_id: u32) !void {
        if (self.tag != .take_from_other_question) return error.InvalidReturnTag;
        self.ret.writeU32(byteOffsetU32(RETURN_TAKE_FROM_OTHER_Q_OFFSET), question_id);
    }

    pub fn setAcceptFromThirdPartyNull(self: *ReturnBuilder) !void {
        if (self.tag != .accept_from_third_party) return error.InvalidReturnTag;
        var any = try self.ret.getAnyPointer(RETURN_RESULTS_PTR);
        try any.setNull();
    }

    pub fn setAcceptFromThirdParty(self: *ReturnBuilder, third_party: message.AnyPointerReader) !void {
        if (self.tag != .accept_from_third_party) return error.InvalidReturnTag;
        const any = try self.ret.getAnyPointer(RETURN_RESULTS_PTR);
        try message.cloneAnyPointer(third_party, any);
    }

    fn ensurePayload(self: *ReturnBuilder) !message.StructBuilder {
        if (self.payload) |payload| return payload;
        const payload = try self.ret.initStruct(RETURN_RESULTS_PTR, PAYLOAD_DATA_WORDS, PAYLOAD_POINTER_WORDS);
        self.payload = payload;
        return payload;
    }
};
