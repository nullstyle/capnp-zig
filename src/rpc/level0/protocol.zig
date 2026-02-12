/// Cap'n Proto RPC protocol message types and their wire-format readers/builders.
///
/// Each struct in this module corresponds to a message or sub-structure defined
/// in `rpc.capnp`. Reader structs decode fields from `StructReader`; builder
/// structs write fields into a `MessageBuilder`.
const std = @import("std");
const message = @import("../../serialization/message.zig");
const rpc_capnp = @import("../gen/capnp/rpc.zig");

/// Discriminant tag for the top-level RPC `Message` union.
pub const MessageTag = rpc_capnp.Message.WhichTag;

/// Discriminant tag for the `Return` union variants.
pub const ReturnTag = rpc_capnp.Return.WhichTag;

pub const MessageTargetTag = rpc_capnp.MessageTarget.WhichTag;

pub const PromisedAnswerOpTag = rpc_capnp.Op.WhichTag;

/// Discriminant tag identifying the kind of capability being transferred.
pub const CapDescriptorTag = rpc_capnp.CapDescriptor.WhichTag;

pub const SendResultsToTag = rpc_capnp.Call.SendResultsTo.WhichTag;

pub const ResolveTag = rpc_capnp.Resolve.WhichTag;

pub const DisembargoContextTag = rpc_capnp.Disembargo.Context.WhichTag;
pub const PayloadBuilder = rpc_capnp.Payload.Builder;

const CapDescriptorListBuilder = message.typed_list_helpers.StructListBuilder(rpc_capnp.CapDescriptor);

/// Describes a capability being passed in a Call or Return payload's cap table.
///
/// Each entry identifies where the capability lives: sender-hosted, sender-promise,
/// receiver-hosted, receiver-answer (promise pipeline), or third-party-hosted.
pub const CapDescriptor = struct {
    tag: CapDescriptorTag,
    id: ?u32 = null,
    promised_answer: ?PromisedAnswer = null,
    third_party: ?ThirdPartyCapDescriptor = null,
    attached_fd: ?u8 = null,

    pub fn fromReader(reader: message.StructReader) !CapDescriptor {
        const tag_value = reader.readUnionDiscriminant(CAP_DESCRIPTOR_DISCRIMINANT_OFFSET_BYTES);
        const tag = std.meta.intToEnum(CapDescriptorTag, tag_value) catch return error.InvalidDiscriminant;
        var id: ?u32 = null;
        var promised_answer: ?PromisedAnswer = null;
        var third_party: ?ThirdPartyCapDescriptor = null;
        var attached_fd: ?u8 = null;
        switch (tag) {
            .senderHosted, .senderPromise, .receiverHosted => {
                id = reader.readU32(byteOffsetU32(CAP_DESCRIPTOR_ID_OFFSET));
            },
            .receiverAnswer => {
                const pa_reader = try reader.readStruct(CAP_DESCRIPTOR_PTR);
                promised_answer = try PromisedAnswer.fromReader(pa_reader);
            },
            .thirdPartyHosted => {
                const third_reader = try reader.readStruct(CAP_DESCRIPTOR_PTR);
                third_party = try ThirdPartyCapDescriptor.fromReader(third_reader);
            },
            .none => {},
        }
        const fd_value = reader.readU8(CAP_DESCRIPTOR_ATTACHED_FD_OFFSET_BYTES);
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

    fn asGeneratedBuilder(builder: anytype) rpc_capnp.CapDescriptor.Builder {
        return switch (@TypeOf(builder)) {
            message.StructBuilder => rpc_capnp.CapDescriptor.Builder.wrap(builder),
            rpc_capnp.CapDescriptor.Builder => builder,
            else => @compileError("expected message.StructBuilder or rpc_capnp.CapDescriptor.Builder"),
        };
    }

    pub fn writeSenderHosted(builder: anytype, id: u32) void {
        var generated = asGeneratedBuilder(builder);
        // Generated setter is !void but only calls writeU16/writeU32 on already-allocated data section.
        generated.setSenderHosted(id) catch unreachable;
    }

    pub fn writeSenderPromise(builder: anytype, id: u32) void {
        var generated = asGeneratedBuilder(builder);
        // Generated setter is !void but only calls writeU16/writeU32 on already-allocated data section.
        generated.setSenderPromise(id) catch unreachable;
    }

    pub fn writeReceiverHosted(builder: anytype, id: u32) void {
        var generated = asGeneratedBuilder(builder);
        // Generated setter is !void but only calls writeU16/writeU32 on already-allocated data section.
        generated.setReceiverHosted(id) catch unreachable;
    }

    pub fn writeReceiverAnswer(builder: anytype, question_id: u32, ops: []const PromisedAnswerOp) !void {
        var generated = asGeneratedBuilder(builder);
        var promised_builder = try generated.initReceiverAnswer();
        try writePromisedAnswerOpsGenerated(&promised_builder, question_id, ops);
    }

    pub fn writeThirdPartyHostedNull(builder: anytype, vine_id: u32) !void {
        var generated = asGeneratedBuilder(builder);
        var third_builder = try generated.initThirdPartyHosted();
        try third_builder.setVineId(vine_id);
        try third_builder.setIdNull();
    }

    pub fn writeThirdPartyHosted(
        builder: anytype,
        third_party_id: message.AnyPointerReader,
        vine_id: u32,
    ) !void {
        var generated = asGeneratedBuilder(builder);
        var third_builder = try generated.initThirdPartyHosted();
        try third_builder.setVineId(vine_id);
        const id_any = try third_builder.initId();
        try message.cloneAnyPointer(third_party_id, id_any);
    }
};

/// Identifies a capability hosted by a third party, with a vine ID for
/// reference counting.
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
const CAP_DESCRIPTOR_ATTACHED_FD_OFFSET_BYTES: usize = 2;

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

/// A parsed RPC message: the underlying `Message` plus the decoded top-level tag.
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

    pub fn asBootstrap(self: *const DecodedMessage) !Bootstrap {
        if (self.tag != .bootstrap) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Bootstrap.fromReader(reader);
    }

    pub fn asUnimplemented(self: *const DecodedMessage) !Unimplemented {
        if (self.tag != .unimplemented) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Unimplemented.fromReader(reader);
    }

    pub fn asAbort(self: *const DecodedMessage) !Abort {
        if (self.tag != .abort) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Abort.fromReader(reader);
    }

    pub fn asCall(self: *const DecodedMessage) !Call {
        if (self.tag != .call) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Call.fromReader(reader);
    }

    pub fn asReturn(self: *const DecodedMessage) !Return {
        if (self.tag != .@"return") return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Return.fromReader(reader);
    }

    pub fn asFinish(self: *const DecodedMessage) !Finish {
        if (self.tag != .finish) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Finish.fromReader(reader);
    }

    pub fn asRelease(self: *const DecodedMessage) !Release {
        if (self.tag != .release) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Release.fromReader(reader);
    }

    pub fn asResolve(self: *const DecodedMessage) !Resolve {
        if (self.tag != .resolve) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Resolve.fromReader(reader);
    }

    pub fn asDisembargo(self: *const DecodedMessage) !Disembargo {
        if (self.tag != .disembargo) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Disembargo.fromReader(reader);
    }

    pub fn asProvide(self: *const DecodedMessage) !Provide {
        if (self.tag != .provide) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Provide.fromReader(reader);
    }

    pub fn asAccept(self: *const DecodedMessage) !Accept {
        if (self.tag != .accept) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Accept.fromReader(reader);
    }

    pub fn asThirdPartyAnswer(self: *const DecodedMessage) !ThirdPartyAnswer {
        if (self.tag != .thirdPartyAnswer) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return ThirdPartyAnswer.fromReader(reader);
    }

    pub fn asJoin(self: *const DecodedMessage) !Join {
        if (self.tag != .join) return error.UnexpectedMessage;
        const root = try self.msg.getRootStruct();
        const reader = try root.readStruct(0);
        return Join.fromReader(reader);
    }
};

/// Request to obtain the remote peer's bootstrap capability.
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

/// Echoed back when the receiver does not implement the received message type.
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

/// Fatal error that terminates the RPC connection.
pub const Abort = struct {
    exception: Exception,

    fn fromReader(reader: message.StructReader) !Abort {
        return .{
            .exception = try Exception.fromReader(reader),
        };
    }
};

/// An RPC method call: identifies the target capability, interface, and method,
/// and carries the parameters payload with its capability table.
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

/// Where the callee should send results: back to caller, to yourself
/// (tail call), or to a third party.
pub const SendResultsTo = struct {
    tag: SendResultsToTag,
    third_party: ?message.AnyPointerReader = null,

    fn fromReader(reader: message.StructReader) !SendResultsTo {
        const generated = rpc_capnp.Call.SendResultsTo.Reader.wrap(reader);
        const tag = generated.which() catch return error.InvalidDiscriminant;

        var third_party: ?message.AnyPointerReader = null;
        if (tag == .thirdParty) {
            third_party = generated.getThirdParty() catch |err| switch (err) {
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

/// The response to a previously sent `Call`, carrying results, an exception,
/// or a redirect to another question/third-party.
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
        const generated = rpc_capnp.Return.Reader.wrap(reader);
        const answer_id = try generated.getAnswerId();
        const release_param_caps = try generated.getReleaseParamCaps();
        const no_finish_needed = try generated.getNoFinishNeeded();
        const tag = generated.which() catch return error.InvalidDiscriminant;

        var results: ?Payload = null;
        var exception: ?Exception = null;
        var take_from_other_question: ?u32 = null;
        var accept_from_third_party: ?message.AnyPointerReader = null;

        switch (tag) {
            .results => {
                const payload_reader = try generated.getResults();
                results = try Payload.fromReader(payload_reader._reader);
            },
            .exception => {
                const ex_reader = try generated.getException();
                exception = try Exception.fromReader(ex_reader._reader);
            },
            .takeFromOtherQuestion => {
                take_from_other_question = try generated.getTakeFromOtherQuestion();
            },
            .awaitFromThirdParty => {
                accept_from_third_party = generated.getAwaitFromThirdParty() catch |err| switch (err) {
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

/// Tells the callee that the caller is done with the question and its results.
pub const Finish = struct {
    question_id: u32,
    release_result_caps: bool,
    // TODO: implement requireEarlyCancellationWorkaround behavior — when true and
    // the Finish arrives before the call is delivered, defer cancellation until
    // after delivery so the callee can opt out (see rpc.capnp Finish.requireEarlyCancellationWorkaround).
    // Currently this field is parsed and forwarded but the deferred-cancellation
    // logic is not yet implemented.
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

/// The target of a Call: either an imported capability ID or a promised answer pipeline.
pub const MessageTarget = struct {
    tag: MessageTargetTag,
    imported_cap: ?u32,
    promised_answer: ?PromisedAnswer,

    fn fromReader(reader: message.StructReader) !MessageTarget {
        const generated = rpc_capnp.MessageTarget.Reader.wrap(reader);
        const tag = generated.which() catch return error.InvalidDiscriminant;
        var imported_cap: ?u32 = null;
        var promised_answer: ?PromisedAnswer = null;
        switch (tag) {
            .importedCap => {
                imported_cap = try generated.getImportedCap();
            },
            .promisedAnswer => {
                const pa_reader = try generated.getPromisedAnswer();
                promised_answer = try PromisedAnswer.fromReader(pa_reader._reader);
            },
        }
        return .{ .tag = tag, .imported_cap = imported_cap, .promised_answer = promised_answer };
    }
};

/// A reference to a not-yet-returned answer, with an optional transform path
/// for promise pipelining.
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
        return try PromisedAnswerOp.fromReader(reader);
    }
};

pub const PromisedAnswerOp = struct {
    tag: PromisedAnswerOpTag,
    pointer_index: u16,

    fn fromReader(reader: message.StructReader) !PromisedAnswerOp {
        const disc = reader.readUnionDiscriminant(PROMISED_ANSWER_OP_DISCRIMINANT_OFFSET_BYTES);
        const tag = std.meta.intToEnum(PromisedAnswerOpTag, disc) catch return error.InvalidDiscriminant;
        const pointer_index = switch (tag) {
            .getPointerField => reader.readU16(PROMISED_ANSWER_OP_GET_POINTER_FIELD_OFFSET_BYTES),
            else => 0,
        };
        return .{ .tag = tag, .pointer_index = pointer_index };
    }
};

fn writePromisedAnswerGenerated(builder: *rpc_capnp.PromisedAnswer.Builder, promised: PromisedAnswer) !void {
    try builder.setQuestionId(promised.question_id);
    const op_count = promised.transform.len();
    if (op_count == 0) return;

    var transform = try builder.initTransform(op_count);
    var idx: u32 = 0;
    while (idx < op_count) : (idx += 1) {
        const op = try promised.transform.get(idx);
        var op_builder = try transform.get(idx);
        switch (op.tag) {
            .noop => try op_builder.setNoop({}),
            .getPointerField => try op_builder.setGetPointerField(op.pointer_index),
        }
    }
}

fn writePromisedAnswerOpsGenerated(builder: *rpc_capnp.PromisedAnswer.Builder, question_id: u32, ops: []const PromisedAnswerOp) !void {
    try builder.setQuestionId(question_id);
    if (ops.len == 0) return;

    var transform = try builder.initTransform(@intCast(ops.len));
    for (ops, 0..) |op, idx| {
        var op_builder = try transform.get(@intCast(idx));
        switch (op.tag) {
            .noop => try op_builder.setNoop({}),
            .getPointerField => try op_builder.setGetPointerField(op.pointer_index),
        }
    }
}

fn writeMessageTargetGenerated(builder: *rpc_capnp.MessageTarget.Builder, target: MessageTarget) !void {
    switch (target.tag) {
        .importedCap => try builder.setImportedCap(target.imported_cap orelse return error.MissingCallTarget),
        .promisedAnswer => {
            const promised_answer = target.promised_answer orelse return error.MissingPromisedAnswer;
            var promised_builder = try builder.initPromisedAnswer();
            try writePromisedAnswerGenerated(&promised_builder, promised_answer);
        },
    }
}

fn writeCapDescriptorGenerated(builder: *rpc_capnp.CapDescriptor.Builder, descriptor: CapDescriptor) !void {
    switch (descriptor.tag) {
        .none => try builder.setNone({}),
        .senderHosted => try builder.setSenderHosted(descriptor.id orelse return error.MissingCapDescriptorId),
        .senderPromise => try builder.setSenderPromise(descriptor.id orelse return error.MissingCapDescriptorId),
        .receiverHosted => try builder.setReceiverHosted(descriptor.id orelse return error.MissingCapDescriptorId),
        .receiverAnswer => {
            const promised_answer = descriptor.promised_answer orelse return error.MissingPromisedAnswer;
            var promised_builder = try builder.initReceiverAnswer();
            try writePromisedAnswerGenerated(&promised_builder, promised_answer);
        },
        .thirdPartyHosted => {
            const third = descriptor.third_party orelse return error.MissingThirdPartyCapDescriptor;
            var third_builder = try builder.initThirdPartyHosted();
            try third_builder.setVineId(third.vine_id);
            if (third.id) |id| {
                const id_builder = try third_builder.initId();
                try message.cloneAnyPointer(id, id_builder);
            } else {
                try third_builder.setIdNull();
            }
        },
    }

    if (descriptor.attached_fd) |fd| {
        // Preserve legacy protocol semantics for attached_fd while resolve encode
        // is only partially migrated to generated builders.
        builder._builder.writeU8(CAP_DESCRIPTOR_ATTACHED_FD_OFFSET_BYTES, fd);
    }
}

/// A content pointer paired with a capability table, used in Call params
/// and Return results.
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

/// Tells the sender to release references to an exported capability.
pub const Release = struct {
    id: u32,
    reference_count: u32,

    fn fromReader(reader: message.StructReader) !Release {
        const id = reader.readU32(byteOffsetU32(RELEASE_ID_OFFSET));
        const reference_count = reader.readU32(byteOffsetU32(RELEASE_REFERENCE_COUNT_OFFSET));
        return .{ .id = id, .reference_count = reference_count };
    }
};

/// Resolves a previously sent promise capability to either a concrete
/// capability descriptor or an exception.
pub const Resolve = struct {
    promise_id: u32,
    tag: ResolveTag,
    cap: ?CapDescriptor,
    exception: ?Exception,

    fn fromReader(reader: message.StructReader) !Resolve {
        const generated = rpc_capnp.Resolve.Reader.wrap(reader);
        const promise_id = try generated.getPromiseId();
        const tag = generated.which() catch return error.InvalidDiscriminant;
        var cap: ?CapDescriptor = null;
        var exception: ?Exception = null;

        switch (tag) {
            .cap => {
                const cap_reader = try generated.getCap();
                cap = try CapDescriptor.fromReader(cap_reader._reader);
            },
            .exception => {
                const ex_reader = try generated.getException();
                exception = try Exception.fromReader(ex_reader._reader);
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

/// Used to lift an embargo after a Resolve, ensuring message ordering is
/// preserved during capability replacement.
pub const Disembargo = struct {
    target: MessageTarget,
    context_tag: DisembargoContextTag,
    embargo_id: ?u32,
    accept: ?[]const u8,

    fn fromReader(reader: message.StructReader) !Disembargo {
        const target_reader = try reader.readStruct(DISEMBARGO_TARGET_PTR);
        const target = try MessageTarget.fromReader(target_reader);

        const tag_value = reader.readUnionDiscriminant(DISEMBARGO_DISCRIMINANT_OFFSET_BYTES);
        const context_tag = std.meta.intToEnum(DisembargoContextTag, tag_value) catch return error.InvalidDiscriminant;
        var embargo_id: ?u32 = null;
        var accept: ?[]const u8 = null;

        switch (context_tag) {
            .senderLoopback, .receiverLoopback => {
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

/// Three-party handoff: offer a capability to a third party via a designated recipient.
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

/// Three-party handoff: accept a capability previously offered via `Provide`.
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

/// Three-party handoff: the introducer tells one peer the answer ID assigned
/// by the third party.
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

/// Three-party handoff: verify that two capabilities resolve to the same object.
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

/// An RPC exception with a human-readable reason, optional stack trace, and type code.
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

/// Builder for constructing Cap'n Proto RPC messages (Bootstrap, Call, Return, etc.).
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
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var bootstrap_builder = try root_builder.initBootstrap();
        try bootstrap_builder.setQuestionId(question_id);
    }

    pub fn buildUnimplementedFromAnyPointer(self: *MessageBuilder, original: message.AnyPointerReader) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        _ = try root_builder.initUnimplemented();
        const payload = try root_builder._builder.getAnyPointer(0);
        try message.cloneAnyPointer(original, payload);
    }

    pub fn buildAbort(self: *MessageBuilder, reason: []const u8) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var ex_builder = try root_builder.initAbort();
        try ex_builder.setReason(reason);
    }

    pub fn buildRelease(self: *MessageBuilder, id: u32, reference_count: u32) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var release_builder = try root_builder.initRelease();
        try release_builder.setId(id);
        try release_builder.setReferenceCount(reference_count);
    }

    pub fn buildFinish(
        self: *MessageBuilder,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var finish_builder = try root_builder.initFinish();
        try finish_builder.setQuestionId(question_id);
        try finish_builder.setReleaseResultCaps(release_result_caps);
        try finish_builder.setRequireEarlyCancellationWorkaround(require_early_cancellation);
    }

    pub fn buildResolveCap(self: *MessageBuilder, promise_id: u32, descriptor: CapDescriptor) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var resolve_builder = try root_builder.initResolve();
        try resolve_builder.setPromiseId(promise_id);
        var cap_builder = try resolve_builder.initCap();
        try writeCapDescriptorGenerated(&cap_builder, descriptor);
    }

    pub fn buildResolveException(self: *MessageBuilder, promise_id: u32, reason: []const u8) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var resolve_builder = try root_builder.initResolve();
        try resolve_builder.setPromiseId(promise_id);
        var ex_builder = try resolve_builder.initException();
        try ex_builder.setReason(reason);
    }

    pub fn buildDisembargoSenderLoopback(self: *MessageBuilder, target: MessageTarget, embargo_id: u32) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var disembargo_builder = try root_builder.initDisembargo();
        var target_builder = try disembargo_builder.initTarget();
        try writeMessageTargetGenerated(&target_builder, target);
        var context = disembargo_builder.getContext();
        try context.setSenderLoopback(embargo_id);
    }

    pub fn buildDisembargoReceiverLoopback(self: *MessageBuilder, target: MessageTarget, embargo_id: u32) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var disembargo_builder = try root_builder.initDisembargo();
        var target_builder = try disembargo_builder.initTarget();
        try writeMessageTargetGenerated(&target_builder, target);
        var context = disembargo_builder.getContext();
        try context.setReceiverLoopback(embargo_id);
    }

    pub fn buildDisembargoAccept(
        self: *MessageBuilder,
        target: MessageTarget,
        accept_embargo: []const u8,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var disembargo_builder = try root_builder.initDisembargo();
        var target_builder = try disembargo_builder.initTarget();
        try writeMessageTargetGenerated(&target_builder, target);
        var context = disembargo_builder.getContext();
        try context.setAccept(accept_embargo);
    }

    pub fn buildProvide(
        self: *MessageBuilder,
        question_id: u32,
        target: MessageTarget,
        recipient: ?message.AnyPointerReader,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var provide_builder = try root_builder.initProvide();
        try provide_builder.setQuestionId(question_id);
        var target_builder = try provide_builder.initTarget();
        try writeMessageTargetGenerated(&target_builder, target);

        if (recipient) |recipient_ptr| {
            const recipient_any = try provide_builder.initRecipient();
            try message.cloneAnyPointer(recipient_ptr, recipient_any);
        } else {
            try provide_builder.setRecipientNull();
        }
    }

    pub fn buildAccept(
        self: *MessageBuilder,
        question_id: u32,
        provision: ?message.AnyPointerReader,
        embargo: ?[]const u8,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var accept_builder = try root_builder.initAccept();
        try accept_builder.setQuestionId(question_id);

        if (provision) |provision_ptr| {
            const provision_any = try accept_builder.initProvision();
            try message.cloneAnyPointer(provision_ptr, provision_any);
        } else {
            try accept_builder.setProvisionNull();
        }

        if (embargo) |embargo_bytes| {
            try accept_builder.setEmbargo(embargo_bytes);
        }
    }

    pub fn buildThirdPartyAnswer(
        self: *MessageBuilder,
        answer_id: u32,
        completion: ?message.AnyPointerReader,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var third_party_answer_builder = try root_builder.initThirdPartyAnswer();
        try third_party_answer_builder.setAnswerId(answer_id);

        if (completion) |completion_ptr| {
            const completion_any = try third_party_answer_builder.initCompletion();
            try message.cloneAnyPointer(completion_ptr, completion_any);
        } else {
            try third_party_answer_builder.setCompletionNull();
        }
    }

    pub fn buildJoin(
        self: *MessageBuilder,
        question_id: u32,
        target: MessageTarget,
        key_part: ?message.AnyPointerReader,
    ) !void {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var join_builder = try root_builder.initJoin();
        try join_builder.setQuestionId(question_id);
        var target_builder = try join_builder.initTarget();
        try writeMessageTargetGenerated(&target_builder, target);

        if (key_part) |key_part_ptr| {
            const key_part_any = try join_builder.initKeyPart();
            try message.cloneAnyPointer(key_part_ptr, key_part_any);
        } else {
            try join_builder.setKeyPartNull();
        }
    }

    pub fn beginCall(self: *MessageBuilder, question_id: u32, interface_id: u64, method_id: u16) !CallBuilder {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var call_builder = try root_builder.initCall();
        try call_builder.setQuestionId(question_id);
        try call_builder.setInterfaceId(interface_id);
        try call_builder.setMethodId(method_id);

        return CallBuilder{ .call = call_builder._builder };
    }

    pub fn beginReturn(self: *MessageBuilder, answer_id: u32, tag: ReturnTag) !ReturnBuilder {
        var root_builder = try rpc_capnp.Message.Builder.init(&self.builder);
        var ret_builder = try root_builder.initReturn();
        try ret_builder.setAnswerId(answer_id);
        ret_builder._builder.writeU16(RETURN_DISCRIMINANT_OFFSET_BYTES, @intFromEnum(tag));

        return ReturnBuilder{ .ret = ret_builder._builder, .tag = tag };
    }
};

/// Builder for populating a Call message's target, parameters, and send-results-to fields.
pub const CallBuilder = struct {
    call: message.StructBuilder,
    payload: ?rpc_capnp.Payload.Builder = null,

    pub fn setTargetImportedCap(self: *CallBuilder, cap_id: u32) !void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var target_builder = try call_builder.initTarget();
        try target_builder.setImportedCap(cap_id);
    }

    pub fn setTargetPromisedAnswer(self: *CallBuilder, question_id: u32) !void {
        try self.setTargetPromisedAnswerWithOps(question_id, &[_]PromisedAnswerOp{});
    }

    pub fn setTargetPromisedAnswerWithOps(self: *CallBuilder, question_id: u32, ops: []const PromisedAnswerOp) !void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var target_builder = try call_builder.initTarget();
        var promised_builder = try target_builder.initPromisedAnswer();
        try writePromisedAnswerOpsGenerated(&promised_builder, question_id, ops);
    }

    pub fn setTargetPromisedAnswerFrom(self: *CallBuilder, promised_answer: PromisedAnswer) !void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var target_builder = try call_builder.initTarget();
        var promised_builder = try target_builder.initPromisedAnswer();
        try writePromisedAnswerGenerated(&promised_builder, promised_answer);
    }

    pub fn setSendResultsToCaller(self: *CallBuilder) void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var send_results_to = call_builder.getSendResultsTo();
        // Generated setter is !void but only calls writeU16 on already-allocated data section.
        send_results_to.setCaller({}) catch unreachable;
    }

    pub fn setSendResultsToYourself(self: *CallBuilder) void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var send_results_to = call_builder.getSendResultsTo();
        // Generated setter is !void but only calls writeU16 on already-allocated data section.
        send_results_to.setYourself({}) catch unreachable;
    }

    pub fn setSendResultsToThirdPartyNull(self: *CallBuilder) !void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var send_results_to = call_builder.getSendResultsTo();
        var ptr = try send_results_to.initThirdParty();
        try ptr.setNull();
    }

    pub fn setSendResultsToThirdParty(self: *CallBuilder, third_party: message.AnyPointerReader) !void {
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        var send_results_to = call_builder.getSendResultsTo();
        const ptr = try send_results_to.initThirdParty();
        try message.cloneAnyPointer(third_party, ptr);
    }

    pub fn payloadTyped(self: *CallBuilder) !rpc_capnp.Payload.Builder {
        return self.ensurePayload();
    }

    pub fn initCapTableTyped(self: *CallBuilder, count: u32) !CapDescriptorListBuilder {
        var payload = try self.ensurePayload();
        return payload.initCapTable(count);
    }

    fn ensurePayload(self: *CallBuilder) !rpc_capnp.Payload.Builder {
        if (self.payload) |payload| return payload;
        var call_builder = rpc_capnp.Call.Builder.wrap(self.call);
        const payload = try call_builder.initParams();
        self.payload = payload;
        return payload;
    }
};

/// Builder for populating a Return message's results, exception, or redirect.
pub const ReturnBuilder = struct {
    ret: message.StructBuilder,
    tag: ReturnTag,
    payload: ?rpc_capnp.Payload.Builder = null,

    pub fn setReleaseParamCaps(self: *ReturnBuilder, release_param_caps: bool) void {
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        // Generated setter is !void but only calls writeBool on already-allocated data section.
        ret_builder.setReleaseParamCaps(release_param_caps) catch unreachable;
    }

    pub fn setNoFinishNeeded(self: *ReturnBuilder, no_finish_needed: bool) void {
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        // Generated setter is !void but only calls writeBool on already-allocated data section.
        ret_builder.setNoFinishNeeded(no_finish_needed) catch unreachable;
    }

    pub fn setException(self: *ReturnBuilder, reason: []const u8) !void {
        if (self.tag != .exception) return error.InvalidReturnTag;
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        var ex_builder = try ret_builder.initException();
        try ex_builder.setReason(reason);
    }

    pub fn setCanceled(self: *ReturnBuilder) void {
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        // Generated setter is !void but only calls writeU16 on already-allocated data section.
        ret_builder.setCanceled({}) catch unreachable;
    }

    pub fn setTakeFromOtherQuestion(self: *ReturnBuilder, question_id: u32) !void {
        if (self.tag != .takeFromOtherQuestion) return error.InvalidReturnTag;
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        try ret_builder.setTakeFromOtherQuestion(question_id);
    }

    pub fn setAcceptFromThirdPartyNull(self: *ReturnBuilder) !void {
        if (self.tag != .awaitFromThirdParty) return error.InvalidReturnTag;
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        try ret_builder.setAwaitFromThirdPartyNull();
    }

    pub fn setAcceptFromThirdParty(self: *ReturnBuilder, third_party: message.AnyPointerReader) !void {
        if (self.tag != .awaitFromThirdParty) return error.InvalidReturnTag;
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        const any = try ret_builder.initAwaitFromThirdParty();
        try message.cloneAnyPointer(third_party, any);
    }

    pub fn payloadTyped(self: *ReturnBuilder) !rpc_capnp.Payload.Builder {
        if (self.tag != .results) return error.InvalidReturnTag;
        return self.ensurePayload();
    }

    pub fn initCapTableTyped(self: *ReturnBuilder, count: u32) !CapDescriptorListBuilder {
        if (self.tag != .results) return error.InvalidReturnTag;
        var payload = try self.ensurePayload();
        return payload.initCapTable(count);
    }

    fn ensurePayload(self: *ReturnBuilder) !rpc_capnp.Payload.Builder {
        if (self.payload) |payload| return payload;
        var ret_builder = rpc_capnp.Return.Builder.wrap(self.ret);
        const payload = try ret_builder.initResults();
        self.payload = payload;
        return payload;
    }
};
