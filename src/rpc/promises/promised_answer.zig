const std = @import("std");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");

pub fn cloneOpsFromSlice(
    allocator: std.mem.Allocator,
    ops: []const protocol.PromisedAnswerOp,
) ![]protocol.PromisedAnswerOp {
    const copied = try allocator.alloc(protocol.PromisedAnswerOp, ops.len);
    errdefer allocator.free(copied);
    std.mem.copyForwards(protocol.PromisedAnswerOp, copied, ops);
    return copied;
}

pub fn cloneOpsFromPromised(
    allocator: std.mem.Allocator,
    promised: protocol.PromisedAnswer,
) ![]protocol.PromisedAnswerOp {
    const op_count = promised.transform.len();
    if (op_count > protocol.max_promised_answer_transform_ops) return error.TransformTooLong;
    const ops = try allocator.alloc(protocol.PromisedAnswerOp, op_count);
    errdefer allocator.free(ops);

    var idx: u32 = 0;
    while (idx < op_count) : (idx += 1) {
        ops[idx] = try promised.transform.get(idx);
    }

    return ops;
}

/// A heap-owned copy of a `PromisedAnswer` (question ID + transform ops).
pub const OwnedPromisedAnswer = struct {
    question_id: u32,
    ops: []protocol.PromisedAnswerOp,

    pub fn deinit(self: OwnedPromisedAnswer, allocator: std.mem.Allocator) void {
        allocator.free(self.ops);
    }

    pub fn fromQuestionAndOps(
        allocator: std.mem.Allocator,
        question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
    ) !OwnedPromisedAnswer {
        return .{
            .question_id = question_id,
            .ops = try cloneOpsFromSlice(allocator, ops),
        };
    }

    pub fn fromPromised(allocator: std.mem.Allocator, promised: protocol.PromisedAnswer) !OwnedPromisedAnswer {
        const ops = try cloneOpsFromPromised(allocator, promised);
        return .{
            .question_id = promised.question_id,
            .ops = ops,
        };
    }
};

/// Capability resolution result for promised-answer transform traversal.
pub const ResolvedPromisedCap = union(enum) {
    none,
    exported_id: u32,
    imported_id: u32,
    promised: protocol.PromisedAnswer,
};

fn resolveOutboundCapIndex(cap_list: message.StructListReader, index: u32) !ResolvedPromisedCap {
    if (index >= cap_list.len()) return error.CapabilityIndexOutOfBounds;
    const reader = try cap_list.get(index);
    const descriptor = try protocol.CapDescriptor.fromReader(reader);
    return switch (descriptor.tag) {
        .none => .none,
        .senderHosted, .senderPromise => .{ .exported_id = descriptor.id orelse return error.MissingCapDescriptorId },
        .receiverHosted => .{ .imported_id = descriptor.id orelse return error.MissingCapDescriptorId },
        .receiverAnswer => .{ .promised = descriptor.promised_answer orelse return error.MissingPromisedAnswer },
        .thirdPartyHosted => {
            const third = descriptor.third_party orelse return error.MissingThirdPartyCapDescriptor;
            return .{ .imported_id = third.vine_id };
        },
    };
}

/// Walk a promised-answer transform path through a results payload to find
/// the referenced capability.
pub fn resolvePromisedAnswer(
    payload: protocol.Payload,
    transform: protocol.PromisedAnswerTransform,
) !ResolvedPromisedCap {
    if (transform.len() > protocol.max_promised_answer_transform_ops) return error.TransformTooLong;

    var current = payload.content;
    var idx: u32 = 0;
    while (idx < transform.len()) : (idx += 1) {
        if (current.isNull()) return .none;
        const op = try transform.get(idx);
        switch (op.tag) {
            .noop => {},
            .getPointerField => {
                const struct_reader = try current.getStruct();
                current = try struct_reader.readAnyPointer(op.pointer_index);
            },
        }
    }

    if (current.isNull()) return .none;
    const cap = current.getCapability() catch |err| switch (err) {
        error.InvalidPointer => return .none,
        else => return err,
    };
    const cap_list = payload.cap_table orelse return error.MissingCapTable;
    return resolveOutboundCapIndex(cap_list, cap.id);
}
