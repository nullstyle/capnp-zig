const std = @import("std");
const protocol = @import("../level0/protocol.zig");

pub fn cloneOpsFromSlice(
    allocator: std.mem.Allocator,
    ops: []const protocol.PromisedAnswerOp,
) ![]protocol.PromisedAnswerOp {
    const copied = try allocator.alloc(protocol.PromisedAnswerOp, ops.len);
    errdefer allocator.free(copied);
    std.mem.copyForwards(protocol.PromisedAnswerOp, copied, ops);
    return copied;
}

/// Maximum number of transform operations allowed in a PromisedAnswer.
/// Matches a reasonable cap on pointer traversal depth (Cap'n Proto spec
/// allows arbitrary nesting, but real schemas rarely exceed a handful).
const max_transform_ops: u32 = 64;

pub fn cloneOpsFromPromised(
    allocator: std.mem.Allocator,
    promised: protocol.PromisedAnswer,
) ![]protocol.PromisedAnswerOp {
    const op_count = promised.transform.len();
    if (op_count > max_transform_ops) return error.TransformTooLong;
    const ops = try allocator.alloc(protocol.PromisedAnswerOp, op_count);
    errdefer allocator.free(ops);

    var idx: u32 = 0;
    while (idx < op_count) : (idx += 1) {
        ops[idx] = try promised.transform.get(idx);
    }

    return ops;
}
