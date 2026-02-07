const std = @import("std");
const protocol = @import("protocol.zig");

pub fn cloneOpsFromPromised(
    allocator: std.mem.Allocator,
    promised: protocol.PromisedAnswer,
) ![]protocol.PromisedAnswerOp {
    const op_count = promised.transform.len();
    const ops = try allocator.alloc(protocol.PromisedAnswerOp, op_count);
    errdefer allocator.free(ops);

    var idx: u32 = 0;
    while (idx < op_count) : (idx += 1) {
        ops[idx] = try promised.transform.get(idx);
    }

    return ops;
}
