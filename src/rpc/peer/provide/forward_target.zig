const std = @import("std");
const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");

/// Heap-independent description of the capability target an originated
/// Provide's vine must call when Level-3 pickup falls back to the introducer.
///
/// Imported targets are a plain id in the provide peer's import table. A
/// promised-answer target owns its transform ops because the MessageTarget
/// passed to `sendProvide` is backed by the caller's decoded frame and cannot
/// outlive that call.
pub const ForwardTarget = union(enum) {
    imported: u32,
    promised: cap_table.OwnedPromisedAnswer,

    pub fn fromMessageTarget(
        allocator: std.mem.Allocator,
        target: protocol.MessageTarget,
    ) !ForwardTarget {
        return switch (target.tag) {
            .importedCap => .{
                .imported = target.imported_cap orelse return error.MissingImportedCap,
            },
            .promisedAnswer => .{
                .promised = try cap_table.OwnedPromisedAnswer.fromPromised(
                    allocator,
                    target.promised_answer orelse return error.MissingPromisedAnswer,
                ),
            },
        };
    }

    pub fn fromQuestionAndOps(
        allocator: std.mem.Allocator,
        question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
    ) !ForwardTarget {
        if (ops.len > protocol.max_promised_answer_transform_ops) return error.TransformTooLong;
        return .{
            .promised = try cap_table.OwnedPromisedAnswer.fromQuestionAndOps(
                allocator,
                question_id,
                ops,
            ),
        };
    }

    pub fn deinit(self: *ForwardTarget, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .promised => |promised| promised.deinit(allocator),
            .imported => {},
        }
        self.* = undefined;
    }
};

test "ForwardTarget owns promised-answer operations" {
    const ops = [_]protocol.PromisedAnswerOp{
        .{ .tag = .getPointerField, .pointer_index = 4 },
    };
    var target = try ForwardTarget.fromQuestionAndOps(std.testing.allocator, 17, &ops);
    defer target.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 17), target.promised.question_id);
    try std.testing.expectEqualSlices(protocol.PromisedAnswerOp, &ops, target.promised.ops);
}

test "ForwardTarget keeps imported targets allocation-free" {
    var target = try ForwardTarget.fromMessageTarget(std.testing.allocator, .{
        .tag = .importedCap,
        .imported_cap = 23,
        .promised_answer = null,
    });
    defer target.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u32, 23), target.imported);
}

test "ForwardTarget rejects promised-answer transforms beyond the wire bound" {
    var ops: [protocol.max_promised_answer_transform_ops + 1]protocol.PromisedAnswerOp = undefined;
    for (&ops) |*op| op.* = .{ .tag = .getPointerField, .pointer_index = 0 };

    try std.testing.expectError(
        error.TransformTooLong,
        ForwardTarget.fromQuestionAndOps(std.testing.allocator, 31, &ops),
    );
}
