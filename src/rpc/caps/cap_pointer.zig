const std = @import("std");
const message = @import("../../serialization/message.zig");

// Compatibility shim for older RPC imports. The implementation is owned by
// serialization because these helpers encode Cap'n Proto pointer bits.
pub const max_traversal_depth = message.capability_remap.max_traversal_depth;
pub const makeCapabilityPointer = message.capability_remap.makeCapabilityPointer;
pub const decodeCapabilityPointer = message.capability_remap.decodeCapabilityPointer;
pub const buildMessageView = message.capability_remap.buildMessageView;
pub const writePointerWord = message.capability_remap.writePointerWord;

test "capability pointer roundtrip" {
    const cap_id: u32 = 12345;
    const word = makeCapabilityPointer(cap_id);
    try std.testing.expectEqual(@as(u64, 3 | (@as(u64, cap_id) << 32)), word);
    try std.testing.expectEqual(cap_id, try decodeCapabilityPointer(word));
}

test "decode capability pointer rejects invalid tags" {
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointer(0));
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointer(1));
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointer(2));
}

test "decode capability pointer rejects high bits" {
    const invalid_word = (@as(u64, 1) << 2) | 3;
    try std.testing.expectError(error.InvalidPointer, decodeCapabilityPointer(invalid_word));
}

test "capability pointer supports full u32 range" {
    const word = makeCapabilityPointer(std.math.maxInt(u32));
    try std.testing.expectEqual(std.math.maxInt(u32), try decodeCapabilityPointer(word));
}
