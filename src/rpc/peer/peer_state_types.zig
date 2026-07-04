const std = @import("std");
const cap_table = @import("../caps/table.zig");

pub const ProvideTarget = union(enum) {
    /// A directly-resolved local capability: its cap id plus the 4-bit origin
    /// code recording whether it is one of our exports or a remote import, so
    /// the outbound Return encodes the correct descriptor variant instead of
    /// re-deriving the id space from the (possibly-colliding) bare id.
    local: LocalProvideTarget,
    promised: cap_table.OwnedPromisedAnswer,

    pub fn deinit(self: *ProvideTarget, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .promised => |promised| promised.deinit(allocator),
            else => {},
        }
    }
};

pub const LocalProvideTarget = struct {
    origin_code: u4,
    cap_id: u32,
};

pub const ProvideEntry = struct {
    recipient_key: []u8,
    target: ProvideTarget,
};

pub const JoinKeyPart = struct {
    join_id: u32,
    part_count: u16,
    part_num: u16,
};

pub const JoinPartEntry = struct {
    question_id: u32,
    target: ProvideTarget,
};

pub const JoinState = struct {
    part_count: u16,
    parts: std.AutoHashMap(u16, JoinPartEntry),

    pub fn init(allocator: std.mem.Allocator, part_count: u16) JoinState {
        return .{
            .part_count = part_count,
            .parts = std.AutoHashMap(u16, JoinPartEntry).init(allocator),
        };
    }

    pub fn deinit(self: *JoinState, allocator: std.mem.Allocator) void {
        var it = self.parts.valueIterator();
        while (it.next()) |part| {
            var target = part.target;
            target.deinit(allocator);
        }
        self.parts.deinit();
    }
};

pub const PendingJoinQuestion = struct {
    join_id: u32,
    part_num: u16,
};

pub const PendingEmbargoedAccept = struct {
    answer_id: u32,
    provided_question_id: u32,
};
