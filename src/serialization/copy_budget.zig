//! Allocation-free checks of expanded clone work. Shared targets are charged
//! once for every incoming pointer because copying duplicates those targets.
const std = @import("std");
const message = @import("message.zig");

pub const Options = struct {
    /// Pointer visits, scalar words, and logical list elements, including Void.
    max_work: usize = 8 * 1024 * 1024,
    /// Reachable cloned words, excluding the destination's existing root slot.
    max_output_words: usize = 8 * 1024 * 1024,
    /// Additional live backing allocation, including temporary copies and
    /// allocator capacity, beyond the builder storage present at entry.
    max_allocation_bytes: usize = 64 * 1024 * 1024,
    nesting_limit: usize = 64,
};

pub const Budget = struct {
    work: usize,
    words: usize,

    pub fn init(options: Options) Budget {
        return .{ .work = options.max_work, .words = options.max_output_words };
    }

    fn chargeWork(self: *Budget, amount: usize) !void {
        if (amount > self.work) return error.CopyWorkLimitExceeded;
        self.work -= amount;
    }

    fn chargeWords(self: *Budget, amount: usize) !void {
        if (amount > self.words) return error.CopyOutputLimitExceeded;
        self.words -= amount;
    }

    pub fn pointer(self: *Budget, source: message.AnyPointerReader, depth: usize) anyerror!void {
        if (depth == 0) return error.RecursionLimitExceeded;
        try self.chargeWork(1);
        const resolved = try source.message.resolvePointer(source.segment_id, source.pointer_pos, source.pointer_word, 8);
        if (resolved.pointer_word == 0 and resolved.content_override == null) return;
        switch (@as(u2, @truncate(resolved.pointer_word))) {
            0 => try self.record(try source.getStruct(), depth - 1),
            1 => {
                const kind: u3 = @truncate(resolved.pointer_word >> 32);
                if (kind == 7) {
                    const list = try (try message.AnyListReader.wrap(source)).getStructList();
                    try self.chargeWork(list.len());
                    try self.chargeWords(1); // inline-composite tag
                    if (list.data_words == 0 and list.pointer_words == 0) return;
                    for (0..list.len()) |index| try self.record(try list.get(@intCast(index)), depth - 1);
                } else {
                    const list = try source.message.resolveListPointer(source.segment_id, source.pointer_pos, source.pointer_word);
                    try self.chargeWork(list.element_count);
                    const bits: usize = switch (kind) {
                        0 => 0,
                        1 => 1,
                        2 => 8,
                        3 => 16,
                        4 => 32,
                        5, 6 => 64,
                        7 => return error.InvalidPointer,
                    };
                    const bit_count = std.math.mul(usize, list.element_count, bits) catch return error.CopyOutputLimitExceeded;
                    const words = bit_count / 64 + @intFromBool(bit_count % 64 != 0);
                    try self.chargeWords(words);
                    if (kind == 6) {
                        const pointers = try source.getPointerList();
                        for (0..pointers.len()) |index| {
                            const pos = pointers.elements_offset + index * 8;
                            const segment = pointers.message.segments[pointers.segment_id];
                            if (pos > segment.len or segment.len - pos < 8) return error.OutOfBounds;
                            const word = std.mem.readInt(u64, segment[pos..][0..8], .little);
                            if (word == 0) try self.chargeWork(1) else try self.pointer(.{ .message = pointers.message, .segment_id = pointers.segment_id, .pointer_pos = pos, .pointer_word = word }, depth - 1);
                        }
                    }
                }
            },
            3 => {
                _ = try source.getCapability();
            },
            else => return error.InvalidPointer,
        }
    }

    pub fn record(self: *Budget, source: message.StructReader, depth: usize) anyerror!void {
        const data_words = @max(source.data_size, @as(u16, if (source.sub_word_data_bytes != 0) 1 else 0));
        try self.chargeWords(@as(usize, data_words) + source.pointer_count);
        try self.chargeWork(data_words);
        for (0..source.pointer_count) |index| {
            const child = try source.readAnyPointer(index);
            if (child.isNull()) try self.chargeWork(1) else try self.pointer(child, depth);
        }
    }

    pub fn rootRecord(self: *Budget, source: message.StructReader, depth: usize) !void {
        if (depth == 0) return error.RecursionLimitExceeded;
        try self.chargeWork(1);
        try self.record(source, depth - 1);
    }
};

const AllocationBudget = @import("allocation_budget.zig");

// All storage in MessageBuilder is released through its current allocator.
// Temporarily wrapping that allocator bounds both scratch copies and growth;
// restore the original allocator before returning any persistent storage.
pub const Allocation = struct {
    builder: *message.MessageBuilder,
    budget: AllocationBudget,

    pub fn begin(self: *Allocation, builder: *message.MessageBuilder, additional: usize) !void {
        var existing = try std.math.mul(usize, builder.segments.capacity, @sizeOf(std.ArrayList(u8)));
        for (builder.segments.items) |segment| existing = try std.math.add(usize, existing, segment.capacity);
        self.* = .{ .builder = builder, .budget = .{
            .parent = builder.allocator,
            .limit = std.math.add(usize, existing, additional) catch std.math.maxInt(usize),
            .used = existing,
        } };
        builder.allocator = self.budget.allocator();
    }

    pub fn end(self: *Allocation) void {
        self.builder.allocator = self.budget.parent;
    }

    pub fn failure(self: *const Allocation, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.budget.denied) error.CopyAllocationLimitExceeded else err;
    }
};
