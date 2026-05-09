const std = @import("std");
const log = std.log.scoped(.rpc_cap_table);
const protocol = @import("../wire/protocol.zig");
const descriptors = @import("descriptors.zig");

const OwnedPromisedAnswer = descriptors.OwnedPromisedAnswer;

/// Tracks capability import/export state for an RPC connection.
///
/// Manages import reference counts, export ID allocation, promise-export
/// markers, and receiver-answer entries used for promise pipelining. Each
/// `Peer` owns one `CapTable`.
pub const max_table_size: u32 = 10_000;

pub const CapTable = struct {
    allocator: std.mem.Allocator,
    exports: std.AutoHashMap(u32, void),
    imports: std.AutoHashMap(u32, ImportEntry),
    promised_exports: std.AutoHashMap(u32, void),
    receiver_answers: std.AutoHashMap(u32, OwnedPromisedAnswer),
    next_export_id: u32 = 0,

    pub fn init(allocator: std.mem.Allocator) CapTable {
        return .{
            .allocator = allocator,
            .exports = std.AutoHashMap(u32, void).init(allocator),
            .imports = std.AutoHashMap(u32, ImportEntry).init(allocator),
            .promised_exports = std.AutoHashMap(u32, void).init(allocator),
            .receiver_answers = std.AutoHashMap(u32, OwnedPromisedAnswer).init(allocator),
        };
    }

    pub fn deinit(self: *CapTable) void {
        self.exports.deinit();
        self.imports.deinit();
        self.promised_exports.deinit();
        var answer_it = self.receiver_answers.valueIterator();
        while (answer_it.next()) |answer| {
            answer.deinit(self.allocator);
        }
        self.receiver_answers.deinit();
    }

    pub fn totalEntries(self: *const CapTable) u32 {
        return @as(u32, @intCast(self.exports.count())) +
            @as(u32, @intCast(self.imports.count())) +
            @as(u32, @intCast(self.receiver_answers.count()));
    }

    /// Allocate a unique export ID that does not collide with any existing
    /// import, export, or receiver-answer entry.
    pub fn allocExportId(self: *CapTable) error{CapTableFull}!u32 {
        return self.allocLocalCapId();
    }

    pub fn noteExport(self: *CapTable, export_id: u32) !void {
        try self.exports.put(export_id, {});
    }

    pub fn clearExport(self: *CapTable, export_id: u32) void {
        _ = self.exports.remove(export_id);
        _ = self.promised_exports.remove(export_id);
    }

    pub fn hasExport(self: *const CapTable, export_id: u32) bool {
        return self.exports.contains(export_id);
    }

    pub fn markExportPromise(self: *CapTable, export_id: u32) !void {
        // Keep promise IDs in the same local export identity set used for
        // outbound descriptor classification.
        try self.exports.put(export_id, {});
        errdefer _ = self.exports.remove(export_id);
        try self.promised_exports.put(export_id, {});
    }

    pub fn clearExportPromise(self: *CapTable, export_id: u32) void {
        _ = self.promised_exports.remove(export_id);
    }

    pub fn isExportPromise(self: *const CapTable, export_id: u32) bool {
        return self.promised_exports.contains(export_id);
    }

    pub fn noteReceiverAnswer(self: *CapTable, promised: protocol.PromisedAnswer) !u32 {
        const id = try self.allocLocalCapId();
        var owned = try OwnedPromisedAnswer.fromPromised(self.allocator, promised);
        errdefer owned.deinit(self.allocator);
        try self.receiver_answers.put(id, owned);
        return id;
    }

    pub fn noteReceiverAnswerOps(
        self: *CapTable,
        question_id: u32,
        ops: []const protocol.PromisedAnswerOp,
    ) !u32 {
        const id = try self.allocLocalCapId();
        var owned = try OwnedPromisedAnswer.fromQuestionAndOps(self.allocator, question_id, ops);
        errdefer owned.deinit(self.allocator);
        try self.receiver_answers.put(id, owned);
        return id;
    }

    pub fn hasReceiverAnswer(self: *const CapTable, cap_id: u32) bool {
        return self.receiver_answers.contains(cap_id);
    }

    pub fn getReceiverAnswer(self: *const CapTable, cap_id: u32) ?OwnedPromisedAnswer {
        return self.receiver_answers.get(cap_id);
    }

    pub fn takeReceiverAnswer(self: *CapTable, cap_id: u32) ?OwnedPromisedAnswer {
        if (self.receiver_answers.fetchRemove(cap_id)) |removed| {
            return removed.value;
        }
        return null;
    }

    /// Record that a capability with `remote_id` was received from the remote
    /// peer. Increments the reference count if already known.
    pub fn noteImport(self: *CapTable, remote_id: u32) !void {
        if (self.imports.getEntry(remote_id)) |entry| {
            entry.value_ptr.ref_count = std.math.add(u32, entry.value_ptr.ref_count, 1) catch return error.RefCountOverflow;
            return;
        }

        try self.ensureCanAddEntry();
        try self.imports.put(remote_id, .{ .ref_count = 1 });
    }

    pub fn hasImport(self: *const CapTable, remote_id: u32) bool {
        return self.imports.contains(remote_id);
    }

    /// Decrement the reference count for an imported capability.
    /// Returns true if the import was fully released (count reached zero).
    pub fn releaseImport(self: *CapTable, remote_id: u32) bool {
        var entry = self.imports.getEntry(remote_id) orelse return false;
        if (entry.value_ptr.ref_count > 1) {
            entry.value_ptr.ref_count -= 1;
            return false;
        }
        _ = self.imports.remove(remote_id);
        return true;
    }

    fn allocLocalCapId(self: *CapTable) error{CapTableFull}!u32 {
        try self.ensureCanAddEntry();
        var iterations: u32 = 0;
        while (iterations < max_table_size + 1) : (iterations += 1) {
            const id = self.next_export_id;
            self.next_export_id +%= 1;
            if (self.exports.contains(id)) continue;
            if (self.imports.contains(id)) continue;
            if (self.receiver_answers.contains(id)) continue;
            return id;
        }
        log.err("cap table full after exhaustive ID search", .{});
        return error.CapTableFull;
    }

    fn ensureCanAddEntry(self: *const CapTable) error{CapTableFull}!void {
        const total = self.totalEntries();
        if (total >= max_table_size) {
            log.err("cap table full ({} entries)", .{total});
            return error.CapTableFull;
        }
        if (total >= max_table_size * 9 / 10) {
            log.warn("cap table near full: {}/{} entries", .{ total, max_table_size });
        }
    }
};

const ImportEntry = struct {
    ref_count: u32,
};
