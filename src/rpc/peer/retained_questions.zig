const std = @import("std");

/// Lifetime policy for an outbound call's remote answer.
///
/// Automatic calls preserve the historic behavior: dispatch the Return and
/// immediately send Finish. Retained calls leave the remote answer open after
/// dispatch until the caller explicitly finishes it or transfers ownership to
/// another protocol lifecycle.
pub const ResultLifetime = enum {
    automatic,
    retained,
};

/// Additive options shared by the `Peer.sendCall*WithOptions` family.
pub const CallOptions = struct {
    result_lifetime: ResultLifetime = .automatic,
};

pub const State = enum {
    /// The Call is on the wire and its terminal Return has not been dispatched.
    pending,
    /// The terminal Return says no Finish is needed. Kept only across callback
    /// dispatch so reentrant ownership APIs can reject precisely, then retired.
    no_finish_needed,
    /// The terminal Return was dispatched and the caller owns the open answer.
    returned,
    /// Caller-owned Finish is synchronously being emitted. Reentrant Finish
    /// attempts reject; a send failure rolls this back to `returned`.
    finishing,
    /// Ownership is provisionally moving into another protocol lifecycle.
    /// Rollback restores `returned`; commit advances to `transferred`.
    transferring,
    /// A coupled protocol lifecycle owns the open source answer. Only that
    /// owner may Finish it; failures remain retryable in this state.
    transferred,
    /// Transfer-owner Finish is synchronously being emitted. A send failure
    /// rolls this back to `transferred` for later lifecycle retry.
    finishing_transferred,
};

pub const Entry = struct {
    /// Wire answer currently owned by this logical caller question. Normally
    /// identical to the map key; redirected `awaitFromThirdParty` results
    /// replace it with the callee-chosen adopted answer id.
    wire_answer_id: u32,
    state: State = .pending,
    /// Loopback questions have no remote answer table. They still participate
    /// in the explicit-lifetime API, but Finish retires them locally.
    is_loopback: bool = false,
    /// The protocol lifecycle that owns this transferred answer has ended.
    /// Kept across transport send failures so peer maintenance can retry the
    /// exactly-once Finish without retaining a separate coupling object.
    finish_requested: bool = false,
};

pub const FinishTarget = struct {
    wire_answer_id: u32,
    is_loopback: bool,
};

/// Peer-local registry of caller-owned retained answers.
///
/// Entries are created before a Call can be emitted, which makes a synchronous
/// Return callback safe: it can finish or begin transferring the answer without
/// allocating or racing a post-send registration step.
pub const Registry = struct {
    entries: std.AutoHashMap(u32, Entry),

    pub fn init(allocator: std.mem.Allocator) Registry {
        return .{ .entries = std.AutoHashMap(u32, Entry).init(allocator) };
    }

    pub fn deinit(self: *Registry) void {
        self.entries.deinit();
    }

    pub fn clearRetainingCapacity(self: *Registry) void {
        self.entries.clearRetainingCapacity();
    }

    pub fn count(self: *const Registry) usize {
        return self.entries.count();
    }

    pub fn contains(self: *const Registry, question_id: u32) bool {
        return self.entries.contains(question_id);
    }

    /// Retained answers reserve both their caller-visible logical id and the
    /// current wire answer id adopted through `awaitFromThirdParty`.
    pub fn containsLogicalOrWire(self: *const Registry, question_id: u32) bool {
        if (self.contains(question_id)) return true;
        return self.logicalQuestionIdForWire(question_id) != null;
    }

    pub fn get(self: *const Registry, question_id: u32) ?Entry {
        return self.entries.get(question_id);
    }

    pub fn logicalQuestionIdForWire(self: *const Registry, wire_answer_id: u32) ?u32 {
        var it = self.entries.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.wire_answer_id == wire_answer_id) return entry.key_ptr.*;
        }
        return null;
    }

    pub fn retireLogicalOrWire(self: *Registry, question_or_wire_id: u32) bool {
        if (self.retire(question_or_wire_id)) return true;
        const logical_id = self.logicalQuestionIdForWire(question_or_wire_id) orelse return false;
        return self.retire(logical_id);
    }

    pub fn ensureUnusedCapacity(self: *Registry, additional: u32) !void {
        try self.entries.ensureUnusedCapacity(additional);
    }

    pub fn registerAssumeCapacity(self: *Registry, question_id: u32) void {
        self.entries.putAssumeCapacityNoClobber(question_id, .{ .wire_answer_id = question_id });
    }

    pub fn register(self: *Registry, question_id: u32) !void {
        try self.entries.putNoClobber(question_id, .{ .wire_answer_id = question_id });
    }

    pub fn retire(self: *Registry, question_id: u32) bool {
        return self.entries.remove(question_id);
    }

    /// Make a completed retained answer available before invoking its callback.
    /// `noFinishNeeded` enters a transient rejecting state across callback
    /// dispatch, then the peer retires it because no remote lifetime remains.
    pub fn noteReturn(
        self: *Registry,
        question_id: u32,
        no_finish_needed: bool,
        is_loopback: bool,
    ) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state != .pending) return;
        entry.state = if (no_finish_needed) .no_finish_needed else .returned;
        entry.is_loopback = is_loopback;
    }

    pub fn rollbackReturn(self: *Registry, question_id: u32) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state == .returned or entry.state == .no_finish_needed) {
            entry.state = .pending;
            entry.is_loopback = false;
        }
    }

    pub fn completeNoFinishNeeded(self: *Registry, question_id: u32) bool {
        const entry = self.entries.get(question_id) orelse return false;
        if (entry.state != .no_finish_needed) return false;
        return self.retire(question_id);
    }

    pub fn beginTransfer(self: *Registry, question_id: u32) !u32 {
        const entry = self.entries.getPtr(question_id) orelse return error.UnknownRetainedQuestion;
        if (entry.is_loopback) return error.RetainedLoopbackQuestion;
        switch (entry.state) {
            .pending => return error.RetainedQuestionPending,
            .no_finish_needed => return error.RetainedQuestionNoFinishNeeded,
            .returned => entry.state = .transferring,
            .finishing => return error.RetainedQuestionFinishInProgress,
            .transferring => return error.RetainedQuestionTransferInProgress,
            .transferred => return error.RetainedQuestionAlreadyTransferred,
            .finishing_transferred => return error.RetainedQuestionFinishInProgress,
        }
        return entry.wire_answer_id;
    }

    pub fn rollbackTransfer(self: *Registry, question_id: u32) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state == .transferring) entry.state = .returned;
    }

    pub fn commitTransfer(self: *Registry, question_id: u32) bool {
        const entry = self.entries.getPtr(question_id) orelse return false;
        if (entry.state != .transferring) return false;
        entry.state = .transferred;
        return true;
    }

    pub fn completeTransfer(self: *Registry, question_id: u32) bool {
        const entry = self.entries.get(question_id) orelse return false;
        if (entry.state != .transferred) return false;
        return self.retire(question_id);
    }

    pub fn countTransferred(self: *const Registry) usize {
        var transferred_count: usize = 0;
        var it = self.entries.valueIterator();
        while (it.next()) |entry| {
            if (entry.state == .transferred or
                entry.state == .transferring or
                entry.state == .finishing_transferred)
            {
                transferred_count += 1;
            }
        }
        return transferred_count;
    }

    pub fn countCallerOwned(self: *const Registry) usize {
        return self.count() - self.countTransferred();
    }

    pub fn beginCallerFinish(self: *Registry, question_id: u32) !FinishTarget {
        const entry = self.entries.getPtr(question_id) orelse return error.UnknownRetainedQuestion;
        switch (entry.state) {
            .pending => return error.RetainedQuestionPending,
            .no_finish_needed => return error.RetainedQuestionNoFinishNeeded,
            .returned => entry.state = .finishing,
            .finishing, .finishing_transferred => return error.RetainedQuestionFinishInProgress,
            .transferring => return error.RetainedQuestionTransferInProgress,
            .transferred => return error.RetainedQuestionAlreadyTransferred,
        }
        return .{
            .wire_answer_id = entry.wire_answer_id,
            .is_loopback = entry.is_loopback,
        };
    }

    pub fn rollbackCallerFinish(self: *Registry, question_id: u32) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state == .finishing) entry.state = .returned;
    }

    pub fn beginTransferredFinish(self: *Registry, question_id: u32) !FinishTarget {
        const entry = self.entries.getPtr(question_id) orelse return error.UnknownRetainedQuestion;
        switch (entry.state) {
            .transferred => entry.state = .finishing_transferred,
            .finishing_transferred, .finishing => return error.RetainedQuestionFinishInProgress,
            .pending, .no_finish_needed, .returned => return error.RetainedQuestionNotTransferred,
            .transferring => return error.RetainedQuestionTransferInProgress,
        }
        return .{
            .wire_answer_id = entry.wire_answer_id,
            .is_loopback = entry.is_loopback,
        };
    }

    /// Retarget a still-pending logical question to the callee-chosen answer id
    /// adopted after `Return.awaitFromThirdParty`. This runs before a buffered
    /// terminal Return can replay, so reentrant Finish/transfer APIs always use
    /// the adopted wire identity.
    pub fn adoptWireAnswer(self: *Registry, question_id: u32, adopted_answer_id: u32) !u32 {
        const entry = self.entries.getPtr(question_id) orelse return error.UnknownRetainedQuestion;
        if (entry.state != .pending) return error.RetainedQuestionAlreadyReturned;
        const previous = entry.wire_answer_id;
        entry.wire_answer_id = adopted_answer_id;
        return previous;
    }

    pub fn rollbackWireAnswer(
        self: *Registry,
        question_id: u32,
        adopted_answer_id: u32,
        previous_answer_id: u32,
    ) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state == .pending and entry.wire_answer_id == adopted_answer_id) {
            entry.wire_answer_id = previous_answer_id;
        }
    }

    pub fn requestTransferredFinish(self: *Registry, question_id: u32) !void {
        const entry = self.entries.getPtr(question_id) orelse return error.UnknownRetainedQuestion;
        switch (entry.state) {
            .transferred, .finishing_transferred => entry.finish_requested = true,
            .finishing => return error.RetainedQuestionFinishInProgress,
            .pending, .no_finish_needed, .returned => return error.RetainedQuestionNotTransferred,
            .transferring => return error.RetainedQuestionTransferInProgress,
        }
    }

    pub fn rollbackTransferredFinish(self: *Registry, question_id: u32) void {
        const entry = self.entries.getPtr(question_id) orelse return;
        if (entry.state == .finishing_transferred) entry.state = .transferred;
    }

    pub fn completeFinish(self: *Registry, question_id: u32) bool {
        const entry = self.entries.get(question_id) orelse return false;
        if (entry.state != .finishing and entry.state != .finishing_transferred) return false;
        return self.retire(question_id);
    }
};

test "retained registry transitions are allocation-free and retryable" {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.ensureUnusedCapacity(1);
    registry.registerAssumeCapacity(7);
    try std.testing.expectError(error.RetainedQuestionPending, registry.beginTransfer(7));

    registry.noteReturn(7, false, false);
    try std.testing.expectEqual(@as(u32, 7), try registry.beginTransfer(7));
    try std.testing.expectEqual(State.transferring, registry.get(7).?.state);
    registry.rollbackTransfer(7);
    try std.testing.expectEqual(State.returned, registry.get(7).?.state);

    _ = try registry.beginTransfer(7);
    try std.testing.expect(registry.commitTransfer(7));
    try std.testing.expectEqual(State.transferred, registry.get(7).?.state);
    try std.testing.expect(registry.completeTransfer(7));
    try std.testing.expectEqual(@as(usize, 0), registry.count());
}

test "noFinishNeeded retires before callback-visible state" {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.ensureUnusedCapacity(1);
    registry.registerAssumeCapacity(9);
    registry.noteReturn(9, true, false);
    try std.testing.expectError(error.RetainedQuestionNoFinishNeeded, registry.beginTransfer(9));
    try std.testing.expect(registry.completeNoFinishNeeded(9));
    try std.testing.expect(!registry.contains(9));
}

test "caller and transfer-owner Finish states reject reentrancy and roll back" {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.ensureUnusedCapacity(2);
    registry.registerAssumeCapacity(11);
    registry.noteReturn(11, false, false);
    const caller_finish = try registry.beginCallerFinish(11);
    try std.testing.expect(!caller_finish.is_loopback);
    try std.testing.expectEqual(@as(u32, 11), caller_finish.wire_answer_id);
    try std.testing.expectError(error.RetainedQuestionFinishInProgress, registry.beginCallerFinish(11));
    registry.rollbackCallerFinish(11);
    try std.testing.expectEqual(State.returned, registry.get(11).?.state);

    _ = try registry.beginTransfer(11);
    try std.testing.expect(registry.commitTransfer(11));
    try registry.requestTransferredFinish(11);
    try std.testing.expect(registry.get(11).?.finish_requested);
    const transferred_finish = try registry.beginTransferredFinish(11);
    try std.testing.expect(!transferred_finish.is_loopback);
    try std.testing.expectEqual(@as(u32, 11), transferred_finish.wire_answer_id);
    try std.testing.expectError(error.RetainedQuestionFinishInProgress, registry.beginTransferredFinish(11));
    registry.rollbackTransferredFinish(11);
    try std.testing.expectEqual(State.transferred, registry.get(11).?.state);
    try std.testing.expect(registry.get(11).?.finish_requested);
    _ = try registry.beginTransferredFinish(11);
    try std.testing.expect(registry.completeFinish(11));
}

test "adopted wire answer identity survives transfer and finish" {
    var registry = Registry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.register(21);
    const previous = try registry.adoptWireAnswer(21, 0x4000_0021);
    try std.testing.expectEqual(@as(u32, 21), previous);
    try std.testing.expectEqual(
        @as(?u32, 21),
        registry.logicalQuestionIdForWire(0x4000_0021),
    );
    registry.noteReturn(21, false, false);
    try std.testing.expectEqual(@as(u32, 0x4000_0021), try registry.beginTransfer(21));
    try std.testing.expect(registry.commitTransfer(21));
    const finish = try registry.beginTransferredFinish(21);
    try std.testing.expectEqual(@as(u32, 0x4000_0021), finish.wire_answer_id);
    try std.testing.expect(registry.completeFinish(21));
}
