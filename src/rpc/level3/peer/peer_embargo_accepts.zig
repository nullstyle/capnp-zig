const std = @import("std");

pub fn queueEmbargoedAccept(
    comptime PendingAcceptType: type,
    allocator: std.mem.Allocator,
    pending_accepts_by_embargo: *std.StringHashMap(std.ArrayList(PendingAcceptType)),
    pending_accept_embargo_by_question: *std.AutoHashMap(u32, []u8),
    answer_id: u32,
    provided_question_id: u32,
    embargo: []const u8,
) !void {
    // Maintain both lookup directions:
    // - embargo key -> pending accept list
    // - answer id -> embargo key (for finish/cancel cleanup)
    //
    // Register the question→embargo mapping first so that if the embargo→accepts
    // update fails we can undo it without leaving a partial commit.
    const embargo_copy = try allocator.alloc(u8, embargo.len);
    errdefer allocator.free(embargo_copy);
    std.mem.copyForwards(u8, embargo_copy, embargo);

    try pending_accept_embargo_by_question.put(answer_id, embargo_copy);
    errdefer _ = pending_accept_embargo_by_question.remove(answer_id);

    if (pending_accepts_by_embargo.getPtr(embargo)) |pending| {
        try pending.append(allocator, .{
            .answer_id = answer_id,
            .provided_question_id = provided_question_id,
        });
    } else {
        const key = try allocator.alloc(u8, embargo.len);
        errdefer allocator.free(key);
        std.mem.copyForwards(u8, key, embargo);

        var pending = std.ArrayList(PendingAcceptType){};
        errdefer pending.deinit(allocator);
        try pending.append(allocator, .{
            .answer_id = answer_id,
            .provided_question_id = provided_question_id,
        });
        try pending_accepts_by_embargo.put(key, pending);
    }
}

pub fn queueEmbargoedAcceptForPeer(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
    peer: *PeerType,
    answer_id: u32,
    provided_question_id: u32,
    embargo: []const u8,
) !void {
    try queueEmbargoedAccept(
        PendingAcceptType,
        peer.allocator,
        &peer.pending_accepts_by_embargo,
        &peer.pending_accept_embargo_by_question,
        answer_id,
        provided_question_id,
        embargo,
    );
}

pub fn queueEmbargoedAcceptForPeerFn(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
) *const fn (*PeerType, u32, u32, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, answer_id: u32, provided_question_id: u32, embargo: []const u8) anyerror!void {
            try queueEmbargoedAcceptForPeer(
                PeerType,
                PendingAcceptType,
                peer,
                answer_id,
                provided_question_id,
                embargo,
            );
        }
    }.call;
}

pub fn clearPendingAcceptQuestion(
    comptime PendingAcceptType: type,
    allocator: std.mem.Allocator,
    pending_accepts_by_embargo: *std.StringHashMap(std.ArrayList(PendingAcceptType)),
    pending_accept_embargo_by_question: *std.AutoHashMap(u32, []u8),
    question_id: u32,
) void {
    const embargo_entry = pending_accept_embargo_by_question.fetchRemove(question_id) orelse return;
    const embargo_key = embargo_entry.value;
    defer allocator.free(embargo_key);

    if (pending_accepts_by_embargo.getEntry(embargo_key)) |entry| {
        const pending = entry.value_ptr;
        var idx: usize = 0;
        while (idx < pending.items.len) : (idx += 1) {
            if (pending.items[idx].answer_id == question_id) {
                _ = pending.swapRemove(idx);
                break;
            }
        }

        if (pending.items.len == 0) {
            // Drop the embargo bucket once no queued accepts remain.
            if (pending_accepts_by_embargo.fetchRemove(embargo_key)) |removed| {
                allocator.free(removed.key);
                var removed_list = removed.value;
                removed_list.deinit(allocator);
            }
        }
    }
}

pub fn clearPendingAcceptQuestionForPeer(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
    peer: *PeerType,
    question_id: u32,
) void {
    clearPendingAcceptQuestion(
        PendingAcceptType,
        peer.allocator,
        &peer.pending_accepts_by_embargo,
        &peer.pending_accept_embargo_by_question,
        question_id,
    );
}

pub fn clearPendingAcceptQuestionForPeerFn(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, question_id: u32) void {
            clearPendingAcceptQuestionForPeer(
                PeerType,
                PendingAcceptType,
                peer,
                question_id,
            );
        }
    }.call;
}

pub fn releaseEmbargoedAccepts(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
    comptime ProvideEntryType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    pending_accepts_by_embargo: *std.StringHashMap(std.ArrayList(PendingAcceptType)),
    pending_accept_embargo_by_question: *std.AutoHashMap(u32, []u8),
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    embargo: []const u8,
    send_return_provided_entry: *const fn (*PeerType, u32, *const ProvideEntryType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    const pending_entry = pending_accepts_by_embargo.fetchRemove(embargo) orelse return;
    var pending_list = pending_entry.value;
    defer {
        allocator.free(pending_entry.key);
        pending_list.deinit(allocator);
    }

    for (pending_list.items) |pending| {
        if (pending_accept_embargo_by_question.fetchRemove(pending.answer_id)) |embargo_key| {
            allocator.free(embargo_key.value);
        }

        const provided = provides_by_question.getPtr(pending.provided_question_id) orelse {
            try send_return_exception(peer, pending.answer_id, "unknown provision");
            continue;
        };

        send_return_provided_entry(peer, pending.answer_id, provided) catch |err| {
            // Preserve accept completion with an exception if target send fails.
            try send_return_exception(peer, pending.answer_id, @errorName(err));
        };
    }
}

pub fn releaseEmbargoedAcceptsForPeer(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    embargo: []const u8,
    comptime send_return_provided_target: *const fn (*PeerType, u32, *const ProvideTargetType) anyerror!void,
    comptime send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    const Adapter = struct {
        fn sendProvided(peer_ctx: *PeerType, answer_id: u32, entry: *const ProvideEntryType) anyerror!void {
            return send_return_provided_target(peer_ctx, answer_id, &entry.target);
        }
    };

    try releaseEmbargoedAccepts(
        PeerType,
        PendingAcceptType,
        ProvideEntryType,
        peer,
        peer.allocator,
        &peer.pending_accepts_by_embargo,
        &peer.pending_accept_embargo_by_question,
        &peer.provides_by_question,
        embargo,
        Adapter.sendProvided,
        send_return_exception,
    );
}

pub fn releaseEmbargoedAcceptsForPeerFn(
    comptime PeerType: type,
    comptime PendingAcceptType: type,
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    comptime send_return_provided_target: *const fn (*PeerType, u32, *const ProvideTargetType) anyerror!void,
    comptime send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) *const fn (*PeerType, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, embargo: []const u8) anyerror!void {
            try releaseEmbargoedAcceptsForPeer(
                PeerType,
                PendingAcceptType,
                ProvideEntryType,
                ProvideTargetType,
                peer,
                embargo,
                send_return_provided_target,
                send_return_exception,
            );
        }
    }.call;
}

fn cleanupPendingMaps(
    allocator: std.mem.Allocator,
    pending_accepts_by_embargo: *std.StringHashMap(std.ArrayList(TestPendingAccept)),
    pending_accept_embargo_by_question: *std.AutoHashMap(u32, []u8),
) void {
    var pending_it = pending_accepts_by_embargo.iterator();
    while (pending_it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit(allocator);
    }
    pending_accepts_by_embargo.deinit();

    var question_it = pending_accept_embargo_by_question.valueIterator();
    while (question_it.next()) |embargo_key| {
        allocator.free(embargo_key.*);
    }
    pending_accept_embargo_by_question.deinit();
}

const TestPendingAccept = struct {
    answer_id: u32,
    provided_question_id: u32,
};

test "peer_embargo_accepts queue and clear keep maps consistent" {
    var pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(TestPendingAccept)).init(std.testing.allocator);
    var pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    defer cleanupPendingMaps(
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
    );

    try queueEmbargoedAccept(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        10,
        100,
        "accept-embargo",
    );
    try queueEmbargoedAccept(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        11,
        101,
        "accept-embargo",
    );

    try std.testing.expectEqual(@as(usize, 1), pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 2), pending_accept_embargo_by_question.count());
    const list = pending_accepts_by_embargo.get("accept-embargo") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), list.items.len);

    clearPendingAcceptQuestion(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        10,
    );

    try std.testing.expectEqual(@as(usize, 1), pending_accept_embargo_by_question.count());
    const remaining = pending_accepts_by_embargo.get("accept-embargo") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 1), remaining.items.len);
    try std.testing.expectEqual(@as(u32, 11), remaining.items[0].answer_id);

    clearPendingAcceptQuestion(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        11,
    );
    try std.testing.expectEqual(@as(usize, 0), pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), pending_accept_embargo_by_question.count());
}

test "peer_embargo_accepts release routes to provided target and exception for missing provision" {
    const ProvideEntry = struct {
        target: u32,
    };
    const SentResult = struct {
        answer_id: u32,
        target: u32,
    };
    const SentException = struct {
        answer_id: u32,
        reason: []u8,
    };
    const State = struct {
        allocator: std.mem.Allocator,
        provided: std.ArrayList(SentResult),
        exceptions: std.ArrayList(SentException),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .provided = std.ArrayList(SentResult){},
                .exceptions = std.ArrayList(SentException){},
            };
        }

        fn deinit(self: *@This()) void {
            for (self.exceptions.items) |entry| self.allocator.free(entry.reason);
            self.provided.deinit(self.allocator);
            self.exceptions.deinit(self.allocator);
        }
    };
    const Hooks = struct {
        fn sendProvided(state: *State, answer_id: u32, entry: *const ProvideEntry) !void {
            try state.provided.append(state.allocator, .{
                .answer_id = answer_id,
                .target = entry.target,
            });
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            const reason_copy = try state.allocator.dupe(u8, reason);
            errdefer state.allocator.free(reason_copy);
            try state.exceptions.append(state.allocator, .{
                .answer_id = answer_id,
                .reason = reason_copy,
            });
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    var pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(TestPendingAccept)).init(std.testing.allocator);
    var pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    defer cleanupPendingMaps(
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
    );

    var provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator);
    defer provides_by_question.deinit();
    try provides_by_question.put(100, .{ .target = 7 });

    try queueEmbargoedAccept(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        20,
        100,
        "release-key",
    );
    try queueEmbargoedAccept(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        21,
        999,
        "release-key",
    );

    try releaseEmbargoedAccepts(
        State,
        TestPendingAccept,
        ProvideEntry,
        &state,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        &provides_by_question,
        "release-key",
        Hooks.sendProvided,
        Hooks.sendException,
    );

    try std.testing.expectEqual(@as(usize, 0), pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), pending_accept_embargo_by_question.count());
    try std.testing.expectEqual(@as(usize, 1), state.provided.items.len);
    try std.testing.expectEqual(@as(u32, 20), state.provided.items[0].answer_id);
    try std.testing.expectEqual(@as(u32, 7), state.provided.items[0].target);
    try std.testing.expectEqual(@as(usize, 1), state.exceptions.items.len);
    try std.testing.expectEqual(@as(u32, 21), state.exceptions.items[0].answer_id);
    try std.testing.expectEqualStrings("unknown provision", state.exceptions.items[0].reason);
}

test "peer_embargo_accepts release converts provided-send errors to return exceptions" {
    const ProvideEntry = struct {
        target: u32,
    };
    const SentException = struct {
        answer_id: u32,
        reason: []u8,
    };
    const State = struct {
        allocator: std.mem.Allocator,
        exceptions: std.ArrayList(SentException),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .exceptions = std.ArrayList(SentException){},
            };
        }

        fn deinit(self: *@This()) void {
            for (self.exceptions.items) |entry| self.allocator.free(entry.reason);
            self.exceptions.deinit(self.allocator);
        }
    };
    const Hooks = struct {
        fn sendProvided(_: *State, _: u32, _: *const ProvideEntry) !void {
            return error.TestExpectedError;
        }

        fn sendException(state: *State, answer_id: u32, reason: []const u8) !void {
            const reason_copy = try state.allocator.dupe(u8, reason);
            errdefer state.allocator.free(reason_copy);
            try state.exceptions.append(state.allocator, .{
                .answer_id = answer_id,
                .reason = reason_copy,
            });
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    var pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(TestPendingAccept)).init(std.testing.allocator);
    var pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    defer cleanupPendingMaps(
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
    );

    var provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator);
    defer provides_by_question.deinit();
    try provides_by_question.put(200, .{ .target = 42 });

    try queueEmbargoedAccept(
        TestPendingAccept,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        30,
        200,
        "error-key",
    );

    try releaseEmbargoedAccepts(
        State,
        TestPendingAccept,
        ProvideEntry,
        &state,
        std.testing.allocator,
        &pending_accepts_by_embargo,
        &pending_accept_embargo_by_question,
        &provides_by_question,
        "error-key",
        Hooks.sendProvided,
        Hooks.sendException,
    );

    try std.testing.expectEqual(@as(usize, 1), state.exceptions.items.len);
    try std.testing.expectEqual(@as(u32, 30), state.exceptions.items[0].answer_id);
    try std.testing.expectEqualStrings("TestExpectedError", state.exceptions.items[0].reason);
}

test "peer_embargo_accepts peer helper factories queue and release through peer maps" {
    const ProvideTarget = struct {
        id: u32,
    };
    const ProvideEntry = struct {
        target: ProvideTarget,
    };
    const SentProvided = struct {
        answer_id: u32,
        target_id: u32,
    };
    const SentException = struct {
        answer_id: u32,
        reason: []u8,
    };
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(TestPendingAccept)),
        pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
        provides_by_question: std.AutoHashMap(u32, ProvideEntry),
        provided: std.ArrayList(SentProvided),
        exceptions: std.ArrayList(SentException),

        fn deinit(self: *@This()) void {
            cleanupPendingMaps(
                self.allocator,
                &self.pending_accepts_by_embargo,
                &self.pending_accept_embargo_by_question,
            );
            self.provides_by_question.deinit();
            for (self.exceptions.items) |entry| self.allocator.free(entry.reason);
            self.provided.deinit(self.allocator);
            self.exceptions.deinit(self.allocator);
        }
    };
    const Hooks = struct {
        fn sendProvided(peer: *FakePeer, answer_id: u32, target: *const ProvideTarget) !void {
            try peer.provided.append(peer.allocator, .{
                .answer_id = answer_id,
                .target_id = target.id,
            });
        }

        fn sendException(peer: *FakePeer, answer_id: u32, reason: []const u8) !void {
            const reason_copy = try peer.allocator.dupe(u8, reason);
            errdefer peer.allocator.free(reason_copy);
            try peer.exceptions.append(peer.allocator, .{
                .answer_id = answer_id,
                .reason = reason_copy,
            });
        }
    };

    var peer = FakePeer{
        .allocator = std.testing.allocator,
        .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(TestPendingAccept)).init(std.testing.allocator),
        .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(std.testing.allocator),
        .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator),
        .provided = std.ArrayList(SentProvided){},
        .exceptions = std.ArrayList(SentException){},
    };
    defer peer.deinit();

    try peer.provides_by_question.put(900, .{ .target = .{ .id = 55 } });

    const queue_fn = queueEmbargoedAcceptForPeerFn(FakePeer, TestPendingAccept);
    try queue_fn(&peer, 44, 900, "peer-release-key");
    try queue_fn(&peer, 45, 999, "peer-release-key");

    const release_fn = releaseEmbargoedAcceptsForPeerFn(
        FakePeer,
        TestPendingAccept,
        ProvideEntry,
        ProvideTarget,
        Hooks.sendProvided,
        Hooks.sendException,
    );
    try release_fn(&peer, "peer-release-key");

    try std.testing.expectEqual(@as(usize, 0), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accept_embargo_by_question.count());
    try std.testing.expectEqual(@as(usize, 1), peer.provided.items.len);
    try std.testing.expectEqual(@as(u32, 44), peer.provided.items[0].answer_id);
    try std.testing.expectEqual(@as(u32, 55), peer.provided.items[0].target_id);
    try std.testing.expectEqual(@as(usize, 1), peer.exceptions.items.len);
    try std.testing.expectEqual(@as(u32, 45), peer.exceptions.items[0].answer_id);
    try std.testing.expectEqualStrings("unknown provision", peer.exceptions.items[0].reason);
}
