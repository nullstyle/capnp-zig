const std = @import("std");

// Provide state keeps two synchronized indexes:
// - question_id -> provide entry (owns recipient_key + target)
// - recipient_key -> question_id (borrowed key view)
pub fn hasProvideQuestion(
    comptime ProvideEntryType: type,
    provides_by_question: *const std.AutoHashMap(u32, ProvideEntryType),
    question_id: u32,
) bool {
    return provides_by_question.contains(question_id);
}

pub fn hasProvideRecipient(
    provides_by_key: *const std.StringHashMap(u32),
    recipient_key: []const u8,
) bool {
    return provides_by_key.contains(recipient_key);
}

pub fn putProvideByQuestion(
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    question_id: u32,
    recipient_key: []u8,
    target: ProvideTargetType,
) !void {
    try provides_by_question.put(question_id, .{
        .recipient_key = recipient_key,
        .target = target,
    });
}

pub fn putProvideByKey(
    provides_by_key: *std.StringHashMap(u32),
    recipient_key: []const u8,
    question_id: u32,
) !void {
    try provides_by_key.put(recipient_key, question_id);
}

pub fn getProvidedQuestion(
    provides_by_key: *const std.StringHashMap(u32),
    recipient_key: []const u8,
) ?u32 {
    return provides_by_key.get(recipient_key);
}

pub fn getProvidedTarget(
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    provided_question_id: u32,
) ?*ProvideTargetType {
    const entry = provides_by_question.getPtr(provided_question_id) orelse return null;
    return &entry.target;
}

pub fn clearProvide(
    comptime ProvideEntryType: type,
    comptime ProvideTargetType: type,
    allocator: std.mem.Allocator,
    provides_by_question: *std.AutoHashMap(u32, ProvideEntryType),
    provides_by_key: *std.StringHashMap(u32),
    question_id: u32,
    deinit_target: *const fn (*ProvideTargetType, std.mem.Allocator) void,
) void {
    if (provides_by_question.fetchRemove(question_id)) |removed| {
        // The key map borrows the same bytes owned by the question map entry.
        _ = provides_by_key.remove(removed.value.recipient_key);
        allocator.free(removed.value.recipient_key);

        var target = removed.value.target;
        deinit_target(&target, allocator);
    }
}

const TestTarget = struct {
    id: u32,
    deinit_count: *usize,
};

const TestProvideEntry = struct {
    recipient_key: []u8,
    target: TestTarget,
};

fn deinitTestTarget(target: *TestTarget, allocator: std.mem.Allocator) void {
    _ = allocator;
    target.deinit_count.* += 1;
}

fn cleanupProvideMaps(
    allocator: std.mem.Allocator,
    provides_by_question: *std.AutoHashMap(u32, TestProvideEntry),
    provides_by_key: *std.StringHashMap(u32),
) void {
    var by_question_it = provides_by_question.valueIterator();
    while (by_question_it.next()) |entry| {
        allocator.free(entry.recipient_key);
        var target = entry.target;
        deinitTestTarget(&target, allocator);
    }
    provides_by_question.deinit();
    provides_by_key.deinit();
}

test "peer_provides_state put/get/clear keeps maps in sync" {
    var provides_by_question = std.AutoHashMap(u32, TestProvideEntry).init(std.testing.allocator);
    var provides_by_key = std.StringHashMap(u32).init(std.testing.allocator);
    defer cleanupProvideMaps(std.testing.allocator, &provides_by_question, &provides_by_key);

    var deinit_count: usize = 0;
    const recipient = try std.testing.allocator.dupe(u8, "recipient-key");

    try putProvideByQuestion(
        TestProvideEntry,
        TestTarget,
        &provides_by_question,
        44,
        recipient,
        .{
            .id = 9,
            .deinit_count = &deinit_count,
        },
    );
    try putProvideByKey(&provides_by_key, recipient, 44);

    try std.testing.expect(hasProvideQuestion(TestProvideEntry, &provides_by_question, 44));
    try std.testing.expect(hasProvideRecipient(&provides_by_key, "recipient-key"));
    try std.testing.expectEqual(@as(?u32, 44), getProvidedQuestion(&provides_by_key, "recipient-key"));

    const target_ptr = getProvidedTarget(
        TestProvideEntry,
        TestTarget,
        &provides_by_question,
        44,
    ) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u32, 9), target_ptr.id);

    clearProvide(
        TestProvideEntry,
        TestTarget,
        std.testing.allocator,
        &provides_by_question,
        &provides_by_key,
        44,
        deinitTestTarget,
    );

    try std.testing.expect(!hasProvideQuestion(TestProvideEntry, &provides_by_question, 44));
    try std.testing.expect(!hasProvideRecipient(&provides_by_key, "recipient-key"));
    try std.testing.expectEqual(@as(?u32, null), getProvidedQuestion(&provides_by_key, "recipient-key"));
    try std.testing.expectEqual(@as(usize, 1), deinit_count);
}

test "peer_provides_state clearProvide is a no-op for missing question id" {
    var provides_by_question = std.AutoHashMap(u32, TestProvideEntry).init(std.testing.allocator);
    var provides_by_key = std.StringHashMap(u32).init(std.testing.allocator);
    defer cleanupProvideMaps(std.testing.allocator, &provides_by_question, &provides_by_key);

    clearProvide(
        TestProvideEntry,
        TestTarget,
        std.testing.allocator,
        &provides_by_question,
        &provides_by_key,
        999,
        deinitTestTarget,
    );
    try std.testing.expectEqual(@as(usize, 0), provides_by_question.count());
    try std.testing.expectEqual(@as(usize, 0), provides_by_key.count());
}
