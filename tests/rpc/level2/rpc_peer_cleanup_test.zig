const std = @import("std");
const capnpc = @import("capnpc-zig");

const cleanup = capnpc.rpc._internal.peer_cleanup;

test "peer_cleanup deinitPendingCallMapOwned releases frames and calls caps deinit" {
    var caps_deinit_count: usize = 0;

    const Caps = struct {
        count: *usize,

        pub fn deinit(self: *@This()) void {
            self.count.* += 1;
        }
    };

    const Pending = struct {
        caps: Caps,
        frame: []u8,
    };

    var pending_map = std.AutoHashMap(u32, std.ArrayList(Pending)).init(std.testing.allocator);

    var list_a = std.ArrayList(Pending){};
    try list_a.append(std.testing.allocator, .{
        .caps = .{ .count = &caps_deinit_count },
        .frame = try std.testing.allocator.alloc(u8, 3),
    });
    try pending_map.put(1, list_a);

    var list_b = std.ArrayList(Pending){};
    try list_b.append(std.testing.allocator, .{
        .caps = .{ .count = &caps_deinit_count },
        .frame = try std.testing.allocator.alloc(u8, 4),
    });
    try list_b.append(std.testing.allocator, .{
        .caps = .{ .count = &caps_deinit_count },
        .frame = try std.testing.allocator.alloc(u8, 5),
    });
    try pending_map.put(2, list_b);

    cleanup.deinitPendingCallMapOwned(
        @TypeOf(pending_map),
        std.testing.allocator,
        &pending_map,
    );
    try std.testing.expectEqual(@as(usize, 3), caps_deinit_count);
}

test "peer_cleanup deinitResolvedAnswerMap/deinitProvideEntryMap/deinitJoinStateMap apply per-value destructors" {
    var target_deinit_count: usize = 0;
    var join_deinit_count: usize = 0;

    const ResolvedAnswer = struct {
        frame: []u8,
    };
    const Target = struct {
        count: *usize,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            _ = allocator;
            self.count.* += 1;
        }
    };
    const ProvideEntry = struct {
        recipient_key: []u8,
        target: Target,
    };
    const JoinState = struct {
        count: *usize,

        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            _ = allocator;
            self.count.* += 1;
        }
    };

    var answers = std.AutoHashMap(u32, ResolvedAnswer).init(std.testing.allocator);
    try answers.put(1, .{ .frame = try std.testing.allocator.alloc(u8, 3) });
    try answers.put(2, .{ .frame = try std.testing.allocator.alloc(u8, 4) });
    cleanup.deinitResolvedAnswerMap(@TypeOf(answers), std.testing.allocator, &answers);

    var provides = std.AutoHashMap(u32, ProvideEntry).init(std.testing.allocator);
    try provides.put(10, .{
        .recipient_key = try std.testing.allocator.dupe(u8, "recipient-a"),
        .target = .{ .count = &target_deinit_count },
    });
    try provides.put(11, .{
        .recipient_key = try std.testing.allocator.dupe(u8, "recipient-b"),
        .target = .{ .count = &target_deinit_count },
    });
    cleanup.deinitProvideEntryMap(@TypeOf(provides), std.testing.allocator, &provides);
    try std.testing.expectEqual(@as(usize, 2), target_deinit_count);

    var joins = std.AutoHashMap(u32, JoinState).init(std.testing.allocator);
    try joins.put(20, .{ .count = &join_deinit_count });
    try joins.put(21, .{ .count = &join_deinit_count });
    cleanup.deinitJoinStateMap(@TypeOf(joins), std.testing.allocator, &joins);
    try std.testing.expectEqual(@as(usize, 2), join_deinit_count);
}

test "peer_cleanup deinitOwnedStringKeyMap and deinitOwnedStringKeyListMap free key storage" {
    var map = std.StringHashMap(u32).init(std.testing.allocator);

    const key_a = try std.testing.allocator.dupe(u8, "alpha");
    const key_b = try std.testing.allocator.dupe(u8, "beta");
    try map.put(key_a, 10);
    try map.put(key_b, 20);

    cleanup.deinitOwnedStringKeyMap(@TypeOf(map), std.testing.allocator, &map);

    var list_map = std.StringHashMap(std.ArrayList(u32)).init(std.testing.allocator);
    const list_key = try std.testing.allocator.dupe(u8, "list");
    var values = std.ArrayList(u32){};
    try values.append(std.testing.allocator, 1);
    try values.append(std.testing.allocator, 2);
    try list_map.put(list_key, values);
    cleanup.deinitOwnedStringKeyListMap(@TypeOf(list_map), std.testing.allocator, &list_map);
}

test "peer_cleanup deinitOwnedBytesMap/deinitOptionalOwnedBytesMap/clearOptionalOwnedBytes free payloads" {
    var bytes_map = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    try bytes_map.put(1, try std.testing.allocator.alloc(u8, 6));
    try bytes_map.put(2, try std.testing.allocator.alloc(u8, 7));
    cleanup.deinitOwnedBytesMap(@TypeOf(bytes_map), std.testing.allocator, &bytes_map);

    var maybe_map = std.AutoHashMap(u32, ?[]u8).init(std.testing.allocator);
    try maybe_map.put(1, try std.testing.allocator.alloc(u8, 6));
    try maybe_map.put(2, null);
    cleanup.deinitOptionalOwnedBytesMap(@TypeOf(maybe_map), std.testing.allocator, &maybe_map);

    var reason: ?[]u8 = try std.testing.allocator.alloc(u8, 7);
    cleanup.clearOptionalOwnedBytes(std.testing.allocator, &reason);
    try std.testing.expect(reason == null);
}
