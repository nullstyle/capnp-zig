const std = @import("std");
const cap_table = @import("../cap_table.zig");

pub fn releaseInboundCaps(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    inbound: *cap_table.InboundCapTable,
    release_import: *const fn (*PeerType, u32) bool,
    release_resolved_import: *const fn (*PeerType, u32) anyerror!void,
    send_release: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    var releases = try collectReleaseCounts(
        PeerType,
        allocator,
        peer,
        inbound,
        release_import,
        release_resolved_import,
    );
    defer releases.deinit();

    var it = releases.iterator();
    while (it.next()) |entry| {
        try send_release(peer, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn collectReleaseCounts(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    inbound: *cap_table.InboundCapTable,
    release_import: *const fn (*PeerType, u32) bool,
    release_resolved_import: *const fn (*PeerType, u32) anyerror!void,
) !std.AutoHashMap(u32, u32) {
    var releases = std.AutoHashMap(u32, u32).init(allocator);
    errdefer releases.deinit();

    var idx: u32 = 0;
    while (idx < inbound.len()) : (idx += 1) {
        if (inbound.isRetained(idx)) continue;
        const entry = try inbound.get(idx);
        switch (entry) {
            .imported => |cap| {
                const removed = release_import(peer, cap.id);
                if (removed) {
                    try release_resolved_import(peer, cap.id);
                }
                const slot = try releases.getOrPut(cap.id);
                if (!slot.found_existing) {
                    slot.value_ptr.* = 1;
                } else {
                    slot.value_ptr.* +%= 1;
                }
            },
            else => {},
        }
    }

    return releases;
}

test "peer_inbound_release aggregates sendRelease counts and handles resolved-import release" {
    const State = struct {
        allocator: std.mem.Allocator,
        release_import_calls: usize = 0,
        release_resolved_import_calls: usize = 0,
        released_promise_id: u32 = 0,
        send_counts: std.AutoHashMap(u32, u32),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .send_counts = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.send_counts.deinit();
        }
    };

    const Hooks = struct {
        fn releaseImport(state: *State, import_id: u32) bool {
            state.release_import_calls += 1;
            return import_id == 7;
        }

        fn releaseResolvedImport(state: *State, promise_id: u32) !void {
            state.release_resolved_import_calls += 1;
            state.released_promise_id = promise_id;
        }

        fn sendRelease(state: *State, import_id: u32, count: u32) !void {
            const entry = try state.send_counts.getOrPut(import_id);
            if (!entry.found_existing) {
                entry.value_ptr.* = count;
            } else {
                entry.value_ptr.* +%= count;
            }
        }
    };

    var inbound = cap_table.InboundCapTable{
        .allocator = std.testing.allocator,
        .entries = try std.testing.allocator.alloc(cap_table.ResolvedCap, 4),
        .retained = try std.testing.allocator.alloc(bool, 4),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .imported = .{ .id = 5 } };
    inbound.entries[1] = .{ .imported = .{ .id = 5 } };
    inbound.entries[2] = .{ .imported = .{ .id = 7 } };
    inbound.entries[3] = .{ .none = {} };
    inbound.retained[0] = false;
    inbound.retained[1] = false;
    inbound.retained[2] = false;
    inbound.retained[3] = false;

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try releaseInboundCaps(
        State,
        std.testing.allocator,
        &state,
        &inbound,
        Hooks.releaseImport,
        Hooks.releaseResolvedImport,
        Hooks.sendRelease,
    );

    try std.testing.expectEqual(@as(usize, 3), state.release_import_calls);
    try std.testing.expectEqual(@as(usize, 1), state.release_resolved_import_calls);
    try std.testing.expectEqual(@as(u32, 7), state.released_promise_id);
    try std.testing.expectEqual(@as(u32, 2), state.send_counts.get(5) orelse 0);
    try std.testing.expectEqual(@as(u32, 1), state.send_counts.get(7) orelse 0);
}

test "peer_inbound_release skips retained entries and propagates sendRelease errors" {
    const State = struct {
        send_calls: usize = 0,
    };

    const Hooks = struct {
        fn releaseImport(_: *State, _: u32) bool {
            return false;
        }

        fn releaseResolvedImport(_: *State, _: u32) !void {
            return error.TestUnexpectedResult;
        }

        fn sendRelease(state: *State, import_id: u32, count: u32) !void {
            _ = import_id;
            _ = count;
            state.send_calls += 1;
            return error.TestExpectedError;
        }
    };

    var inbound = cap_table.InboundCapTable{
        .allocator = std.testing.allocator,
        .entries = try std.testing.allocator.alloc(cap_table.ResolvedCap, 2),
        .retained = try std.testing.allocator.alloc(bool, 2),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .imported = .{ .id = 3 } };
    inbound.entries[1] = .{ .imported = .{ .id = 4 } };
    inbound.retained[0] = true;
    inbound.retained[1] = false;

    var state = State{};
    const err = releaseInboundCaps(
        State,
        std.testing.allocator,
        &state,
        &inbound,
        Hooks.releaseImport,
        Hooks.releaseResolvedImport,
        Hooks.sendRelease,
    );
    try std.testing.expectError(error.TestExpectedError, err);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
}
