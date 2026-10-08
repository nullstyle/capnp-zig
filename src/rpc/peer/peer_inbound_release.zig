const std = @import("std");
const cap_table = @import("../caps/table.zig");

const ImportRefSpend = cap_table.CapTable.ImportRefSpend;

/// Spend the reference each unretained `.imported` entry holds, then send
/// one Release per import for the WIRE references among them. An entry of a
/// loopback table holds a loopback reference, which owes the remote nothing.
pub fn releaseInboundCaps(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    inbound: *cap_table.InboundCapTable,
    spend_import_ref: *const fn (*PeerType, u32) ImportRefSpend,
    release_resolved_import: *const fn (*PeerType, u32) anyerror!void,
    send_release: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    var releases = try collectReleaseCounts(
        PeerType,
        allocator,
        peer,
        inbound,
        spend_import_ref,
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
    spend_import_ref: *const fn (*PeerType, u32) ImportRefSpend,
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
                const spent = spend_import_ref(peer, cap.id);
                if (spent.fully_released) {
                    try release_resolved_import(peer, cap.id);
                }
                if (!spent.wire) continue;
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
        fn releaseImport(state: *State, import_id: u32) ImportRefSpend {
            state.release_import_calls += 1;
            return .{ .wire = true, .fully_released = import_id == 7 };
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
        fn releaseImport(_: *State, _: u32) ImportRefSpend {
            return .{ .wire = true, .fully_released = false };
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

test "peer_inbound_release sends no Release for a loopback reference" {
    var caps = cap_table.CapTable.init(std.testing.allocator);
    defer caps.deinit();
    try caps.noteImport(6);
    try caps.noteLoopbackImportRef(6);

    const State = struct {
        caps: *cap_table.CapTable,
        send_calls: usize = 0,

        fn spend(state: *@This(), import_id: u32) ImportRefSpend {
            return state.caps.spendImportRef(import_id);
        }

        fn releaseResolvedImport(_: *@This(), _: u32) !void {}

        fn sendRelease(state: *@This(), _: u32, _: u32) !void {
            state.send_calls += 1;
        }
    };

    // A loopback table: its `.imported` entry holds the loopback reference.
    var inbound = cap_table.InboundCapTable{
        .allocator = std.testing.allocator,
        .entries = try std.testing.allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try std.testing.allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .imported = .{ .id = 6 } };
    inbound.retained[0] = false;

    var state = State{ .caps = &caps };
    try releaseInboundCaps(State, std.testing.allocator, &state, &inbound, State.spend, State.releaseResolvedImport, State.sendRelease);
    try std.testing.expectEqual(@as(usize, 0), state.send_calls);
    // The wire reference the remote granted is still held.
    try std.testing.expectEqual(@as(u32, 1), caps.imports.get(6).?.ref_count);
    try std.testing.expectEqual(@as(u32, 0), caps.imports.get(6).?.loopback_ref_count);
}
