const std = @import("std");

pub fn deinitPendingCallMap(
    comptime MapType: type,
    comptime PendingCallType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
    deinit_pending_call: *const fn (std.mem.Allocator, *PendingCallType) void,
) void {
    // Pending call queues own both frame bytes and inbound-cap snapshots.
    var it = map.valueIterator();
    while (it.next()) |list| {
        for (list.items) |*pending| {
            deinit_pending_call(allocator, pending);
        }
        list.deinit(allocator);
    }
    map.deinit();
}

pub fn deinitPendingCallMapOwned(
    comptime MapType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
) void {
    var it = map.valueIterator();
    while (it.next()) |list| {
        for (list.items) |*pending| {
            pending.caps.deinit();
            allocator.free(pending.frame);
        }
        list.deinit(allocator);
    }
    map.deinit();
}

pub fn deinitValueMap(
    comptime MapType: type,
    comptime ValueType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
    deinit_value: *const fn (std.mem.Allocator, *ValueType) void,
) void {
    var it = map.valueIterator();
    while (it.next()) |value| {
        deinit_value(allocator, value);
    }
    map.deinit();
}

pub fn deinitOwnedStringKeyMap(comptime MapType: type, allocator: std.mem.Allocator, map: *MapType) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
    }
    map.deinit();
}

pub fn deinitOwnedStringKeyMapValues(
    comptime MapType: type,
    comptime ValueType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
    deinit_value: *const fn (std.mem.Allocator, *ValueType) void,
) void {
    // String-key maps in peer state own duplicated keys that must be freed explicitly.
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        deinit_value(allocator, entry.value_ptr);
    }
    map.deinit();
}

pub fn clearOptionalOwnedBytes(allocator: std.mem.Allocator, maybe_bytes: *?[]u8) void {
    if (maybe_bytes.*) |bytes| {
        allocator.free(bytes);
        maybe_bytes.* = null;
    }
}

pub fn deinitResolvedAnswerMap(
    comptime MapType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
) void {
    var it = map.valueIterator();
    while (it.next()) |value| {
        allocator.free(value.frame);
    }
    map.deinit();
}

pub fn deinitProvideEntryMap(
    comptime MapType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
) void {
    var it = map.valueIterator();
    while (it.next()) |entry| {
        allocator.free(entry.recipient_key);
        entry.target.deinit(allocator);
    }
    map.deinit();
}

pub fn deinitJoinStateMap(
    comptime MapType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
) void {
    var it = map.valueIterator();
    while (it.next()) |join_state| {
        join_state.deinit(allocator);
    }
    map.deinit();
}

pub fn deinitOwnedStringKeyListMap(
    comptime MapType: type,
    allocator: std.mem.Allocator,
    map: *MapType,
) void {
    var it = map.iterator();
    while (it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit(allocator);
    }
    map.deinit();
}

pub fn deinitOwnedBytesMap(comptime MapType: type, allocator: std.mem.Allocator, map: *MapType) void {
    var it = map.valueIterator();
    while (it.next()) |bytes| {
        allocator.free(bytes.*);
    }
    map.deinit();
}

pub fn deinitOptionalOwnedBytesMap(comptime MapType: type, allocator: std.mem.Allocator, map: *MapType) void {
    var it = map.valueIterator();
    while (it.next()) |maybe_bytes| {
        if (maybe_bytes.*) |bytes| allocator.free(bytes);
    }
    map.deinit();
}
