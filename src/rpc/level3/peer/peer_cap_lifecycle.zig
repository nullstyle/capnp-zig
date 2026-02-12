const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const protocol = @import("../../level0/protocol.zig");

pub fn releaseImport(
    comptime PeerType: type,
    peer: *PeerType,
    import_id: u32,
    count: u32,
    release_import_ref: *const fn (*PeerType, u32) bool,
    release_resolved_import: *const fn (*PeerType, u32) anyerror!void,
    send_release: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    if (count == 0) return;
    var remaining = count;
    var removed = false;
    while (remaining > 0) : (remaining -= 1) {
        if (release_import_ref(peer, import_id)) removed = true;
    }
    if (removed) {
        try release_resolved_import(peer, import_id);
    }
    try send_release(peer, import_id, count);
}

pub fn releaseImportRefForPeer(comptime PeerType: type, peer: *PeerType, import_id: u32) bool {
    return peer.caps.releaseImport(import_id);
}

pub fn releaseImportRefForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) bool {
    return struct {
        fn call(peer: *PeerType, import_id: u32) bool {
            return releaseImportRefForPeer(PeerType, peer, import_id);
        }
    }.call;
}

pub fn clearExportPromiseForPeer(comptime PeerType: type, peer: *PeerType, export_id: u32) void {
    peer.caps.clearExportPromise(export_id);
}

pub fn clearExportPromiseForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, export_id: u32) void {
            clearExportPromiseForPeer(PeerType, peer, export_id);
        }
    }.call;
}

pub fn noteExportRef(
    comptime ExportEntryType: type,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    id: u32,
) !void {
    var entry = exports.getEntry(id) orelse return error.UnknownExport;
    entry.value_ptr.ref_count = std.math.add(u32, entry.value_ptr.ref_count, 1) catch return error.RefCountOverflow;
}

pub fn releaseExport(
    comptime PeerType: type,
    comptime ExportEntryType: type,
    comptime PendingCallType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    pending_export_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    bootstrap_export_id: ?u32,
    id: u32,
    count: u32,
    clear_export_promise: *const fn (*PeerType, u32) void,
    deinit_pending_call: *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void,
) void {
    if (count == 0) return;
    var entry = exports.getEntry(id) orelse {
        log.warn("release for unknown export id={}", .{id});
        return;
    };

    if (bootstrap_export_id) |bootstrap_id| {
        if (bootstrap_id == id) {
            if (entry.value_ptr.ref_count <= count) {
                entry.value_ptr.ref_count = 0;
            } else {
                entry.value_ptr.ref_count -= count;
            }
            return;
        }
    }

    if (entry.value_ptr.ref_count <= count) {
        _ = exports.remove(id);
        clear_export_promise(peer, id);
        if (pending_export_promises.fetchRemove(id)) |removed| {
            var pending = removed.value;
            for (pending.items) |*pending_call| {
                deinit_pending_call(peer, pending_call, allocator);
            }
            pending.deinit(allocator);
        }
    } else {
        entry.value_ptr.ref_count -= count;
    }
}

pub fn storeResolvedImport(
    comptime PeerType: type,
    comptime ResolvedImportType: type,
    comptime ResolvedCapType: type,
    peer: *PeerType,
    resolved_imports: *std.AutoHashMap(u32, ResolvedImportType),
    pending_embargoes: *std.AutoHashMap(u32, u32),
    promise_id: u32,
    cap: ?ResolvedCapType,
    embargo_id: ?u32,
    embargoed: bool,
    release_resolved_cap: *const fn (*PeerType, ResolvedCapType) anyerror!void,
) !void {
    if (resolved_imports.fetchRemove(promise_id)) |existing| {
        if (existing.value.embargo_id) |id| {
            _ = pending_embargoes.remove(id);
        }
        if (existing.value.cap) |old_cap| {
            try release_resolved_cap(peer, old_cap);
        }
    }
    try resolved_imports.put(promise_id, .{
        .cap = cap,
        .embargo_id = embargo_id,
        .embargoed = embargoed,
    });
}

pub fn releaseResolvedImport(
    comptime PeerType: type,
    comptime ResolvedImportType: type,
    comptime ResolvedCapType: type,
    peer: *PeerType,
    resolved_imports: *std.AutoHashMap(u32, ResolvedImportType),
    pending_embargoes: *std.AutoHashMap(u32, u32),
    promise_id: u32,
    release_resolved_cap: *const fn (*PeerType, ResolvedCapType) anyerror!void,
) !void {
    if (resolved_imports.fetchRemove(promise_id)) |existing| {
        if (existing.value.embargo_id) |id| {
            _ = pending_embargoes.remove(id);
        }
        if (existing.value.cap) |resolved| {
            try release_resolved_cap(peer, resolved);
        }
    }
}

pub fn releaseResultCaps(
    comptime PeerType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    frame: []const u8,
    release_export: *const fn (*PeerType, u32, u32) void,
) !void {
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    if (decoded.tag != .@"return") return;
    const ret = try decoded.asReturn();
    if (ret.tag != .results or ret.results == null) return;
    const cap_list = ret.results.?.cap_table orelse return;
    var idx: u32 = 0;
    while (idx < cap_list.len()) : (idx += 1) {
        const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(idx));
        switch (desc.tag) {
            .senderHosted, .senderPromise => {
                if (desc.id) |id| {
                    release_export(peer, id, 1);
                }
            },
            else => {},
        }
    }
}

test "peer_cap_lifecycle releaseImport sends release and only releases resolved import when ref removed" {
    const State = struct {
        release_import_calls: usize = 0,
        release_resolved_calls: usize = 0,
        send_release_calls: usize = 0,
        last_send_import: u32 = 0,
        last_send_count: u32 = 0,
        remove_on_call: usize = 2,

        fn releaseImportRef(state: *@This(), import_id: u32) bool {
            _ = import_id;
            state.release_import_calls += 1;
            return state.release_import_calls == state.remove_on_call;
        }

        fn releaseResolved(state: *@This(), promise_id: u32) !void {
            _ = promise_id;
            state.release_resolved_calls += 1;
        }

        fn sendRelease(state: *@This(), import_id: u32, count: u32) !void {
            state.send_release_calls += 1;
            state.last_send_import = import_id;
            state.last_send_count = count;
        }
    };

    var state = State{};
    try releaseImport(
        State,
        &state,
        55,
        3,
        State.releaseImportRef,
        State.releaseResolved,
        State.sendRelease,
    );
    try std.testing.expectEqual(@as(usize, 3), state.release_import_calls);
    try std.testing.expectEqual(@as(usize, 1), state.release_resolved_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_release_calls);
    try std.testing.expectEqual(@as(u32, 55), state.last_send_import);
    try std.testing.expectEqual(@as(u32, 3), state.last_send_count);
}

test "peer_cap_lifecycle releaseImportRefForPeerFn delegates to peer cap table" {
    const Caps = struct {
        calls: usize = 0,
        last_import_id: u32 = 0,
        removed: bool = false,

        fn releaseImport(self: *@This(), import_id: u32) bool {
            self.calls += 1;
            self.last_import_id = import_id;
            return self.removed;
        }
    };
    const Peer = struct {
        caps: Caps,
    };

    var peer = Peer{
        .caps = .{ .removed = true },
    };
    const release_import = releaseImportRefForPeerFn(Peer);
    try std.testing.expect(release_import(&peer, 42));
    try std.testing.expectEqual(@as(usize, 1), peer.caps.calls);
    try std.testing.expectEqual(@as(u32, 42), peer.caps.last_import_id);
}

test "peer_cap_lifecycle clearExportPromiseForPeerFn delegates to peer cap table" {
    const Caps = struct {
        calls: usize = 0,
        last_export_id: u32 = 0,

        fn clearExportPromise(self: *@This(), export_id: u32) void {
            self.calls += 1;
            self.last_export_id = export_id;
        }
    };
    const Peer = struct {
        caps: Caps,
    };

    var peer = Peer{ .caps = .{} };
    const clear_export = clearExportPromiseForPeerFn(Peer);
    clear_export(&peer, 71);
    try std.testing.expectEqual(@as(usize, 1), peer.caps.calls);
    try std.testing.expectEqual(@as(u32, 71), peer.caps.last_export_id);
}

test "peer_cap_lifecycle releaseResultCaps releases sender-hosted and sender-promise ids" {
    const State = struct {
        releases: std.ArrayList(u32),

        fn init() @This() {
            return .{ .releases = std.ArrayList(u32){} };
        }

        fn deinit(state: *@This(), allocator: std.mem.Allocator) void {
            state.releases.deinit(allocator);
        }
    };
    const Hooks = struct {
        fn releaseExport(state: *State, id: u32, count: u32) void {
            _ = count;
            state.releases.append(std.testing.allocator, id) catch unreachable;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(9, .results);
    var cap_list = try ret.initCapTableTyped(3);

    protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), 10);
    protocol.CapDescriptor.writeNone(try cap_list.get(1));
    protocol.CapDescriptor.writeSenderPromise(try cap_list.get(2), 11);
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var state = State.init();
    defer state.deinit(std.testing.allocator);
    try releaseResultCaps(State, &state, std.testing.allocator, frame, Hooks.releaseExport);
    try std.testing.expectEqual(@as(usize, 2), state.releases.items.len);
    try std.testing.expectEqual(@as(u32, 10), state.releases.items[0]);
    try std.testing.expectEqual(@as(u32, 11), state.releases.items[1]);
}
