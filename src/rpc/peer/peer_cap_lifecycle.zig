const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const protocol = @import("../wire/protocol.zig");

pub fn handleRelease(
    comptime PeerType: type,
    peer: *PeerType,
    release: protocol.Release,
    on_release_export: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    try on_release_export(peer, release.id, release.reference_count);
}

pub fn releaseImport(
    comptime PeerType: type,
    peer: *PeerType,
    import_id: u32,
    count: u32,
    get_import_ref_count: *const fn (*PeerType, u32) u32,
    release_import_ref: *const fn (*PeerType, u32) bool,
    release_resolved_import: *const fn (*PeerType, u32) anyerror!void,
    send_release: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    if (count == 0) return;
    var remaining = count;
    var released: u32 = 0;
    var removed = false;
    while (remaining > 0) : (remaining -= 1) {
        if (get_import_ref_count(peer, import_id) == 0) break;
        released += 1;
        if (release_import_ref(peer, import_id)) removed = true;
    }
    if (released == 0) return;
    if (removed) {
        try release_resolved_import(peer, import_id);
    }
    try send_release(peer, import_id, released);
}

pub fn importRefCountForPeer(comptime PeerType: type, peer: *PeerType, import_id: u32) u32 {
    const import_entry = peer.caps.imports.get(import_id) orelse return 0;
    return import_entry.ref_count;
}

pub fn importRefCountForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) u32 {
    return struct {
        fn call(peer: *PeerType, import_id: u32) u32 {
            return importRefCountForPeer(PeerType, peer, import_id);
        }
    }.call;
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

pub fn clearExportForPeer(comptime PeerType: type, peer: *PeerType, export_id: u32) void {
    peer.caps.clearExport(export_id);
}

pub fn clearExportForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, export_id: u32) void {
            clearExportForPeer(PeerType, peer, export_id);
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

/// Take one answer-held reference on an export: the resolved answer that
/// carries the export in its results owns this reference until its Finish.
/// Tracked separately from the wire `ref_count` so an inbound Release can
/// never free a reference the answer still needs for pipelined dispatch.
pub fn noteAnswerExportRef(
    comptime ExportEntryType: type,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    id: u32,
) !void {
    var entry = exports.getEntry(id) orelse return error.UnknownExport;
    entry.value_ptr.answer_ref_count = std.math.add(u32, entry.value_ptr.answer_ref_count, 1) catch return error.RefCountOverflow;
}

/// Take one promise-held reference on an export: a promise export that has
/// resolved to this export as its target owns this reference for its own
/// lifetime, so an inbound Release that zeroes the target's wire `ref_count`
/// cannot destroy it while the promise export still routes calls at it.
/// Tracked separately from both the wire `ref_count` and `answer_ref_count`.
pub fn notePromiseExportRef(
    comptime ExportEntryType: type,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    id: u32,
) !void {
    var entry = exports.getEntry(id) orelse return error.UnknownExport;
    entry.value_ptr.promise_ref_count = std.math.add(u32, entry.value_ptr.promise_ref_count, 1) catch return error.RefCountOverflow;
}

fn destroyExportEntry(
    comptime PeerType: type,
    comptime ExportEntryType: type,
    comptime PendingCallType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    pending_export_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    id: u32,
    clear_export: *const fn (*PeerType, u32) void,
    deinit_pending_call: *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void,
) void {
    const removed = exports.fetchRemove(id) orelse return;
    clear_export(peer, id);
    if (pending_export_promises.fetchRemove(id)) |pending_removed| {
        var pending = pending_removed.value;
        for (pending.items) |*pending_call| {
            deinit_pending_call(peer, pending_call, allocator);
        }
        pending.deinit(allocator);
    }
    if (@hasField(ExportEntryType, "deinit_ctx")) {
        if (removed.value.deinit_ctx) |deinit_ctx| {
            if (removed.value.handler) |handler| deinit_ctx(allocator, handler.ctx);
        }
    }
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
    clear_export: *const fn (*PeerType, u32) void,
    deinit_pending_call: *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void,
) !void {
    if (count == 0) return;
    var entry = exports.getEntry(id) orelse {
        log.warn("release for unknown export id={}", .{id});
        return;
    };

    // NOTE: Bootstrap export entry persists for the connection lifetime by design.
    // The bootstrap handler (Export.ctx) must outlive the Peer.
    if (bootstrap_export_id) |bootstrap_id| {
        if (bootstrap_id == id) {
            if (count > entry.value_ptr.ref_count) return error.ReleaseCountExceeded;
            entry.value_ptr.ref_count -= count;
            return;
        }
    }

    if (count > entry.value_ptr.ref_count) return error.ReleaseCountExceeded;

    entry.value_ptr.ref_count -= count;
    // Destruction requires ALL ref classes at zero: a Release only spends wire
    // references, so an export pinned by a still-unfinished resolved answer
    // (answer_ref_count > 0) or by a live promise export that resolved to it
    // (promise_ref_count > 0) survives for dispatch until that holder releases.
    if (entry.value_ptr.ref_count == 0 and
        entry.value_ptr.answer_ref_count == 0 and
        entry.value_ptr.promise_ref_count == 0)
    {
        destroyExportEntry(
            PeerType,
            ExportEntryType,
            PendingCallType,
            peer,
            allocator,
            exports,
            pending_export_promises,
            id,
            clear_export,
            deinit_pending_call,
        );
    }
}

/// Release one answer-held reference on an export, destroying the entry when
/// no wire or answer references remain. Invoked (via the recorded Return
/// frame's cap table) when the answer's Finish arrives.
pub fn releaseAnswerHeldExport(
    comptime PeerType: type,
    comptime ExportEntryType: type,
    comptime PendingCallType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    pending_export_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    bootstrap_export_id: ?u32,
    id: u32,
    clear_export: *const fn (*PeerType, u32) void,
    deinit_pending_call: *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void,
) !void {
    var entry = exports.getEntry(id) orelse {
        log.warn("answer-held release for unknown export id={}", .{id});
        return;
    };
    if (entry.value_ptr.answer_ref_count == 0) {
        log.warn("answer-held release underflow for export id={}", .{id});
        return;
    }
    entry.value_ptr.answer_ref_count -= 1;

    // Bootstrap export entry persists for the connection lifetime (see
    // releaseExport); only its counters move.
    if (bootstrap_export_id) |bootstrap_id| {
        if (bootstrap_id == id) return;
    }

    if (entry.value_ptr.ref_count == 0 and
        entry.value_ptr.answer_ref_count == 0 and
        entry.value_ptr.promise_ref_count == 0)
    {
        destroyExportEntry(
            PeerType,
            ExportEntryType,
            PendingCallType,
            peer,
            allocator,
            exports,
            pending_export_promises,
            id,
            clear_export,
            deinit_pending_call,
        );
    }
}

/// Release one promise-held reference on an export, destroying the entry when
/// no wire, answer, or promise references remain. Invoked when the promise
/// export that took the reference (via resolvePromiseExportToExport) is itself
/// destroyed — releasing its pin on the resolution target it routed calls to.
pub fn releasePromiseHeldExport(
    comptime PeerType: type,
    comptime ExportEntryType: type,
    comptime PendingCallType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    exports: *std.AutoHashMap(u32, ExportEntryType),
    pending_export_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    bootstrap_export_id: ?u32,
    id: u32,
    clear_export: *const fn (*PeerType, u32) void,
    deinit_pending_call: *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void,
) !void {
    var entry = exports.getEntry(id) orelse {
        log.warn("promise-held release for unknown export id={}", .{id});
        return;
    };
    if (entry.value_ptr.promise_ref_count == 0) {
        log.warn("promise-held release underflow for export id={}", .{id});
        return;
    }
    entry.value_ptr.promise_ref_count -= 1;

    // Bootstrap export entry persists for the connection lifetime (see
    // releaseExport); only its counters move.
    if (bootstrap_export_id) |bootstrap_id| {
        if (bootstrap_id == id) return;
    }

    if (entry.value_ptr.ref_count == 0 and
        entry.value_ptr.answer_ref_count == 0 and
        entry.value_ptr.promise_ref_count == 0)
    {
        destroyExportEntry(
            PeerType,
            ExportEntryType,
            PendingCallType,
            peer,
            allocator,
            exports,
            pending_export_promises,
            id,
            clear_export,
            deinit_pending_call,
        );
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
    _ = peer;
    _ = pending_embargoes;
    _ = release_resolved_cap;

    if (resolved_imports.contains(promise_id)) return error.DuplicateResolve;
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

/// Take one answer-held reference on every senderHosted/senderPromise export
/// named by a results Return frame's cap table. Runs when the resolved answer
/// is recorded (before its frame is sent), so the answer keeps its pipeline
/// targets alive until Finish even if the remote Releases the caps it
/// imported from this Return right away — a legal interleaving.
///
/// Returns the noted export ids (caller owns the slice) so a caller that
/// fails between reservation and commit can roll the references back without
/// re-decoding the frame. On error every reference already taken is rolled
/// back before returning.
pub fn noteAnswerHeldResultCaps(
    comptime PeerType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    frame: []const u8,
    note_answer_export_ref: *const fn (*PeerType, u32) anyerror!void,
    rollback_answer_export_ref: *const fn (*PeerType, u32) void,
) ![]u32 {
    var noted = std.ArrayList(u32).empty;
    errdefer {
        var undo = noted.items.len;
        while (undo > 0) {
            undo -= 1;
            rollback_answer_export_ref(peer, noted.items[undo]);
        }
        noted.deinit(allocator);
    }

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    walk: {
        if (decoded.tag != .@"return") break :walk;
        const ret = try decoded.asReturn();
        if (ret.tag != .results or ret.results == null) break :walk;
        const cap_list = ret.results.?.cap_table orelse break :walk;
        var idx: u32 = 0;
        while (idx < cap_list.len()) : (idx += 1) {
            const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(idx));
            switch (desc.tag) {
                .senderHosted, .senderPromise => {
                    if (desc.id) |id| {
                        try noted.ensureUnusedCapacity(allocator, 1);
                        try note_answer_export_ref(peer, id);
                        noted.appendAssumeCapacity(id);
                    }
                },
                else => {},
            }
        }
    }
    return noted.toOwnedSlice(allocator);
}

pub fn releaseResultCaps(
    comptime PeerType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    frame: []const u8,
    release_export: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    if (decoded.tag != .@"return") return;
    const ret = try decoded.asReturn();
    if (ret.tag != .results) return;
    const results = ret.results orelse return;
    const cap_list = results.cap_table orelse return;
    var idx: u32 = 0;
    while (idx < cap_list.len()) : (idx += 1) {
        const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(idx));
        switch (desc.tag) {
            .senderHosted, .senderPromise => {
                if (desc.id) |id| {
                    try release_export(peer, id, 1);
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
        ref_count: u32 = 3,

        fn getRefCount(state: *@This(), import_id: u32) u32 {
            _ = import_id;
            return state.ref_count;
        }

        fn releaseImportRef(state: *@This(), import_id: u32) bool {
            _ = import_id;
            state.release_import_calls += 1;
            if (state.ref_count == 0) return false;
            state.ref_count -= 1;
            return state.ref_count == 0;
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
        State.getRefCount,
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

test "peer_cap_lifecycle releaseImport only sends decremented ref count" {
    const State = struct {
        release_import_calls: usize = 0,
        send_release_calls: usize = 0,
        last_send_count: u32 = 0,
        ref_count: u32 = 2,

        fn getRefCount(state: *@This(), import_id: u32) u32 {
            _ = import_id;
            return state.ref_count;
        }

        fn releaseImportRef(state: *@This(), import_id: u32) bool {
            _ = import_id;
            state.release_import_calls += 1;
            if (state.ref_count == 0) return false;
            state.ref_count -= 1;
            return state.ref_count == 0;
        }

        fn releaseResolved(_: *@This(), _: u32) !void {}

        fn sendRelease(state: *@This(), _: u32, count: u32) !void {
            state.send_release_calls += 1;
            state.last_send_count = count;
        }
    };

    var state = State{};
    try releaseImport(
        State,
        &state,
        42,
        5,
        State.getRefCount,
        State.releaseImportRef,
        State.releaseResolved,
        State.sendRelease,
    );
    try std.testing.expectEqual(@as(usize, 2), state.release_import_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_release_calls);
    try std.testing.expectEqual(@as(u32, 2), state.last_send_count);
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

test "peer_cap_lifecycle clearExportForPeerFn delegates to peer cap table" {
    const Caps = struct {
        calls: usize = 0,
        last_export_id: u32 = 0,

        fn clearExport(self: *@This(), export_id: u32) void {
            self.calls += 1;
            self.last_export_id = export_id;
        }
    };
    const Peer = struct {
        caps: Caps,
    };

    var peer = Peer{ .caps = .{} };
    const clear_export = clearExportForPeerFn(Peer);
    clear_export(&peer, 71);
    try std.testing.expectEqual(@as(usize, 1), peer.caps.calls);
    try std.testing.expectEqual(@as(u32, 71), peer.caps.last_export_id);
}

test "peer_cap_lifecycle storeResolvedImport rejects duplicate before mutation" {
    const ResolvedCap = union(enum) {
        none,
        imported: struct { id: u32 },
    };
    const ResolvedImport = struct {
        cap: ?ResolvedCap = null,
        embargo_id: ?u32 = null,
        embargoed: bool = false,
    };
    const State = struct {
        release_calls: usize = 0,

        fn releaseResolved(state: *@This(), resolved: ResolvedCap) !void {
            _ = resolved;
            state.release_calls += 1;
        }
    };

    var resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(std.testing.allocator);
    defer resolved_imports.deinit();
    var pending_embargoes = std.AutoHashMap(u32, u32).init(std.testing.allocator);
    defer pending_embargoes.deinit();

    try resolved_imports.put(7, .{
        .cap = .{ .imported = .{ .id = 1 } },
        .embargo_id = 55,
        .embargoed = true,
    });
    try pending_embargoes.put(55, 7);

    var state = State{};
    try std.testing.expectError(error.DuplicateResolve, storeResolvedImport(
        State,
        ResolvedImport,
        ResolvedCap,
        &state,
        &resolved_imports,
        &pending_embargoes,
        7,
        .{ .imported = .{ .id = 2 } },
        null,
        false,
        State.releaseResolved,
    ));

    const existing = resolved_imports.get(7) orelse return error.MissingResolvedImport;
    try std.testing.expectEqual(@as(u32, 1), existing.cap.?.imported.id);
    try std.testing.expectEqual(@as(?u32, 55), existing.embargo_id);
    try std.testing.expect(existing.embargoed);
    try std.testing.expectEqual(@as(?u32, 7), pending_embargoes.get(55));
    try std.testing.expectEqual(@as(usize, 0), state.release_calls);
}

test "peer_cap_lifecycle releaseResultCaps releases sender-hosted and sender-promise ids" {
    const State = struct {
        releases: std.ArrayList(u32),

        fn init() @This() {
            return .{ .releases = std.ArrayList(u32).empty };
        }

        fn deinit(state: *@This(), allocator: std.mem.Allocator) void {
            state.releases.deinit(allocator);
        }
    };
    const Hooks = struct {
        fn releaseExport(state: *State, id: u32, count: u32) !void {
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

const TestExportEntry = struct {
    ref_count: u32,
    answer_ref_count: u32 = 0,
    promise_ref_count: u32 = 0,
};

const TestPendingCall = struct {
    frame: []u8,
};

const TestReleasePeer = struct {
    clear_export_calls: usize = 0,
    last_cleared_export: ?u32 = null,
    deinit_pending_calls: usize = 0,

    fn clearExport(peer: *@This(), export_id: u32) void {
        peer.clear_export_calls += 1;
        peer.last_cleared_export = export_id;
    }

    fn deinitPendingCall(peer: *@This(), pending_call: *TestPendingCall, allocator: std.mem.Allocator) void {
        peer.deinit_pending_calls += 1;
        allocator.free(pending_call.frame);
    }
};

test "peer_cap_lifecycle releaseExport keeps an export pinned by answer refs" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    // One wire ref (from the Return descriptor) and one answer-held ref
    // (from the recorded resolved answer).
    try exports.put(4, .{ .ref_count = 1, .answer_ref_count = 1 });

    var peer = TestReleasePeer{};
    // The remote legally Releases its wire ref before Finish. The export must
    // survive: the unfinished answer still needs it for pipelined dispatch.
    try releaseExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        4,
        1,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const pinned = exports.get(4) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 0), pinned.ref_count);
    try std.testing.expectEqual(@as(u32, 1), pinned.answer_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);

    // Finish releases the answer-held ref: now both counts are zero and the
    // export is destroyed.
    try releaseAnswerHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        4,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expect(!exports.contains(4));
    try std.testing.expectEqual(@as(usize, 1), peer.clear_export_calls);
    try std.testing.expectEqual(@as(?u32, 4), peer.last_cleared_export);
}

test "peer_cap_lifecycle releaseAnswerHeldExport keeps an export with live wire refs" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    try exports.put(9, .{ .ref_count = 1, .answer_ref_count = 1 });

    var peer = TestReleasePeer{};
    // Finish first (remote keeps its import): only the answer ref dies.
    try releaseAnswerHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        9,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const kept = exports.get(9) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 1), kept.ref_count);
    try std.testing.expectEqual(@as(u32, 0), kept.answer_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);

    // Unknown export and underflow are tolerated (warn only), matching
    // releaseExport's tolerance for unknown ids.
    try releaseAnswerHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        9,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expect(exports.contains(9));
    try releaseAnswerHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        1000,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);
}

test "peer_cap_lifecycle releaseAnswerHeldExport never destroys the bootstrap export" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    try exports.put(0, .{ .ref_count = 0, .answer_ref_count = 1 });

    var peer = TestReleasePeer{};
    try releaseAnswerHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        0,
        0,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const entry = exports.get(0) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 0), entry.answer_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);
}

test "peer_cap_lifecycle releaseExport keeps an export pinned by promise refs" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    // One wire ref (from the Resolve descriptor handed to the remote) and one
    // promise-held ref (the resolved promise pinning this target).
    try exports.put(7, .{ .ref_count = 1, .promise_ref_count = 1 });

    var peer = TestReleasePeer{};
    // The remote Releases its wire ref to the resolved cap. The export must
    // survive: the live promise export still routes calls at it.
    try releaseExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        7,
        1,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const pinned = exports.get(7) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 0), pinned.ref_count);
    try std.testing.expectEqual(@as(u32, 1), pinned.promise_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);

    // Destroying the promise export releases the promise-held ref: now all
    // three counts are zero and the target is destroyed.
    try releasePromiseHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        7,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expect(!exports.contains(7));
    try std.testing.expectEqual(@as(usize, 1), peer.clear_export_calls);
    try std.testing.expectEqual(@as(?u32, 7), peer.last_cleared_export);
}

test "peer_cap_lifecycle releasePromiseHeldExport tolerates unknown and underflow" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    // A wire ref outlives the promise-held ref: the export survives the
    // promise release, and a second (underflowing) release is a warn-only
    // no-op that never touches the surviving wire ref.
    try exports.put(8, .{ .ref_count = 1, .promise_ref_count = 1 });

    var peer = TestReleasePeer{};
    try releasePromiseHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        8,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const kept = exports.get(8) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 1), kept.ref_count);
    try std.testing.expectEqual(@as(u32, 0), kept.promise_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);

    // Underflow (promise_ref_count already 0) and unknown id are tolerated.
    try releasePromiseHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        8,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expect(exports.contains(8));
    try releasePromiseHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        null,
        1000,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);
}

test "peer_cap_lifecycle releasePromiseHeldExport never destroys the bootstrap export" {
    var exports = std.AutoHashMap(u32, TestExportEntry).init(std.testing.allocator);
    defer exports.deinit();
    var pending = std.AutoHashMap(u32, std.ArrayList(TestPendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    try exports.put(0, .{ .ref_count = 0, .promise_ref_count = 1 });

    var peer = TestReleasePeer{};
    try releasePromiseHeldExport(
        TestReleasePeer,
        TestExportEntry,
        TestPendingCall,
        &peer,
        std.testing.allocator,
        &exports,
        &pending,
        0,
        0,
        TestReleasePeer.clearExport,
        TestReleasePeer.deinitPendingCall,
    );
    const entry = exports.get(0) orelse return error.MissingExport;
    try std.testing.expectEqual(@as(u32, 0), entry.promise_ref_count);
    try std.testing.expectEqual(@as(usize, 0), peer.clear_export_calls);
}

test "peer_cap_lifecycle noteAnswerHeldResultCaps notes exports and rolls back on failure" {
    const State = struct {
        noted: std.ArrayList(u32),
        rolled_back: std.ArrayList(u32),
        fail_on_id: ?u32 = null,

        fn noteRef(state: *@This(), id: u32) !void {
            if (state.fail_on_id) |fail_id| {
                if (id == fail_id) return error.UnknownExport;
            }
            state.noted.append(std.testing.allocator, id) catch unreachable;
        }

        fn rollbackRef(state: *@This(), id: u32) void {
            state.rolled_back.append(std.testing.allocator, id) catch unreachable;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    var ret = try builder.beginReturn(3, .results);
    var cap_list = try ret.initCapTableTyped(3);
    protocol.CapDescriptor.writeSenderHosted(try cap_list.get(0), 21);
    protocol.CapDescriptor.writeNone(try cap_list.get(1));
    protocol.CapDescriptor.writeSenderPromise(try cap_list.get(2), 22);
    const frame = try builder.finish();
    defer std.testing.allocator.free(frame);

    var state = State{
        .noted = std.ArrayList(u32).empty,
        .rolled_back = std.ArrayList(u32).empty,
    };
    defer state.noted.deinit(std.testing.allocator);
    defer state.rolled_back.deinit(std.testing.allocator);

    const held = try noteAnswerHeldResultCaps(
        State,
        &state,
        std.testing.allocator,
        frame,
        State.noteRef,
        State.rollbackRef,
    );
    defer std.testing.allocator.free(held);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 21, 22 }, held);
    try std.testing.expectEqualSlices(u32, &[_]u32{ 21, 22 }, state.noted.items);
    try std.testing.expectEqual(@as(usize, 0), state.rolled_back.items.len);

    // A mid-walk failure rolls back every reference already taken.
    state.noted.clearRetainingCapacity();
    state.fail_on_id = 22;
    try std.testing.expectError(error.UnknownExport, noteAnswerHeldResultCaps(
        State,
        &state,
        std.testing.allocator,
        frame,
        State.noteRef,
        State.rollbackRef,
    ));
    try std.testing.expectEqualSlices(u32, &[_]u32{21}, state.noted.items);
    try std.testing.expectEqualSlices(u32, &[_]u32{21}, state.rolled_back.items);
}
