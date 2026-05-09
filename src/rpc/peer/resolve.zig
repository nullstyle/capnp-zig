const std = @import("std");
const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");

/// Bundles the callback parameters of handleResolve into a single operations struct.
pub fn ResolveOps(comptime PeerType: type) type {
    return struct {
        has_known_promise: *const fn (*PeerType, u32) bool,
        resolve_cap_descriptor: *const fn (*PeerType, protocol.CapDescriptor) anyerror!cap_table.ResolvedCap,
        release_resolved_cap: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!void,
        alloc_embargo_id: *const fn (*PeerType) anyerror!u32,
        remember_pending_embargo: *const fn (*PeerType, u32, u32) anyerror!void,
        forget_pending_embargo: *const fn (*PeerType, u32) void,
        send_disembargo_sender_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
        store_resolved_import: *const fn (*PeerType, u32, ?cap_table.ResolvedCap, ?u32, bool) anyerror!void,
    };
}

pub fn handleResolve(
    comptime PeerType: type,
    peer: *PeerType,
    resolve: protocol.Resolve,
    has_known_promise: *const fn (*PeerType, u32) bool,
    resolve_cap_descriptor: *const fn (*PeerType, protocol.CapDescriptor) anyerror!cap_table.ResolvedCap,
    release_resolved_cap: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!void,
    alloc_embargo_id: *const fn (*PeerType) anyerror!u32,
    remember_pending_embargo: *const fn (*PeerType, u32, u32) anyerror!void,
    forget_pending_embargo: *const fn (*PeerType, u32) void,
    send_disembargo_sender_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
    store_resolved_import: *const fn (*PeerType, u32, ?cap_table.ResolvedCap, ?u32, bool) anyerror!void,
) !void {
    const ops = ResolveOps(PeerType){
        .has_known_promise = has_known_promise,
        .resolve_cap_descriptor = resolve_cap_descriptor,
        .release_resolved_cap = release_resolved_cap,
        .alloc_embargo_id = alloc_embargo_id,
        .remember_pending_embargo = remember_pending_embargo,
        .forget_pending_embargo = forget_pending_embargo,
        .send_disembargo_sender_loopback = send_disembargo_sender_loopback,
        .store_resolved_import = store_resolved_import,
    };
    try handleResolveWithOps(PeerType, peer, resolve, ops);
}

/// handleResolve variant that accepts a bundled ResolveOps instead of individual callbacks.
pub fn handleResolveWithOps(
    comptime PeerType: type,
    peer: *PeerType,
    resolve: protocol.Resolve,
    ops: ResolveOps(PeerType),
) !void {
    const promise_id = resolve.promise_id;
    const known_promise = ops.has_known_promise(peer, promise_id);

    switch (resolve.tag) {
        .cap => {
            const descriptor = resolve.cap orelse return error.MissingResolveCap;
            const resolved = try ops.resolve_cap_descriptor(peer, descriptor);
            var resolved_owned = true;
            errdefer if (resolved_owned) ops.release_resolved_cap(peer, resolved) catch {};

            if (!known_promise) {
                // Late resolve for an unknown promise id: release any received capability
                // references immediately because no local table entry can own them.
                resolved_owned = false;
                try ops.release_resolved_cap(peer, resolved);
                return;
            }

            var embargo_id: ?u32 = null;
            var embargoed = false;
            var pending_embargo_id: ?u32 = null;
            errdefer if (pending_embargo_id) |id| ops.forget_pending_embargo(peer, id);
            if (resolved == .exported or resolved == .promised) {
                // Exported/promise resolutions require a sender-loopback disembargo handshake
                // before the resolved import can be considered callable locally.
                const new_embargo_id = try ops.alloc_embargo_id(peer);
                embargo_id = new_embargo_id;
                embargoed = true;
                try ops.remember_pending_embargo(peer, new_embargo_id, promise_id);
                pending_embargo_id = new_embargo_id;
                const target = switch (resolved) {
                    .promised => |promised| protocol.MessageTarget{
                        .tag = .promisedAnswer,
                        .imported_cap = null,
                        .promised_answer = promised,
                    },
                    else => protocol.MessageTarget{
                        .tag = .importedCap,
                        .imported_cap = promise_id,
                        .promised_answer = null,
                    },
                };
                try ops.send_disembargo_sender_loopback(peer, target, new_embargo_id);
            }

            try ops.store_resolved_import(peer, promise_id, resolved, embargo_id, embargoed);
            resolved_owned = false;
        },
        .exception => {
            if (!known_promise) return;
            try ops.store_resolved_import(peer, promise_id, null, null, false);
        },
    }
}

pub fn hasKnownResolvePromiseForPeer(comptime PeerType: type, peer: *PeerType, promise_id: u32) bool {
    return peer.caps.imports.contains(promise_id);
}

pub fn hasKnownResolvePromiseForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) bool {
    return struct {
        fn call(peer: *PeerType, promise_id: u32) bool {
            return hasKnownResolvePromiseForPeer(PeerType, peer, promise_id);
        }
    }.call;
}

pub fn resolveCapDescriptorForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    descriptor: protocol.CapDescriptor,
) !cap_table.ResolvedCap {
    return cap_table.resolveCapDescriptor(&peer.caps, descriptor);
}

pub fn resolveCapDescriptorForPeerFn(
    comptime PeerType: type,
) *const fn (*PeerType, protocol.CapDescriptor) anyerror!cap_table.ResolvedCap {
    return struct {
        fn call(peer: *PeerType, descriptor: protocol.CapDescriptor) anyerror!cap_table.ResolvedCap {
            return resolveCapDescriptorForPeer(PeerType, peer, descriptor);
        }
    }.call;
}

pub fn allocateEmbargoIdForPeer(comptime PeerType: type, peer: *PeerType) error{EmbargoIdExhausted}!u32 {
    const start_id = peer.next_embargo_id;
    while (true) {
        const embargo_id = peer.next_embargo_id;
        peer.next_embargo_id +%= 1;
        if (!peer.pending_embargoes.contains(embargo_id)) {
            return embargo_id;
        }
        if (peer.next_embargo_id == start_id) return error.EmbargoIdExhausted;
    }
}

pub fn allocateEmbargoIdForPeerFn(comptime PeerType: type) *const fn (*PeerType) anyerror!u32 {
    return struct {
        fn call(peer: *PeerType) anyerror!u32 {
            return allocateEmbargoIdForPeer(PeerType, peer);
        }
    }.call;
}

pub fn rememberPendingEmbargoForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    embargo_id: u32,
    promise_id: u32,
) !void {
    try peer.pending_embargoes.put(embargo_id, promise_id);
}

pub fn rememberPendingEmbargoForPeerFn(
    comptime PeerType: type,
) *const fn (*PeerType, u32, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, embargo_id: u32, promise_id: u32) anyerror!void {
            return rememberPendingEmbargoForPeer(PeerType, peer, embargo_id, promise_id);
        }
    }.call;
}

pub fn forgetPendingEmbargoForPeer(comptime PeerType: type, peer: *PeerType, embargo_id: u32) void {
    _ = peer.pending_embargoes.remove(embargo_id);
}

pub fn forgetPendingEmbargoForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, embargo_id: u32) void {
            forgetPendingEmbargoForPeer(PeerType, peer, embargo_id);
        }
    }.call;
}

test "peer_resolve rolls back pending embargo on sender-loopback failure" {
    const State = struct {
        pending_embargoes: std.AutoHashMap(u32, u32),
        release_calls: usize = 0,
        send_calls: usize = 0,
        store_calls: usize = 0,
        next_embargo_id: u32 = 77,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.pending_embargoes.deinit();
        }

        fn hasKnown(_: *@This(), promise_id: u32) bool {
            return promise_id == 41;
        }

        fn resolve(_: *@This(), descriptor: protocol.CapDescriptor) !cap_table.ResolvedCap {
            try std.testing.expectEqual(protocol.CapDescriptorTag.receiverHosted, descriptor.tag);
            return .{ .exported = .{ .id = descriptor.id orelse return error.MissingCapDescriptorId } };
        }

        fn release(state: *@This(), resolved: cap_table.ResolvedCap) !void {
            try std.testing.expect(resolved == .exported);
            state.release_calls += 1;
        }

        fn allocEmbargo(state: *@This()) !u32 {
            const id = state.next_embargo_id;
            state.next_embargo_id += 1;
            return id;
        }

        fn remember(state: *@This(), embargo_id: u32, promise_id: u32) !void {
            try state.pending_embargoes.put(embargo_id, promise_id);
        }

        fn forget(state: *@This(), embargo_id: u32) void {
            _ = state.pending_embargoes.remove(embargo_id);
        }

        fn send(state: *@This(), target: protocol.MessageTarget, embargo_id: u32) !void {
            try std.testing.expectEqual(@as(u32, 77), embargo_id);
            try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, target.tag);
            try std.testing.expectEqual(@as(?u32, 41), target.imported_cap);
            state.send_calls += 1;
            return error.TestExpectedError;
        }

        fn store(state: *@This(), promise_id: u32, cap: ?cap_table.ResolvedCap, embargo_id: ?u32, embargoed: bool) !void {
            _ = promise_id;
            _ = cap;
            _ = embargo_id;
            _ = embargoed;
            state.store_calls += 1;
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try std.testing.expectError(error.TestExpectedError, handleResolveWithOps(State, &state, .{
        .promise_id = 41,
        .tag = .cap,
        .cap = .{ .tag = .receiverHosted, .id = 5 },
        .exception = null,
    }, .{
        .has_known_promise = State.hasKnown,
        .resolve_cap_descriptor = State.resolve,
        .release_resolved_cap = State.release,
        .alloc_embargo_id = State.allocEmbargo,
        .remember_pending_embargo = State.remember,
        .forget_pending_embargo = State.forget,
        .send_disembargo_sender_loopback = State.send,
        .store_resolved_import = State.store,
    }));

    try std.testing.expectEqual(@as(usize, 0), state.pending_embargoes.count());
    try std.testing.expectEqual(@as(usize, 1), state.release_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqual(@as(usize, 0), state.store_calls);
}

test "peer_resolve rolls back pending embargo on store failure" {
    const State = struct {
        pending_embargoes: std.AutoHashMap(u32, u32),
        release_calls: usize = 0,
        send_calls: usize = 0,
        store_calls: usize = 0,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.pending_embargoes.deinit();
        }

        fn hasKnown(_: *@This(), _: u32) bool {
            return true;
        }

        fn resolve(_: *@This(), _: protocol.CapDescriptor) !cap_table.ResolvedCap {
            return .{ .exported = .{ .id = 9 } };
        }

        fn release(state: *@This(), resolved: cap_table.ResolvedCap) !void {
            try std.testing.expect(resolved == .exported);
            state.release_calls += 1;
        }

        fn allocEmbargo(_: *@This()) !u32 {
            return 12;
        }

        fn remember(state: *@This(), embargo_id: u32, promise_id: u32) !void {
            try state.pending_embargoes.put(embargo_id, promise_id);
        }

        fn forget(state: *@This(), embargo_id: u32) void {
            _ = state.pending_embargoes.remove(embargo_id);
        }

        fn send(state: *@This(), _: protocol.MessageTarget, embargo_id: u32) !void {
            try std.testing.expectEqual(@as(u32, 12), embargo_id);
            state.send_calls += 1;
        }

        fn store(state: *@This(), promise_id: u32, cap: ?cap_table.ResolvedCap, embargo_id: ?u32, embargoed: bool) !void {
            try std.testing.expectEqual(@as(u32, 41), promise_id);
            try std.testing.expect(cap.?.exported.id == 9);
            try std.testing.expectEqual(@as(?u32, 12), embargo_id);
            try std.testing.expect(embargoed);
            state.store_calls += 1;
            return error.TestExpectedError;
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try std.testing.expectError(error.TestExpectedError, handleResolveWithOps(State, &state, .{
        .promise_id = 41,
        .tag = .cap,
        .cap = .{ .tag = .receiverHosted, .id = 9 },
        .exception = null,
    }, .{
        .has_known_promise = State.hasKnown,
        .resolve_cap_descriptor = State.resolve,
        .release_resolved_cap = State.release,
        .alloc_embargo_id = State.allocEmbargo,
        .remember_pending_embargo = State.remember,
        .forget_pending_embargo = State.forget,
        .send_disembargo_sender_loopback = State.send,
        .store_resolved_import = State.store,
    }));

    try std.testing.expectEqual(@as(usize, 0), state.pending_embargoes.count());
    try std.testing.expectEqual(@as(usize, 1), state.release_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqual(@as(usize, 1), state.store_calls);
}
