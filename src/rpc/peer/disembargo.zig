const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const protocol = @import("../wire/protocol.zig");

/// Bundles the callback parameters of handleDisembargo into a single operations struct.
pub fn DisembargoOps(comptime PeerType: type) type {
    return struct {
        has_known_disembargo_target: *const fn (*PeerType, protocol.MessageTarget) bool,
        send_disembargo_receiver_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
        take_pending_embargo_promise: *const fn (*PeerType, u32) ?u32,
        clear_resolved_import_embargo: *const fn (*PeerType, u32) void,
        /// Handle an inbound `context.accept` Disembargo. Receives the target so
        /// the introducer (VatB) can forward the Disembargo to the capability
        /// host (VatC) it originated the `Provide` on, rather than release
        /// locally: only VatC holds the embargoed `Accept`. Implementations that
        /// are the host release; implementations that are the introducer forward
        /// (rpc.capnp:889-903).
        release_embargoed_accepts: *const fn (*PeerType, protocol.MessageTarget, []const u8) anyerror!void,
    };
}

pub fn handleDisembargo(
    comptime PeerType: type,
    peer: *PeerType,
    disembargo: protocol.Disembargo,
    has_known_disembargo_target: *const fn (*PeerType, protocol.MessageTarget) bool,
    send_disembargo_receiver_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
    take_pending_embargo_promise: *const fn (*PeerType, u32) ?u32,
    clear_resolved_import_embargo: *const fn (*PeerType, u32) void,
    release_embargoed_accepts: *const fn (*PeerType, protocol.MessageTarget, []const u8) anyerror!void,
) !void {
    const ops = DisembargoOps(PeerType){
        .has_known_disembargo_target = has_known_disembargo_target,
        .send_disembargo_receiver_loopback = send_disembargo_receiver_loopback,
        .take_pending_embargo_promise = take_pending_embargo_promise,
        .clear_resolved_import_embargo = clear_resolved_import_embargo,
        .release_embargoed_accepts = release_embargoed_accepts,
    };
    try handleDisembargoWithOps(PeerType, peer, disembargo, ops);
}

/// handleDisembargo variant that accepts a bundled DisembargoOps instead of individual callbacks.
pub fn handleDisembargoWithOps(
    comptime PeerType: type,
    peer: *PeerType,
    disembargo: protocol.Disembargo,
    ops: DisembargoOps(PeerType),
) !void {
    switch (disembargo.context_tag) {
        .senderLoopback => {
            // Peer echoes receiverLoopback so both sides can clear embargoed call paths.
            const embargo_id = disembargo.embargo_id orelse return error.MissingEmbargoId;
            switch (disembargo.target.tag) {
                .importedCap => {
                    _ = disembargo.target.imported_cap orelse return error.MissingCallTarget;
                },
                .promisedAnswer => {
                    _ = disembargo.target.promised_answer orelse return error.MissingPromisedAnswer;
                },
            }
            if (!ops.has_known_disembargo_target(peer, disembargo.target)) {
                return error.UnknownDisembargoTarget;
            }
            try ops.send_disembargo_receiver_loopback(peer, disembargo.target, embargo_id);
        },
        .receiverLoopback => {
            // ReceiverLoopback completes the local embargo lifecycle for that promise id.
            const embargo_id = disembargo.embargo_id orelse return error.MissingEmbargoId;
            const promise_id = ops.take_pending_embargo_promise(peer, embargo_id) orelse {
                log.warn("disembargo receiver_loopback for unknown embargo id={}", .{embargo_id});
                return;
            };
            ops.clear_resolved_import_embargo(peer, promise_id);
        },
        .accept => {
            const accept_embargo = disembargo.accept orelse return;
            try ops.release_embargoed_accepts(peer, disembargo.target, accept_embargo);
        },
    }
}

pub fn takePendingEmbargoPromiseForPeer(comptime PeerType: type, peer: *PeerType, embargo_id: u32) ?u32 {
    if (peer.pending_embargoes.fetchRemove(embargo_id)) |entry| {
        return entry.value;
    }
    return null;
}

pub fn takePendingEmbargoPromiseForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?u32 {
    return struct {
        fn call(peer: *PeerType, embargo_id: u32) ?u32 {
            return takePendingEmbargoPromiseForPeer(PeerType, peer, embargo_id);
        }
    }.call;
}

pub fn clearResolvedImportEmbargoForPeer(comptime PeerType: type, peer: *PeerType, promise_id: u32) void {
    if (peer.resolved_imports.getEntry(promise_id)) |resolved| {
        resolved.value_ptr.embargoed = false;
        resolved.value_ptr.embargo_id = null;
    }
}

pub fn clearResolvedImportEmbargoForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) void {
    return struct {
        fn call(peer: *PeerType, promise_id: u32) void {
            clearResolvedImportEmbargoForPeer(PeerType, peer, promise_id);
        }
    }.call;
}

test "peer_disembargo receiver loopback clears resolved import embargo" {
    const ResolvedImport = struct {
        embargo_id: ?u32 = null,
        embargoed: bool = false,
    };
    const State = struct {
        pending_embargoes: std.AutoHashMap(u32, u32),
        resolved_imports: std.AutoHashMap(u32, ResolvedImport),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
                .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.pending_embargoes.deinit();
            self.resolved_imports.deinit();
        }
    };

    var state = State.init(std.testing.allocator);
    defer state.deinit();

    try state.pending_embargoes.put(77, 41);
    try state.resolved_imports.put(41, .{
        .embargo_id = 77,
        .embargoed = true,
    });

    try handleDisembargoWithOps(State, &state, .{
        .target = .{
            .tag = .importedCap,
            .imported_cap = 41,
            .promised_answer = null,
        },
        .context_tag = .receiverLoopback,
        .embargo_id = 77,
        .accept = null,
    }, .{
        .has_known_disembargo_target = struct {
            fn call(_: *State, _: protocol.MessageTarget) bool {
                return false;
            }
        }.call,
        .send_disembargo_receiver_loopback = struct {
            fn call(_: *State, _: protocol.MessageTarget, _: u32) !void {
                return error.TestUnexpectedResult;
            }
        }.call,
        .take_pending_embargo_promise = takePendingEmbargoPromiseForPeerFn(State),
        .clear_resolved_import_embargo = clearResolvedImportEmbargoForPeerFn(State),
        .release_embargoed_accepts = struct {
            fn call(_: *State, _: protocol.MessageTarget, _: []const u8) !void {
                return error.TestUnexpectedResult;
            }
        }.call,
    });

    try std.testing.expect(!state.pending_embargoes.contains(77));
    const resolved = state.resolved_imports.get(41) orelse return error.MissingResolvedImport;
    try std.testing.expect(!resolved.embargoed);
    try std.testing.expectEqual(@as(?u32, null), resolved.embargo_id);
}

test "peer_disembargo sender loopback validates target and echoes receiver loopback" {
    const State = struct {
        send_calls: usize = 0,
        sent_embargo_id: ?u32 = null,
        sent_target: ?protocol.MessageTarget = null,

        fn hasKnown(_: *@This(), target: protocol.MessageTarget) bool {
            return target.tag == .importedCap and target.imported_cap == 41;
        }

        fn send(state: *@This(), target: protocol.MessageTarget, embargo_id: u32) !void {
            state.send_calls += 1;
            state.sent_embargo_id = embargo_id;
            state.sent_target = target;
        }
    };

    var state = State{};
    const target = protocol.MessageTarget{
        .tag = .importedCap,
        .imported_cap = 41,
        .promised_answer = null,
    };

    try handleDisembargo(
        State,
        &state,
        .{
            .target = target,
            .context_tag = .senderLoopback,
            .embargo_id = 99,
            .accept = null,
        },
        State.hasKnown,
        State.send,
        struct {
            fn call(_: *State, _: u32) ?u32 {
                return null;
            }
        }.call,
        struct {
            fn call(_: *State, _: u32) void {}
        }.call,
        struct {
            fn call(_: *State, _: protocol.MessageTarget, _: []const u8) !void {}
        }.call,
    );

    try std.testing.expectEqual(@as(usize, 1), state.send_calls);
    try std.testing.expectEqual(@as(?u32, 99), state.sent_embargo_id);
    try std.testing.expectEqual(protocol.MessageTargetTag.importedCap, state.sent_target.?.tag);
    try std.testing.expectEqual(@as(?u32, 41), state.sent_target.?.imported_cap);

    try std.testing.expectError(error.UnknownDisembargoTarget, handleDisembargo(
        State,
        &state,
        .{
            .target = .{
                .tag = .importedCap,
                .imported_cap = 42,
                .promised_answer = null,
            },
            .context_tag = .senderLoopback,
            .embargo_id = 100,
            .accept = null,
        },
        State.hasKnown,
        State.send,
        struct {
            fn call(_: *State, _: u32) ?u32 {
                return null;
            }
        }.call,
        struct {
            fn call(_: *State, _: u32) void {}
        }.call,
        struct {
            fn call(_: *State, _: protocol.MessageTarget, _: []const u8) !void {}
        }.call,
    ));
}
