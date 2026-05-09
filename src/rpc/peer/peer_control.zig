const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
const peer_forwarded_return_logic = @import("./forward/peer_forwarded_return_logic.zig");
const provide_accept_join = @import("./provide_accept_join.zig");
const protocol = @import("../wire/protocol.zig");
const third_party = @import("./third_party.zig");

pub const bootstrap = @import("./bootstrap.zig");
pub const handleUnimplemented = bootstrap.handleUnimplemented;
pub const handleUnimplementedQuestion = bootstrap.handleUnimplementedQuestion;
pub const handleUnimplementedQuestionForPeerFn = bootstrap.handleUnimplementedQuestionForPeerFn;
pub const handleAbort = bootstrap.handleAbort;
pub const buildBootstrapReturnFrame = bootstrap.buildBootstrapReturnFrame;
pub const handleBootstrap = bootstrap.handleBootstrap;

pub const finish = @import("./finish.zig");
pub const FinishOps = finish.FinishOps;
pub const clearFinishQuestionState = finish.clearFinishQuestionState;
pub const forwardTailFinishIfNeeded = finish.forwardTailFinishIfNeeded;
pub const handleResolvedAnswerCleanup = finish.handleResolvedAnswerCleanup;
pub const handleFinish = finish.handleFinish;
pub const handleFinishWithOps = finish.handleFinishWithOps;
pub const takeResolvedAnswerFrameForPeer = finish.takeResolvedAnswerFrameForPeer;
pub const takeResolvedAnswerFrameForPeerFn = finish.takeResolvedAnswerFrameForPeerFn;
pub const freeOwnedFrameForPeer = finish.freeOwnedFrameForPeer;
pub const freeOwnedFrameForPeerFn = finish.freeOwnedFrameForPeerFn;

pub const resolveProvideImportedCapForPeer = provide_accept_join.resolveProvideImportedCapForPeer;
pub const resolveProvideImportedCapForPeerFn = provide_accept_join.resolveProvideImportedCapForPeerFn;
pub const resolveProvidePromisedAnswerForPeer = provide_accept_join.resolveProvidePromisedAnswerForPeer;
pub const resolveProvidePromisedAnswerForPeerFn = provide_accept_join.resolveProvidePromisedAnswerForPeerFn;
pub const resolveProvideTarget = provide_accept_join.resolveProvideTarget;
pub const resolveProvideTargetForPeerFn = provide_accept_join.resolveProvideTargetForPeerFn;
pub const handleProvide = provide_accept_join.handleProvide;
pub const handleAccept = provide_accept_join.handleAccept;
pub const JoinInsertOutcome = provide_accept_join.JoinInsertOutcome;
pub const handleJoin = provide_accept_join.handleJoin;

pub const noteCallSendResults = third_party.noteCallSendResults;
pub const noteCallSendResultsForPeerFn = third_party.noteCallSendResultsForPeerFn;
pub const isThirdPartyAnswerId = third_party.isThirdPartyAnswerId;
pub const ForwardedReturnMode = third_party.ForwardedReturnMode;
pub const ForwardedCallDestination = third_party.ForwardedCallDestination;
pub const buildForwardedCallDestination = third_party.buildForwardedCallDestination;
pub const applyForwardedCallSendResults = third_party.applyForwardedCallSendResults;
pub const setForwardedCallThirdPartyFromPayloadForPeer = third_party.setForwardedCallThirdPartyFromPayloadForPeer;
pub const setForwardedCallThirdPartyFromPayloadForPeerFn = third_party.setForwardedCallThirdPartyFromPayloadForPeerFn;
pub const captureAnyPointerPayloadForPeer = third_party.captureAnyPointerPayloadForPeer;
pub const captureAnyPointerPayloadForPeerFn = third_party.captureAnyPointerPayloadForPeerFn;
pub const handleMissingReturnQuestion = third_party.handleMissingReturnQuestion;

pub fn handleRelease(
    comptime PeerType: type,
    peer: *PeerType,
    release: protocol.Release,
    on_release_export: *const fn (*PeerType, u32, u32) anyerror!void,
) !void {
    try on_release_export(peer, release.id, release.reference_count);
}

/// Bundles the 8 callback parameters of handleResolve into a single operations struct.
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

/// handleResolve variant that accepts a bundled ResolveOps instead of 7 individual callbacks.
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

test "peer_control handleResolve rolls back pending embargo on sender-loopback failure" {
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

test "peer_control handleResolve rolls back pending embargo on store failure" {
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

/// Bundles the 4 callback parameters of handleDisembargo into a single operations struct.
pub fn DisembargoOps(comptime PeerType: type) type {
    return struct {
        has_known_disembargo_target: *const fn (*PeerType, protocol.MessageTarget) bool,
        send_disembargo_receiver_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
        take_pending_embargo_promise: *const fn (*PeerType, u32) ?u32,
        clear_resolved_import_embargo: *const fn (*PeerType, u32) void,
        release_embargoed_accepts: *const fn (*PeerType, []const u8) anyerror!void,
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
    release_embargoed_accepts: *const fn (*PeerType, []const u8) anyerror!void,
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

/// handleDisembargo variant that accepts a bundled DisembargoOps instead of 4 individual callbacks.
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
            try ops.release_embargoed_accepts(peer, accept_embargo);
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

pub const ForwardResolvedMode = enum {
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

fn forwardModeForSendResults(tag: protocol.SendResultsToTag) ForwardResolvedMode {
    return switch (tag) {
        .caller => .sent_elsewhere,
        .yourself => .propagate_results_sent_elsewhere,
        .thirdParty => .propagate_accept_from_third_party,
    };
}

pub fn handleResolvedCall(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    call: protocol.Call,
    inbound_caps: *const InboundCapsType,
    resolved: cap_table.ResolvedCap,
    handle_exported: *const fn (*PeerType, protocol.Call, *const InboundCapsType, u32) anyerror!void,
    forward_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap, ForwardResolvedMode) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    switch (resolved) {
        .exported => |cap| {
            try handle_exported(peer, call, inbound_caps, cap.id);
        },
        .imported, .promised => {
            const mode = forwardModeForSendResults(call.send_results_to.tag);
            forward_resolved_call(peer, call, inbound_caps, resolved, mode) catch |err| {
                try send_return_exception(peer, call.question_id, @errorName(err));
            };
        },
        .none => {
            try send_return_exception(peer, call.question_id, "promised answer missing");
        },
    }
}

/// Re-export ForwardedReturnOps from forwarded_return_logic for callers that prefer
/// the bundled ops pattern.
pub fn ForwardedReturnOps(comptime PeerType: type, comptime InboundCapsType: type) type {
    return peer_forwarded_return_logic.ForwardedReturnOps(PeerType, InboundCapsType);
}

pub fn handleForwardedReturn(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    mode: ForwardedReturnMode,
    answer_id: u32,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    send_return_results: *const fn (*PeerType, u32, protocol.Payload, *const InboundCapsType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    send_return_tag: *const fn (*PeerType, u32, protocol.ReturnTag) anyerror!void,
    lookup_forwarded_question: *const fn (*PeerType, u32) ?u32,
    send_take_from_other_question: *const fn (*PeerType, u32, u32) anyerror!void,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_accept_from_third_party: *const fn (*PeerType, u32, ?[]const u8) anyerror!void,
    context_third_party_payload: ?[]const u8,
) !void {
    try peer_forwarded_return_logic.handleForwardedReturn(
        PeerType,
        InboundCapsType,
        peer,
        mode,
        answer_id,
        ret,
        inbound_caps,
        send_return_results,
        send_return_exception,
        send_return_tag,
        lookup_forwarded_question,
        send_take_from_other_question,
        capture_payload,
        free_payload,
        send_accept_from_third_party,
        context_third_party_payload,
    );
}

/// handleForwardedReturn variant that accepts a bundled ForwardedReturnOps instead of 8 individual callbacks.
pub fn handleForwardedReturnWithOps(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    mode: ForwardedReturnMode,
    answer_id: u32,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    ops: ForwardedReturnOps(PeerType, InboundCapsType),
    context_third_party_payload: ?[]const u8,
) !void {
    try peer_forwarded_return_logic.handleForwardedReturnWithOps(
        PeerType,
        InboundCapsType,
        peer,
        mode,
        answer_id,
        ret,
        inbound_caps,
        ops,
        context_third_party_payload,
    );
}

pub fn handleReturnRegular(
    comptime PeerType: type,
    comptime QuestionType: type,
    comptime InboundCapsType: type,
    peer: *PeerType,
    question: QuestionType,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    take_adopted_answer_original: *const fn (*PeerType, u32) ?u32,
    restore_adopted_answer_original: *const fn (*PeerType, u32, u32) void,
    dispatch_question_return: *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *const InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    maybe_send_auto_finish: *const fn (*PeerType, QuestionType, u32, bool) anyerror!void,
) !void {
    var callback_ret = ret;
    var restore_adopted_answer = false;
    var adopted_original_answer_id: u32 = undefined;
    if (take_adopted_answer_original(peer, ret.answer_id)) |original_answer_id| {
        callback_ret.answer_id = original_answer_id;
        adopted_original_answer_id = original_answer_id;
        restore_adopted_answer = true;
    }
    errdefer if (restore_adopted_answer) {
        restore_adopted_answer_original(peer, ret.answer_id, adopted_original_answer_id);
    };

    try dispatch_question_return(peer, question, callback_ret, inbound_caps);
    restore_adopted_answer = false;

    if (ret.tag == .results and ret.results != null) {
        release_inbound_caps(peer, inbound_caps) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            report_nonfatal_error(peer, err);
        };
    }

    try maybe_send_auto_finish(peer, question, ret.answer_id, ret.no_finish_needed);
}
