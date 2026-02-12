const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const cap_table = @import("../../level0/cap_table.zig");
const message = @import("../../../serialization/message.zig");
const peer_forwarded_return_logic = @import("forward/peer_forwarded_return_logic.zig");
const protocol = @import("../../level0/protocol.zig");

pub fn handleUnimplemented(
    comptime PeerType: type,
    peer: *PeerType,
    unimplemented: protocol.Unimplemented,
    on_unimplemented_question: *const fn (*PeerType, u32) anyerror!void,
) !void {
    const tag = unimplemented.message_tag orelse return;
    const question_id = unimplemented.question_id orelse return;
    switch (tag) {
        .bootstrap, .call => try on_unimplemented_question(peer, question_id),
        else => {},
    }
}

pub fn handleUnimplementedQuestion(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) !void {
    const ret = protocol.Return{
        .answer_id = question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .exception,
        .results = null,
        .exception = .{
            .reason = "unimplemented",
            .trace = "",
            .type_value = 0,
        },
        .take_from_other_question = null,
    };
    on_return(peer, &.{}, ret) catch |err| switch (err) {
        error.UnknownQuestion => {},
        else => return err,
    };
}

pub fn handleUnimplementedQuestionForPeerFn(
    comptime PeerType: type,
    comptime on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) *const fn (*PeerType, u32) anyerror!void {
    return struct {
        fn call(peer: *PeerType, question_id: u32) anyerror!void {
            try handleUnimplementedQuestion(PeerType, peer, question_id, on_return);
        }
    }.call;
}

pub fn handleAbort(
    allocator: std.mem.Allocator,
    last_remote_abort_reason: *?[]u8,
    abort: protocol.Abort,
) !void {
    if (last_remote_abort_reason.*) |existing| {
        allocator.free(existing);
        last_remote_abort_reason.* = null;
    }
    const reason_copy = try allocator.alloc(u8, abort.exception.reason.len);
    std.mem.copyForwards(u8, reason_copy, abort.exception.reason);
    last_remote_abort_reason.* = reason_copy;
    return error.RemoteAbort;
}

pub fn buildBootstrapReturnFrame(
    allocator: std.mem.Allocator,
    question_id: u32,
    export_id: u32,
) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();

    var ret = try builder.beginReturn(question_id, .results);
    var payload = try ret.payloadTyped();
    var any = try payload.initContent();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTableTyped(1);
    var entry = try cap_list.get(0);
    try entry.setSenderHosted(export_id);

    return builder.finish();
}

pub fn handleBootstrap(
    comptime PeerType: type,
    peer: *PeerType,
    allocator: std.mem.Allocator,
    bootstrap: protocol.Bootstrap,
    bootstrap_export_id: ?u32,
    note_export_ref: *const fn (*PeerType, u32) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    send_frame: *const fn (*PeerType, []const u8) anyerror!void,
    record_resolved_answer: *const fn (*PeerType, u32, []u8) anyerror!void,
) !void {
    const export_id = bootstrap_export_id orelse {
        try send_return_exception(peer, bootstrap.question_id, "bootstrap not configured");
        return;
    };

    const bytes = try buildBootstrapReturnFrame(allocator, bootstrap.question_id, export_id);
    defer allocator.free(bytes);

    try send_frame(peer, bytes);
    try note_export_ref(peer, export_id);

    const copy = try allocator.alloc(u8, bytes.len);
    errdefer allocator.free(copy);
    std.mem.copyForwards(u8, copy, bytes);
    try record_resolved_answer(peer, bootstrap.question_id, copy);
}

pub fn handleRelease(
    comptime PeerType: type,
    peer: *PeerType,
    release: protocol.Release,
    on_release_export: *const fn (*PeerType, u32, u32) void,
) void {
    on_release_export(peer, release.id, release.reference_count);
}

/// Bundles the 11 callback parameters of handleFinish into a single operations struct.
/// Groups: question-state cleanup (5), tail-finish forwarding (2), answer-frame cleanup (3).
pub fn FinishOps(comptime PeerType: type) type {
    return struct {
        // Question-state cleanup
        remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
        clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
        clear_provide: *const fn (*PeerType, u32) void,
        clear_pending_join_question: *const fn (*PeerType, u32) void,
        clear_pending_accept_question: *const fn (*PeerType, u32) void,
        // Tail-finish forwarding
        take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
        send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
        // Answer-frame cleanup
        take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
        release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
        free_frame: *const fn (*PeerType, []u8) void,
    };
}

pub fn clearFinishQuestionState(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
    clear_provide: *const fn (*PeerType, u32) void,
    clear_pending_join_question: *const fn (*PeerType, u32) void,
    clear_pending_accept_question: *const fn (*PeerType, u32) void,
) void {
    remove_send_results_to_yourself(peer, question_id);
    clear_send_results_to_third_party(peer, question_id);
    clear_provide(peer, question_id);
    clear_pending_join_question(peer, question_id);
    clear_pending_accept_question(peer, question_id);
}

pub fn forwardTailFinishIfNeeded(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
    send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
) !void {
    if (take_forwarded_tail_question(peer, question_id)) |tail_question_id| {
        try send_finish(peer, tail_question_id, false);
    }
}

pub fn handleResolvedAnswerCleanup(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
    release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
    free_frame: *const fn (*PeerType, []u8) void,
) !void {
    if (take_resolved_answer_frame(peer, question_id)) |frame| {
        defer free_frame(peer, frame);
        if (release_result_caps) {
            try release_caps_for_frame(peer, frame);
        }
    }
}

pub fn handleFinish(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    remove_send_results_to_yourself: *const fn (*PeerType, u32) void,
    clear_send_results_to_third_party: *const fn (*PeerType, u32) void,
    clear_provide: *const fn (*PeerType, u32) void,
    clear_pending_join_question: *const fn (*PeerType, u32) void,
    clear_pending_accept_question: *const fn (*PeerType, u32) void,
    take_forwarded_tail_question: *const fn (*PeerType, u32) ?u32,
    send_finish: *const fn (*PeerType, u32, bool) anyerror!void,
    take_resolved_answer_frame: *const fn (*PeerType, u32) ?[]u8,
    release_caps_for_frame: *const fn (*PeerType, []const u8) anyerror!void,
    free_frame: *const fn (*PeerType, []u8) void,
) !void {
    const ops = FinishOps(PeerType){
        .remove_send_results_to_yourself = remove_send_results_to_yourself,
        .clear_send_results_to_third_party = clear_send_results_to_third_party,
        .clear_provide = clear_provide,
        .clear_pending_join_question = clear_pending_join_question,
        .clear_pending_accept_question = clear_pending_accept_question,
        .take_forwarded_tail_question = take_forwarded_tail_question,
        .send_finish = send_finish,
        .take_resolved_answer_frame = take_resolved_answer_frame,
        .release_caps_for_frame = release_caps_for_frame,
        .free_frame = free_frame,
    };
    try handleFinishWithOps(PeerType, peer, question_id, release_result_caps, ops);
}

/// handleFinish variant that accepts a bundled FinishOps instead of 11 individual callbacks.
pub fn handleFinishWithOps(
    comptime PeerType: type,
    peer: *PeerType,
    question_id: u32,
    release_result_caps: bool,
    ops: FinishOps(PeerType),
) !void {
    clearFinishQuestionState(
        PeerType,
        peer,
        question_id,
        ops.remove_send_results_to_yourself,
        ops.clear_send_results_to_third_party,
        ops.clear_provide,
        ops.clear_pending_join_question,
        ops.clear_pending_accept_question,
    );
    try forwardTailFinishIfNeeded(
        PeerType,
        peer,
        question_id,
        ops.take_forwarded_tail_question,
        ops.send_finish,
    );
    try handleResolvedAnswerCleanup(
        PeerType,
        peer,
        question_id,
        release_result_caps,
        ops.take_resolved_answer_frame,
        ops.release_caps_for_frame,
        ops.free_frame,
    );
}

/// Bundles the 7 callback parameters of handleResolve into a single operations struct.
pub fn ResolveOps(comptime PeerType: type) type {
    return struct {
        has_known_promise: *const fn (*PeerType, u32) bool,
        resolve_cap_descriptor: *const fn (*PeerType, protocol.CapDescriptor) anyerror!cap_table.ResolvedCap,
        release_resolved_cap: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!void,
        alloc_embargo_id: *const fn (*PeerType) u32,
        remember_pending_embargo: *const fn (*PeerType, u32, u32) anyerror!void,
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
    alloc_embargo_id: *const fn (*PeerType) u32,
    remember_pending_embargo: *const fn (*PeerType, u32, u32) anyerror!void,
    send_disembargo_sender_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
    store_resolved_import: *const fn (*PeerType, u32, ?cap_table.ResolvedCap, ?u32, bool) anyerror!void,
) !void {
    const ops = ResolveOps(PeerType){
        .has_known_promise = has_known_promise,
        .resolve_cap_descriptor = resolve_cap_descriptor,
        .release_resolved_cap = release_resolved_cap,
        .alloc_embargo_id = alloc_embargo_id,
        .remember_pending_embargo = remember_pending_embargo,
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

            if (!known_promise) {
                // Late resolve for an unknown promise id: release any received capability
                // references immediately because no local table entry can own them.
                try ops.release_resolved_cap(peer, resolved);
                return;
            }

            var embargo_id: ?u32 = null;
            var embargoed = false;
            if (resolved == .exported or resolved == .promised) {
                // Exported/promise resolutions require a sender-loopback disembargo handshake
                // before the resolved import can be considered callable locally.
                const new_embargo_id = ops.alloc_embargo_id(peer);
                embargo_id = new_embargo_id;
                embargoed = true;
                try ops.remember_pending_embargo(peer, new_embargo_id, promise_id);
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
        },
        .exception => {
            if (!known_promise) return;
            try ops.store_resolved_import(peer, promise_id, null, null, false);
        },
    }
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

pub fn allocateEmbargoIdForPeer(comptime PeerType: type, peer: *PeerType) u32 {
    const embargo_id = peer.next_embargo_id;
    peer.next_embargo_id +%= 1;
    return embargo_id;
}

pub fn allocateEmbargoIdForPeerFn(comptime PeerType: type) *const fn (*PeerType) u32 {
    return struct {
        fn call(peer: *PeerType) u32 {
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

pub fn takeResolvedAnswerFrameForPeer(comptime PeerType: type, peer: *PeerType, question_id: u32) ?[]u8 {
    if (peer.resolved_answers.fetchRemove(question_id)) |entry| {
        return entry.value.frame;
    }
    return null;
}

pub fn takeResolvedAnswerFrameForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) ?[]u8 {
    return struct {
        fn call(peer: *PeerType, question_id: u32) ?[]u8 {
            return takeResolvedAnswerFrameForPeer(PeerType, peer, question_id);
        }
    }.call;
}

pub fn freeOwnedFrameForPeer(comptime PeerType: type, peer: *PeerType, frame: []u8) void {
    peer.allocator.free(frame);
}

pub fn freeOwnedFrameForPeerFn(comptime PeerType: type) *const fn (*PeerType, []u8) void {
    return struct {
        fn call(peer: *PeerType, frame: []u8) void {
            freeOwnedFrameForPeer(PeerType, peer, frame);
        }
    }.call;
}

pub fn resolveProvideImportedCapForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    export_id: u32,
) !cap_table.ResolvedCap {
    const exported_entry = peer.exports.getEntry(export_id) orelse return error.UnknownExport;

    if (exported_entry.value_ptr.is_promise) {
        const resolved = exported_entry.value_ptr.resolved orelse return error.PromiseUnresolved;
        if (resolved == .none) return error.PromiseBroken;
        return resolved;
    }
    return .{ .exported = .{ .id = export_id } };
}

pub fn resolveProvideImportedCapForPeerFn(
    comptime PeerType: type,
) *const fn (*PeerType, u32) anyerror!cap_table.ResolvedCap {
    return struct {
        fn call(peer: *PeerType, export_id: u32) anyerror!cap_table.ResolvedCap {
            return try resolveProvideImportedCapForPeer(PeerType, peer, export_id);
        }
    }.call;
}

pub fn resolveProvidePromisedAnswerForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    promised: protocol.PromisedAnswer,
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
) !cap_table.ResolvedCap {
    const resolved = try resolve_promised_answer(peer, promised);
    if (resolved == .none) return error.PromisedAnswerMissing;
    return resolved;
}

pub fn resolveProvidePromisedAnswerForPeerFn(
    comptime PeerType: type,
    comptime resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
) *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap {
    return struct {
        fn call(peer: *PeerType, promised: protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap {
            return try resolveProvidePromisedAnswerForPeer(
                PeerType,
                peer,
                promised,
                resolve_promised_answer,
            );
        }
    }.call;
}

pub fn resolveProvideTarget(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    resolve_imported_cap: *const fn (*PeerType, u32) anyerror!cap_table.ResolvedCap,
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
) !cap_table.ResolvedCap {
    return switch (target.tag) {
        .importedCap => {
            const export_id = target.imported_cap orelse return error.MissingCallTarget;
            return resolve_imported_cap(peer, export_id);
        },
        .promisedAnswer => {
            const promised = target.promised_answer orelse return error.MissingPromisedAnswer;
            return resolve_promised_answer(peer, promised);
        },
    };
}

pub fn resolveProvideTargetForPeerFn(
    comptime PeerType: type,
    comptime resolve_imported_cap: *const fn (*PeerType, u32) anyerror!cap_table.ResolvedCap,
    comptime resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
) *const fn (*PeerType, protocol.MessageTarget) anyerror!cap_table.ResolvedCap {
    return struct {
        fn call(peer: *PeerType, target: protocol.MessageTarget) anyerror!cap_table.ResolvedCap {
            return try resolveProvideTarget(
                PeerType,
                peer,
                target,
                resolve_imported_cap,
                resolve_promised_answer,
            );
        }
    }.call;
}

pub fn handleProvide(
    comptime PeerType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    provide: protocol.Provide,
    capture_recipient: *const fn (*PeerType, protocol.Provide) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    has_question: *const fn (*PeerType, u32) bool,
    has_recipient: *const fn (*PeerType, []const u8) bool,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    resolve_target: *const fn (*PeerType, protocol.MessageTarget) anyerror!cap_table.ResolvedCap,
    make_target: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!ProvideTargetType,
    deinit_target: *const fn (*PeerType, *ProvideTargetType) void,
    put_question: *const fn (*PeerType, u32, []u8, ProvideTargetType) anyerror!void,
    clear_provide: *const fn (*PeerType, u32) void,
    put_key: *const fn (*PeerType, []const u8, u32) anyerror!void,
) !void {
    const key = try capture_recipient(peer, provide);
    const key_bytes = key orelse {
        try send_abort(peer, "provide missing recipient");
        return error.MissingThirdPartyPayload;
    };
    errdefer free_payload(peer, key_bytes);

    if (has_question(peer, provide.question_id)) {
        try send_abort(peer, "duplicate provide question");
        return error.DuplicateProvideQuestionId;
    }
    if (has_recipient(peer, key_bytes)) {
        try send_abort(peer, "duplicate provide recipient");
        return error.DuplicateProvideRecipient;
    }

    const resolved = resolve_target(peer, provide.target) catch |err| {
        try send_abort(peer, @errorName(err));
        return err;
    };
    var target = try make_target(peer, resolved);
    errdefer deinit_target(peer, &target);

    try put_question(peer, provide.question_id, key_bytes, target);
    errdefer clear_provide(peer, provide.question_id);
    try put_key(peer, key_bytes, provide.question_id);
}

pub fn handleAccept(
    comptime PeerType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    accept: protocol.Accept,
    capture_provision: *const fn (*PeerType, protocol.Accept) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    get_provided_question: *const fn (*PeerType, []const u8) ?u32,
    get_provided_target: *const fn (*PeerType, u32) ?*ProvideTargetType,
    queue_embargoed_accept: *const fn (*PeerType, u32, u32, []const u8) anyerror!void,
    send_return_provided_target: *const fn (*PeerType, u32, *const ProvideTargetType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    const key = try capture_provision(peer, accept);
    defer if (key) |bytes| free_payload(peer, bytes);
    const key_bytes = key orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };

    const provided_question_id = get_provided_question(peer, key_bytes) orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };
    const target = get_provided_target(peer, provided_question_id) orelse {
        try send_return_exception(peer, accept.question_id, "unknown provision");
        return;
    };

    if (accept.embargo) |embargo| {
        try queue_embargoed_accept(peer, accept.question_id, provided_question_id, embargo);
        return;
    }

    send_return_provided_target(peer, accept.question_id, target) catch |err| {
        try send_return_exception(peer, accept.question_id, @errorName(err));
    };
}

pub fn noteCallSendResults(
    comptime PeerType: type,
    peer: *PeerType,
    call: protocol.Call,
    note_send_results_to_yourself: *const fn (*PeerType, u32) anyerror!void,
    note_send_results_to_third_party: *const fn (*PeerType, u32, ?message.AnyPointerReader) anyerror!void,
) !void {
    switch (call.send_results_to.tag) {
        .caller => {},
        .yourself => {
            try note_send_results_to_yourself(peer, call.question_id);
        },
        .thirdParty => {
            try note_send_results_to_third_party(peer, call.question_id, call.send_results_to.third_party);
        },
    }
}

pub fn noteCallSendResultsForPeerFn(
    comptime PeerType: type,
    comptime note_send_results_to_yourself: *const fn (*PeerType, u32) anyerror!void,
    comptime note_send_results_to_third_party: *const fn (*PeerType, u32, ?message.AnyPointerReader) anyerror!void,
) *const fn (*PeerType, protocol.Call) anyerror!void {
    return struct {
        fn call(peer: *PeerType, rpc_call: protocol.Call) anyerror!void {
            try noteCallSendResults(
                PeerType,
                peer,
                rpc_call,
                note_send_results_to_yourself,
                note_send_results_to_third_party,
            );
        }
    }.call;
}

pub const JoinInsertOutcome = enum {
    inserted,
    inserted_ready,
    part_count_mismatch,
    duplicate_part,
};

pub fn handleJoin(
    comptime PeerType: type,
    comptime JoinKeyPartType: type,
    comptime ProvideTargetType: type,
    peer: *PeerType,
    join: protocol.Join,
    has_pending_join_question: *const fn (*PeerType, u32) bool,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    parse_join_key_part: *const fn (*PeerType, protocol.Join) anyerror!JoinKeyPartType,
    resolve_target: *const fn (*PeerType, protocol.MessageTarget) anyerror!cap_table.ResolvedCap,
    make_target: *const fn (*PeerType, cap_table.ResolvedCap) anyerror!ProvideTargetType,
    deinit_target: *const fn (*PeerType, *ProvideTargetType) void,
    insert_join_part: *const fn (*PeerType, JoinKeyPartType, u32, ProvideTargetType) anyerror!JoinInsertOutcome,
    complete_join: *const fn (*PeerType, JoinKeyPartType) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    if (has_pending_join_question(peer, join.question_id)) {
        try send_abort(peer, "duplicate join question");
        return error.DuplicateJoinQuestionId;
    }

    const join_key_part = parse_join_key_part(peer, join) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    const resolved = resolve_target(peer, join.target) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    var target = make_target(peer, resolved) catch |err| {
        try send_return_exception(peer, join.question_id, @errorName(err));
        return;
    };

    const outcome = insert_join_part(peer, join_key_part, join.question_id, target) catch |err| {
        deinit_target(peer, &target);
        return err;
    };

    switch (outcome) {
        .inserted => {},
        .inserted_ready => {
            try complete_join(peer, join_key_part);
        },
        .part_count_mismatch => {
            deinit_target(peer, &target);
            try send_return_exception(peer, join.question_id, "join partCount mismatch");
        },
        .duplicate_part => {
            deinit_target(peer, &target);
            try send_return_exception(peer, join.question_id, "duplicate join part");
        },
    }
}

pub fn isThirdPartyAnswerId(answer_id: u32) bool {
    return (answer_id & 0x4000_0000) != 0 and (answer_id & 0x8000_0000) == 0;
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

pub const ForwardedReturnMode = peer_forwarded_return_logic.ForwardedReturnMode;

pub const ForwardedCallDestination = union(enum) {
    caller,
    yourself,
    third_party: ?[]u8,

    pub fn sendResultsToTag(self: ForwardedCallDestination) protocol.SendResultsToTag {
        return switch (self) {
            .caller => .caller,
            .yourself => .yourself,
            .third_party => .thirdParty,
        };
    }

    pub fn thirdPartyPayload(self: ForwardedCallDestination) ?[]u8 {
        return switch (self) {
            .third_party => |payload| payload,
            else => null,
        };
    }
};

pub fn buildForwardedCallDestination(
    comptime PeerType: type,
    peer: *PeerType,
    mode: ForwardedReturnMode,
    third_party: ?message.AnyPointerReader,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
) !ForwardedCallDestination {
    return switch (mode) {
        .translate_to_caller => .caller,
        .sent_elsewhere, .propagate_results_sent_elsewhere => .yourself,
        .propagate_accept_from_third_party => .{
            .third_party = try capture_payload(peer, third_party),
        },
    };
}

pub fn applyForwardedCallSendResults(
    comptime PeerType: type,
    peer: *PeerType,
    call_builder: *protocol.CallBuilder,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]const u8,
    set_third_party_from_payload: *const fn (*PeerType, *protocol.CallBuilder, []const u8) anyerror!void,
) !void {
    switch (send_results_to) {
        .caller => call_builder.setSendResultsToCaller(),
        .yourself => call_builder.setSendResultsToYourself(),
        .thirdParty => {
            if (send_results_to_third_party_payload) |payload| {
                try set_third_party_from_payload(peer, call_builder, payload);
            } else {
                try call_builder.setSendResultsToThirdPartyNull();
            }
        },
    }
}

pub fn setForwardedCallThirdPartyFromPayloadForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    call_builder: *protocol.CallBuilder,
    payload: []const u8,
) !void {
    var msg = try message.Message.init(peer.allocator, payload);
    defer msg.deinit();
    const third_party = try msg.getRootAnyPointer();
    try call_builder.setSendResultsToThirdParty(third_party);
}

pub fn setForwardedCallThirdPartyFromPayloadForPeerFn(
    comptime PeerType: type,
) *const fn (*PeerType, *protocol.CallBuilder, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, call_builder: *protocol.CallBuilder, payload: []const u8) anyerror!void {
            try setForwardedCallThirdPartyFromPayloadForPeer(
                PeerType,
                peer,
                call_builder,
                payload,
            );
        }
    }.call;
}

pub fn captureAnyPointerPayloadForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    ptr: ?message.AnyPointerReader,
    capture_payload: *const fn (std.mem.Allocator, ?message.AnyPointerReader) anyerror!?[]u8,
) !?[]u8 {
    return capture_payload(peer.allocator, ptr);
}

pub fn captureAnyPointerPayloadForPeerFn(
    comptime PeerType: type,
    comptime capture_payload: *const fn (std.mem.Allocator, ?message.AnyPointerReader) anyerror!?[]u8,
) *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8 {
    return struct {
        fn call(peer: *PeerType, ptr: ?message.AnyPointerReader) anyerror!?[]u8 {
            return try captureAnyPointerPayloadForPeer(
                PeerType,
                peer,
                ptr,
                capture_payload,
            );
        }
    }.call;
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

pub fn handleMissingReturnQuestion(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    answer_id: u32,
    is_third_party_answer_id: *const fn (u32) bool,
    has_pending_third_party_return: *const fn (*PeerType, u32) bool,
    buffer_pending_third_party_return: *const fn (*PeerType, u32, []const u8) anyerror!void,
) !void {
    if (is_third_party_answer_id(answer_id)) {
        if (has_pending_third_party_return(peer, answer_id)) {
            return error.DuplicateThirdPartyReturn;
        }
        try buffer_pending_third_party_return(peer, answer_id, frame);
        return;
    }
    return error.UnknownQuestion;
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
    dispatch_question_return: *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *const InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    maybe_send_auto_finish: *const fn (*PeerType, QuestionType, u32, bool) anyerror!void,
) !void {
    var callback_ret = ret;
    if (take_adopted_answer_original(peer, ret.answer_id)) |original_answer_id| {
        callback_ret.answer_id = original_answer_id;
    }

    try dispatch_question_return(peer, question, callback_ret, inbound_caps);

    if (ret.tag == .results and ret.results != null) {
        release_inbound_caps(peer, inbound_caps) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            report_nonfatal_error(peer, err);
        };
    }

    try maybe_send_auto_finish(peer, question, ret.answer_id, ret.no_finish_needed);
}
