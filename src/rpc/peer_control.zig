const std = @import("std");
const cap_table = @import("cap_table.zig");
const message = @import("../message.zig");
const peer_forwarded_return_logic = @import("peer_forwarded_return_logic.zig");
const protocol = @import("protocol.zig");

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
    var any = try ret.getResultsAnyPointer();
    try any.setCapability(.{ .id = 0 });

    var cap_list = try ret.initCapTable(1);
    const entry = try cap_list.get(0);
    protocol.CapDescriptor.writeSenderHosted(entry, export_id);

    return builder.finish();
}

pub fn handleRelease(
    comptime PeerType: type,
    peer: *PeerType,
    release: protocol.Release,
    on_release_export: *const fn (*PeerType, u32, u32) void,
) void {
    on_release_export(peer, release.id, release.reference_count);
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
    clearFinishQuestionState(
        PeerType,
        peer,
        question_id,
        remove_send_results_to_yourself,
        clear_send_results_to_third_party,
        clear_provide,
        clear_pending_join_question,
        clear_pending_accept_question,
    );
    try forwardTailFinishIfNeeded(
        PeerType,
        peer,
        question_id,
        take_forwarded_tail_question,
        send_finish,
    );
    try handleResolvedAnswerCleanup(
        PeerType,
        peer,
        question_id,
        release_result_caps,
        take_resolved_answer_frame,
        release_caps_for_frame,
        free_frame,
    );
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
    const promise_id = resolve.promise_id;
    const known_promise = has_known_promise(peer, promise_id);

    switch (resolve.tag) {
        .cap => {
            const descriptor = resolve.cap orelse return error.MissingResolveCap;
            const resolved = try resolve_cap_descriptor(peer, descriptor);

            if (!known_promise) {
                try release_resolved_cap(peer, resolved);
                return;
            }

            var embargo_id: ?u32 = null;
            var embargoed = false;
            if (resolved == .exported or resolved == .promised) {
                const new_embargo_id = alloc_embargo_id(peer);
                embargo_id = new_embargo_id;
                embargoed = true;
                try remember_pending_embargo(peer, new_embargo_id, promise_id);
                const target = switch (resolved) {
                    .promised => |promised| protocol.MessageTarget{
                        .tag = .promised_answer,
                        .imported_cap = null,
                        .promised_answer = promised,
                    },
                    else => protocol.MessageTarget{
                        .tag = .imported_cap,
                        .imported_cap = promise_id,
                        .promised_answer = null,
                    },
                };
                try send_disembargo_sender_loopback(peer, target, new_embargo_id);
            }

            try store_resolved_import(peer, promise_id, resolved, embargo_id, embargoed);
        },
        .exception => {
            if (!known_promise) return;
            try store_resolved_import(peer, promise_id, null, null, false);
        },
    }
}

pub fn handleDisembargo(
    comptime PeerType: type,
    peer: *PeerType,
    disembargo: protocol.Disembargo,
    send_disembargo_receiver_loopback: *const fn (*PeerType, protocol.MessageTarget, u32) anyerror!void,
    take_pending_embargo_promise: *const fn (*PeerType, u32) ?u32,
    clear_resolved_import_embargo: *const fn (*PeerType, u32) void,
    release_embargoed_accepts: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    switch (disembargo.context_tag) {
        .sender_loopback => {
            const embargo_id = disembargo.embargo_id orelse return error.MissingEmbargoId;
            switch (disembargo.target.tag) {
                .imported_cap => {
                    _ = disembargo.target.imported_cap orelse return error.MissingCallTarget;
                },
                .promised_answer => {
                    _ = disembargo.target.promised_answer orelse return error.MissingPromisedAnswer;
                },
            }
            try send_disembargo_receiver_loopback(peer, disembargo.target, embargo_id);
        },
        .receiver_loopback => {
            const embargo_id = disembargo.embargo_id orelse return error.MissingEmbargoId;
            const promise_id = take_pending_embargo_promise(peer, embargo_id) orelse return;
            clear_resolved_import_embargo(peer, promise_id);
        },
        .accept => {
            const accept_embargo = disembargo.accept orelse return;
            try release_embargoed_accepts(peer, accept_embargo);
        },
    }
}

pub fn resolveProvideTarget(
    comptime PeerType: type,
    peer: *PeerType,
    target: protocol.MessageTarget,
    resolve_imported_cap: *const fn (*PeerType, u32) anyerror!cap_table.ResolvedCap,
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
) !cap_table.ResolvedCap {
    return switch (target.tag) {
        .imported_cap => {
            const export_id = target.imported_cap orelse return error.MissingCallTarget;
            return resolve_imported_cap(peer, export_id);
        },
        .promised_answer => {
            const promised = target.promised_answer orelse return error.MissingPromisedAnswer;
            return resolve_promised_answer(peer, promised);
        },
    };
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
        .third_party => {
            try note_send_results_to_third_party(peer, call.question_id, call.send_results_to.third_party);
        },
    }
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

pub fn handleThirdPartyAnswer(
    comptime PeerType: type,
    peer: *PeerType,
    third_party_answer: protocol.ThirdPartyAnswer,
    is_third_party_answer_id: *const fn (u32) bool,
    capture_completion: *const fn (*PeerType, protocol.ThirdPartyAnswer) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    adopt_pending_await: *const fn (*PeerType, []const u8, u32) anyerror!bool,
    get_pending_answer_id: *const fn (*PeerType, []const u8) ?u32,
    put_pending_answer: *const fn (*PeerType, []u8, u32) anyerror!void,
) !void {
    if (!is_third_party_answer_id(third_party_answer.answer_id)) {
        try send_abort(peer, "invalid thirdPartyAnswer answerId");
        return error.InvalidThirdPartyAnswerId;
    }

    const completion_payload = try capture_completion(peer, third_party_answer);
    const completion_key = completion_payload orelse {
        try send_abort(peer, "thirdPartyAnswer missing completion");
        return error.MissingThirdPartyPayload;
    };
    var owns_completion_key = true;
    errdefer if (owns_completion_key) free_payload(peer, completion_key);

    if (try adopt_pending_await(peer, completion_key, third_party_answer.answer_id)) {
        owns_completion_key = false;
        free_payload(peer, completion_key);
        return;
    }

    if (get_pending_answer_id(peer, completion_key)) |existing_id| {
        if (existing_id == third_party_answer.answer_id) {
            owns_completion_key = false;
            free_payload(peer, completion_key);
            return;
        }
        try send_abort(peer, "conflicting thirdPartyAnswer completion");
        return error.ConflictingThirdPartyAnswer;
    }

    try put_pending_answer(peer, completion_key, third_party_answer.answer_id);
    owns_completion_key = false;
}

pub fn isThirdPartyAnswerId(answer_id: u32) bool {
    return (answer_id & 0x4000_0000) != 0 and (answer_id & 0x8000_0000) == 0;
}

pub fn adoptThirdPartyAnswer(
    comptime PeerType: type,
    comptime QuestionType: type,
    peer: *PeerType,
    question_id: u32,
    adopted_answer_id: u32,
    question: QuestionType,
    has_question: *const fn (*PeerType, u32) bool,
    has_adopted_answer: *const fn (*PeerType, u32) bool,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    put_question: *const fn (*PeerType, u32, QuestionType) anyerror!void,
    remove_question: *const fn (*PeerType, u32) void,
    put_adopted_answer: *const fn (*PeerType, u32, u32) anyerror!void,
    remove_adopted_answer: *const fn (*PeerType, u32) void,
    take_pending_return_frame: *const fn (*PeerType, u32) ?[]u8,
    free_frame: *const fn (*PeerType, []u8) void,
    handle_pending_return_frame: *const fn (*PeerType, []const u8) anyerror!void,
) !void {
    if (!isThirdPartyAnswerId(adopted_answer_id)) {
        try send_abort(peer, "invalid thirdPartyAnswer answerId");
        return error.InvalidThirdPartyAnswerId;
    }
    if (has_question(peer, adopted_answer_id) or has_adopted_answer(peer, adopted_answer_id)) {
        try send_abort(peer, "duplicate thirdPartyAnswer answerId");
        return error.DuplicateThirdPartyAnswerId;
    }

    try put_question(peer, adopted_answer_id, question);
    errdefer remove_question(peer, adopted_answer_id);
    try put_adopted_answer(peer, adopted_answer_id, question_id);
    errdefer remove_adopted_answer(peer, adopted_answer_id);

    if (take_pending_return_frame(peer, adopted_answer_id)) |pending_frame| {
        defer free_frame(peer, pending_frame);
        try handle_pending_return_frame(peer, pending_frame);
    }
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
        .third_party => .propagate_accept_from_third_party,
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
            .third_party => .third_party,
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
        .third_party => {
            if (send_results_to_third_party_payload) |payload| {
                try set_third_party_from_payload(peer, call_builder, payload);
            } else {
                try call_builder.setSendResultsToThirdPartyNull();
            }
        },
    }
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

pub fn handleReturnAcceptFromThirdParty(
    comptime PeerType: type,
    comptime QuestionType: type,
    peer: *PeerType,
    answer_id: u32,
    question: QuestionType,
    accept_from_third_party: ?message.AnyPointerReader,
    capture_completion_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    has_pending_await: *const fn (*PeerType, []const u8) bool,
    send_abort: *const fn (*PeerType, []const u8) anyerror!void,
    take_pending_answer_id: *const fn (*PeerType, []const u8) ?u32,
    adopt_third_party_answer: *const fn (*PeerType, u32, u32, QuestionType) anyerror!void,
    put_pending_await: *const fn (*PeerType, []u8, u32, QuestionType) anyerror!void,
) !void {
    const completion_payload = try capture_completion_payload(peer, accept_from_third_party);
    const completion_key = completion_payload orelse return error.MissingThirdPartyPayload;
    var owns_completion_key = true;
    errdefer if (owns_completion_key) free_payload(peer, completion_key);

    if (has_pending_await(peer, completion_key)) {
        try send_abort(peer, "duplicate awaitFromThirdParty completion");
        return error.DuplicateThirdPartyAwait;
    }

    if (take_pending_answer_id(peer, completion_key)) |pending_answer_id| {
        free_payload(peer, completion_key);
        owns_completion_key = false;
        try adopt_third_party_answer(peer, answer_id, pending_answer_id, question);
    } else {
        try put_pending_await(peer, completion_key, answer_id, question);
        owns_completion_key = false;
    }
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
    dispatch_question_return: *const fn (*PeerType, QuestionType, protocol.Return, *const InboundCapsType) void,
    release_inbound_caps: *const fn (*PeerType, *const InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    maybe_send_auto_finish: *const fn (*PeerType, QuestionType, u32, bool) void,
) void {
    var callback_ret = ret;
    if (take_adopted_answer_original(peer, ret.answer_id)) |original_answer_id| {
        callback_ret.answer_id = original_answer_id;
    }

    dispatch_question_return(peer, question, callback_ret, inbound_caps);

    if (ret.tag == .results and ret.results != null) {
        release_inbound_caps(peer, inbound_caps) catch |err| {
            report_nonfatal_error(peer, err);
        };
    }

    maybe_send_auto_finish(peer, question, ret.answer_id, ret.no_finish_needed);
}

test "peer_control handleFinish runs clear, tail-forward, and resolved cleanup" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        tail_question_id: ?u32 = null,
        send_finish_calls: usize = 0,
        last_finish_question_id: u32 = 0,
        last_finish_release_result_caps: bool = true,
        resolved_frame: ?[]u8 = null,
        release_caps_calls: usize = 0,
        free_frame_calls: usize = 0,
    };

    const Hooks = struct {
        fn noteClear(state: *State, question_id: u32) void {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            state.clear_calls += 1;
        }

        fn removeSendResultsToYourself(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearSendResultsToThirdParty(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearProvide(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingJoinQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingAcceptQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn takeForwardedTailQuestion(state: *State, question_id: u32) ?u32 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return state.tail_question_id;
        }

        fn sendFinish(state: *State, question_id: u32, release_result_caps: bool) !void {
            state.send_finish_calls += 1;
            state.last_finish_question_id = question_id;
            state.last_finish_release_result_caps = release_result_caps;
        }

        fn takeResolvedAnswerFrame(state: *State, question_id: u32) ?[]u8 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            const frame = state.resolved_frame;
            state.resolved_frame = null;
            return frame;
        }

        fn releaseCapsForFrame(state: *State, frame: []const u8) !void {
            state.release_caps_calls += 1;
            try std.testing.expectEqualSlices(u8, &[_]u8{ 9, 8, 7 }, frame);
        }

        fn freeFrame(state: *State, frame: []u8) void {
            std.testing.expectEqualSlices(u8, &[_]u8{ 9, 8, 7 }, frame) catch unreachable;
            state.free_frame_calls += 1;
        }
    };

    var frame_storage = [_]u8{ 9, 8, 7 };
    var state = State{
        .expected_question_id = 51,
        .tail_question_id = 88,
        .resolved_frame = frame_storage[0..],
    };

    try handleFinish(
        State,
        &state,
        51,
        true,
        Hooks.removeSendResultsToYourself,
        Hooks.clearSendResultsToThirdParty,
        Hooks.clearProvide,
        Hooks.clearPendingJoinQuestion,
        Hooks.clearPendingAcceptQuestion,
        Hooks.takeForwardedTailQuestion,
        Hooks.sendFinish,
        Hooks.takeResolvedAnswerFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 1), state.send_finish_calls);
    try std.testing.expectEqual(@as(u32, 88), state.last_finish_question_id);
    try std.testing.expectEqual(false, state.last_finish_release_result_caps);
    try std.testing.expectEqual(@as(usize, 1), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 1), state.free_frame_calls);
}

test "peer_control handleFinish skips optional tail and resolved cleanup when absent" {
    const State = struct {
        expected_question_id: u32,
        clear_calls: usize = 0,
        send_finish_calls: usize = 0,
        release_caps_calls: usize = 0,
        free_frame_calls: usize = 0,
    };

    const Hooks = struct {
        fn noteClear(state: *State, question_id: u32) void {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            state.clear_calls += 1;
        }

        fn removeSendResultsToYourself(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearSendResultsToThirdParty(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearProvide(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingJoinQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn clearPendingAcceptQuestion(state: *State, question_id: u32) void {
            noteClear(state, question_id);
        }

        fn takeForwardedTailQuestion(state: *State, question_id: u32) ?u32 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return null;
        }

        fn sendFinish(state: *State, question_id: u32, release_result_caps: bool) !void {
            _ = question_id;
            _ = release_result_caps;
            state.send_finish_calls += 1;
        }

        fn takeResolvedAnswerFrame(state: *State, question_id: u32) ?[]u8 {
            std.testing.expectEqual(state.expected_question_id, question_id) catch unreachable;
            return null;
        }

        fn releaseCapsForFrame(state: *State, frame: []const u8) !void {
            _ = frame;
            state.release_caps_calls += 1;
        }

        fn freeFrame(state: *State, frame: []u8) void {
            _ = frame;
            state.free_frame_calls += 1;
        }
    };

    var state = State{ .expected_question_id = 19 };
    try handleFinish(
        State,
        &state,
        19,
        true,
        Hooks.removeSendResultsToYourself,
        Hooks.clearSendResultsToThirdParty,
        Hooks.clearProvide,
        Hooks.clearPendingJoinQuestion,
        Hooks.clearPendingAcceptQuestion,
        Hooks.takeForwardedTailQuestion,
        Hooks.sendFinish,
        Hooks.takeResolvedAnswerFrame,
        Hooks.releaseCapsForFrame,
        Hooks.freeFrame,
    );

    try std.testing.expectEqual(@as(usize, 5), state.clear_calls);
    try std.testing.expectEqual(@as(usize, 0), state.send_finish_calls);
    try std.testing.expectEqual(@as(usize, 0), state.release_caps_calls);
    try std.testing.expectEqual(@as(usize, 0), state.free_frame_calls);
}

test "peer_control adoptThirdPartyAnswer rejects invalid adopted answer id" {
    const Question = struct { marker: u32 };
    const State = struct {
        abort_reason: ?[]const u8 = null,
    };

    const Hooks = struct {
        fn hasQuestion(_: *State, _: u32) bool {
            return false;
        }

        fn hasAdopted(_: *State, _: u32) bool {
            return false;
        }

        fn sendAbort(state: *State, reason: []const u8) !void {
            state.abort_reason = reason;
        }

        fn putQuestion(_: *State, _: u32, _: Question) !void {
            return error.TestUnexpectedResult;
        }

        fn removeQuestion(_: *State, _: u32) void {}

        fn putAdopted(_: *State, _: u32, _: u32) !void {
            return error.TestUnexpectedResult;
        }

        fn removeAdopted(_: *State, _: u32) void {}

        fn takePendingReturn(_: *State, _: u32) ?[]u8 {
            return null;
        }

        fn freeFrame(_: *State, _: []u8) void {}

        fn handlePendingReturn(_: *State, _: []const u8) !void {
            return error.TestUnexpectedResult;
        }
    };

    var state = State{};
    const err = adoptThirdPartyAnswer(
        State,
        Question,
        &state,
        55,
        1,
        .{ .marker = 123 },
        Hooks.hasQuestion,
        Hooks.hasAdopted,
        Hooks.sendAbort,
        Hooks.putQuestion,
        Hooks.removeQuestion,
        Hooks.putAdopted,
        Hooks.removeAdopted,
        Hooks.takePendingReturn,
        Hooks.freeFrame,
        Hooks.handlePendingReturn,
    );
    try std.testing.expectError(error.InvalidThirdPartyAnswerId, err);
    try std.testing.expectEqualStrings("invalid thirdPartyAnswer answerId", state.abort_reason orelse "");
}

test "peer_control adoptThirdPartyAnswer stores mapping and replays pending return frame" {
    const Question = struct { marker: u32 };
    const State = struct {
        has_question: bool = false,
        has_adopted: bool = false,
        put_question_calls: usize = 0,
        put_adopted_calls: usize = 0,
        remove_question_calls: usize = 0,
        remove_adopted_calls: usize = 0,
        stored_answer_id: u32 = 0,
        stored_question: Question = .{ .marker = 0 },
        stored_adopted_answer_id: u32 = 0,
        stored_original_question_id: u32 = 0,
        pending_frame: ?[]u8 = null,
        handled_pending_frames: usize = 0,
        freed_pending_frames: usize = 0,
        abort_reason: ?[]const u8 = null,
    };

    const Hooks = struct {
        fn hasQuestion(state: *State, answer_id: u32) bool {
            _ = answer_id;
            return state.has_question;
        }

        fn hasAdopted(state: *State, answer_id: u32) bool {
            _ = answer_id;
            return state.has_adopted;
        }

        fn sendAbort(state: *State, reason: []const u8) !void {
            state.abort_reason = reason;
        }

        fn putQuestion(state: *State, answer_id: u32, question: Question) !void {
            state.has_question = true;
            state.put_question_calls += 1;
            state.stored_answer_id = answer_id;
            state.stored_question = question;
        }

        fn removeQuestion(state: *State, _: u32) void {
            state.has_question = false;
            state.remove_question_calls += 1;
        }

        fn putAdopted(state: *State, adopted_answer_id: u32, question_id: u32) !void {
            state.has_adopted = true;
            state.put_adopted_calls += 1;
            state.stored_adopted_answer_id = adopted_answer_id;
            state.stored_original_question_id = question_id;
        }

        fn removeAdopted(state: *State, _: u32) void {
            state.has_adopted = false;
            state.remove_adopted_calls += 1;
        }

        fn takePendingReturn(state: *State, _: u32) ?[]u8 {
            const frame = state.pending_frame;
            state.pending_frame = null;
            return frame;
        }

        fn freeFrame(state: *State, frame: []u8) void {
            std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD }, frame) catch unreachable;
            state.freed_pending_frames += 1;
        }

        fn handlePendingReturn(state: *State, frame: []const u8) !void {
            try std.testing.expectEqualSlices(u8, &[_]u8{ 0xDE, 0xAD }, frame);
            state.handled_pending_frames += 1;
        }
    };

    var pending_storage = [_]u8{ 0xDE, 0xAD };
    var state = State{
        .pending_frame = pending_storage[0..],
    };
    const adopted_answer_id: u32 = 0x4000_0101;
    const original_question_id: u32 = 77;
    const adopted_question = Question{ .marker = 999 };

    try adoptThirdPartyAnswer(
        State,
        Question,
        &state,
        original_question_id,
        adopted_answer_id,
        adopted_question,
        Hooks.hasQuestion,
        Hooks.hasAdopted,
        Hooks.sendAbort,
        Hooks.putQuestion,
        Hooks.removeQuestion,
        Hooks.putAdopted,
        Hooks.removeAdopted,
        Hooks.takePendingReturn,
        Hooks.freeFrame,
        Hooks.handlePendingReturn,
    );

    try std.testing.expectEqual(@as(usize, 1), state.put_question_calls);
    try std.testing.expectEqual(@as(usize, 1), state.put_adopted_calls);
    try std.testing.expectEqual(adopted_answer_id, state.stored_answer_id);
    try std.testing.expectEqual(adopted_answer_id, state.stored_adopted_answer_id);
    try std.testing.expectEqual(original_question_id, state.stored_original_question_id);
    try std.testing.expectEqual(adopted_question.marker, state.stored_question.marker);
    try std.testing.expectEqual(@as(usize, 0), state.remove_question_calls);
    try std.testing.expectEqual(@as(usize, 0), state.remove_adopted_calls);
    try std.testing.expectEqual(@as(usize, 1), state.handled_pending_frames);
    try std.testing.expectEqual(@as(usize, 1), state.freed_pending_frames);
    try std.testing.expect(state.abort_reason == null);
}
