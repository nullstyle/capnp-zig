const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");

pub const join_state = @import("./provide/peer_join_state.zig");
pub const provides_state = @import("./provide/peer_provides_state.zig");
pub const orchestration = @import("./provide/peer_provide_join_orchestration.zig");
pub const embargo_accepts = @import("./peer_embargo_accepts.zig");

pub const captureProvideRecipientForPeer = orchestration.captureProvideRecipientForPeer;
pub const captureProvideRecipientForPeerFn = orchestration.captureProvideRecipientForPeerFn;
pub const captureAcceptProvisionForPeer = orchestration.captureAcceptProvisionForPeer;
pub const captureAcceptProvisionForPeerFn = orchestration.captureAcceptProvisionForPeerFn;

pub const hasProvideQuestion = provides_state.hasProvideQuestion;
pub const hasProvideRecipient = provides_state.hasProvideRecipient;
pub const putProvideByQuestion = provides_state.putProvideByQuestion;
pub const putProvideByKey = provides_state.putProvideByKey;
pub const getProvidedQuestion = provides_state.getProvidedQuestion;
pub const getProvidedTarget = provides_state.getProvidedTarget;
pub const clearProvide = provides_state.clearProvide;
pub const clearProvideForPeer = provides_state.clearProvideForPeer;
pub const clearProvideForPeerFn = provides_state.clearProvideForPeerFn;

pub const InsertOutcome = join_state.InsertOutcome;
pub const parseJoinKeyPart = join_state.parseJoinKeyPart;
pub const insertJoinPart = join_state.insertJoinPart;
pub const clearPendingJoinQuestion = join_state.clearPendingJoinQuestion;
pub const clearPendingJoinQuestionForPeer = join_state.clearPendingJoinQuestionForPeer;
pub const clearPendingJoinQuestionForPeerFn = join_state.clearPendingJoinQuestionForPeerFn;
pub const completeJoin = join_state.completeJoin;
pub const completeJoinForPeer = join_state.completeJoinForPeer;
pub const completeJoinForPeerFn = join_state.completeJoinForPeerFn;

pub const queueEmbargoedAccept = embargo_accepts.queueEmbargoedAccept;
pub const queueEmbargoedAcceptForPeer = embargo_accepts.queueEmbargoedAcceptForPeer;
pub const queueEmbargoedAcceptForPeerFn = embargo_accepts.queueEmbargoedAcceptForPeerFn;
pub const clearPendingAcceptQuestion = embargo_accepts.clearPendingAcceptQuestion;
pub const clearPendingAcceptQuestionForPeer = embargo_accepts.clearPendingAcceptQuestionForPeer;
pub const clearPendingAcceptQuestionForPeerFn = embargo_accepts.clearPendingAcceptQuestionForPeerFn;
pub const releaseEmbargoedAccepts = embargo_accepts.releaseEmbargoedAccepts;
pub const releaseEmbargoedAcceptsForPeer = embargo_accepts.releaseEmbargoedAcceptsForPeer;
pub const releaseEmbargoedAcceptsForPeerFn = embargo_accepts.releaseEmbargoedAcceptsForPeerFn;

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
