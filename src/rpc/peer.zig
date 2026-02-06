const std = @import("std");
const protocol = @import("protocol.zig");
const cap_table = @import("cap_table.zig");
const Connection = @import("connection.zig").Connection;
const message = @import("../message.zig");

pub const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
pub const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
pub const CallHandler = *const fn (ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void;
pub const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
pub const SendFrameOverride = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;

pub const Export = struct {
    ctx: *anyopaque,
    on_call: CallHandler,
};

const ExportEntry = struct {
    handler: ?Export = null,
    ref_count: u32,
    is_promise: bool = false,
    resolved: ?cap_table.ResolvedCap = null,
};

const ResolvedAnswer = struct {
    frame: []u8,
};

const PendingCall = struct {
    frame: []u8,
    caps: cap_table.InboundCapTable,
};

const StoredPromisedAnswer = struct {
    question_id: u32,
    ops: []protocol.PromisedAnswerOp,

    fn fromPromised(allocator: std.mem.Allocator, promised: protocol.PromisedAnswer) !StoredPromisedAnswer {
        const op_count = promised.transform.len();
        const copied_ops = try allocator.alloc(protocol.PromisedAnswerOp, op_count);
        errdefer allocator.free(copied_ops);

        var idx: u32 = 0;
        while (idx < op_count) : (idx += 1) {
            copied_ops[idx] = try promised.transform.get(idx);
        }

        return .{
            .question_id = promised.question_id,
            .ops = copied_ops,
        };
    }

    fn deinit(self: StoredPromisedAnswer, allocator: std.mem.Allocator) void {
        allocator.free(self.ops);
    }
};

const ProvideTarget = union(enum) {
    cap_id: u32,
    promised: StoredPromisedAnswer,

    fn deinit(self: *ProvideTarget, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .promised => |promised| promised.deinit(allocator),
            else => {},
        }
    }
};

const ProvideEntry = struct {
    recipient_key: []u8,
    target: ProvideTarget,
};

const JoinKeyPart = struct {
    join_id: u32,
    part_count: u16,
    part_num: u16,
};

const JoinPartEntry = struct {
    question_id: u32,
    target: ProvideTarget,
};

const JoinState = struct {
    part_count: u16,
    parts: std.AutoHashMap(u16, JoinPartEntry),

    fn init(allocator: std.mem.Allocator, part_count: u16) JoinState {
        return .{
            .part_count = part_count,
            .parts = std.AutoHashMap(u16, JoinPartEntry).init(allocator),
        };
    }

    fn deinit(self: *JoinState, allocator: std.mem.Allocator) void {
        var it = self.parts.valueIterator();
        while (it.next()) |part| {
            var target = part.target;
            target.deinit(allocator);
        }
        self.parts.deinit();
    }
};

const PendingJoinQuestion = struct {
    join_id: u32,
    part_num: u16,
};

const PendingEmbargoedAccept = struct {
    answer_id: u32,
    provided_question_id: u32,
};

const ForwardReturnMode = enum {
    translate_to_caller,
    sent_elsewhere,
    propagate_results_sent_elsewhere,
    propagate_accept_from_third_party,
};

const ResolvedImport = struct {
    cap: ?cap_table.ResolvedCap,
    embargo_id: ?u32 = null,
    embargoed: bool = false,
};

const Question = struct {
    ctx: *anyopaque,
    on_return: QuestionCallback,
    is_loopback: bool = false,
    suppress_auto_finish: bool = false,
};

const PendingThirdPartyAwait = struct {
    question_id: u32,
    question: Question,
};

const ForwardCallContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
    send_results_to: protocol.SendResultsToTag,
    send_results_to_third_party_payload: ?[]u8 = null,
    answer_id: u32,
    mode: ForwardReturnMode,
};

const ForwardReturnBuildContext = struct {
    peer: *Peer,
    payload: protocol.Payload,
    inbound_caps: *const cap_table.InboundCapTable,
};

pub const Peer = struct {
    allocator: std.mem.Allocator,
    conn: *Connection,
    caps: cap_table.CapTable,
    exports: std.AutoHashMap(u32, ExportEntry),
    questions: std.AutoHashMap(u32, Question),
    resolved_answers: std.AutoHashMap(u32, ResolvedAnswer),
    pending_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),
    pending_export_promises: std.AutoHashMap(u32, std.ArrayList(PendingCall)),
    forwarded_questions: std.AutoHashMap(u32, u32),
    forwarded_tail_questions: std.AutoHashMap(u32, u32),
    provides_by_question: std.AutoHashMap(u32, ProvideEntry),
    provides_by_key: std.StringHashMap(u32),
    pending_joins: std.AutoHashMap(u32, JoinState),
    pending_join_questions: std.AutoHashMap(u32, PendingJoinQuestion),
    pending_accepts_by_embargo: std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)),
    pending_accept_embargo_by_question: std.AutoHashMap(u32, []u8),
    pending_third_party_awaits: std.StringHashMap(PendingThirdPartyAwait),
    pending_third_party_answers: std.StringHashMap(u32),
    pending_third_party_returns: std.AutoHashMap(u32, []u8),
    adopted_third_party_answers: std.AutoHashMap(u32, u32),
    resolved_imports: std.AutoHashMap(u32, ResolvedImport),
    pending_embargoes: std.AutoHashMap(u32, u32),
    loopback_questions: std.AutoHashMap(u32, void),
    send_results_to_yourself: std.AutoHashMap(u32, void),
    send_results_to_third_party: std.AutoHashMap(u32, ?[]u8),
    next_question_id: u32 = 0,
    next_embargo_id: u32 = 0,
    bootstrap_export_id: ?u32 = null,
    send_frame_ctx: ?*anyopaque = null,
    send_frame_override: ?SendFrameOverride = null,
    on_error: ?*const fn (peer: *Peer, err: anyerror) void = null,
    on_close: ?*const fn (peer: *Peer) void = null,
    last_inbound_tag: ?protocol.MessageTag = null,
    last_remote_abort_reason: ?[]u8 = null,

    pub fn init(allocator: std.mem.Allocator, conn: *Connection) Peer {
        return .{
            .allocator = allocator,
            .conn = conn,
            .caps = cap_table.CapTable.init(allocator),
            .exports = std.AutoHashMap(u32, ExportEntry).init(allocator),
            .questions = std.AutoHashMap(u32, Question).init(allocator),
            .resolved_answers = std.AutoHashMap(u32, ResolvedAnswer).init(allocator),
            .pending_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .pending_export_promises = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(allocator),
            .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            .forwarded_tail_questions = std.AutoHashMap(u32, u32).init(allocator),
            .provides_by_question = std.AutoHashMap(u32, ProvideEntry).init(allocator),
            .provides_by_key = std.StringHashMap(u32).init(allocator),
            .pending_joins = std.AutoHashMap(u32, JoinState).init(allocator),
            .pending_join_questions = std.AutoHashMap(u32, PendingJoinQuestion).init(allocator),
            .pending_accepts_by_embargo = std.StringHashMap(std.ArrayList(PendingEmbargoedAccept)).init(allocator),
            .pending_accept_embargo_by_question = std.AutoHashMap(u32, []u8).init(allocator),
            .pending_third_party_awaits = std.StringHashMap(PendingThirdPartyAwait).init(allocator),
            .pending_third_party_answers = std.StringHashMap(u32).init(allocator),
            .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            .adopted_third_party_answers = std.AutoHashMap(u32, u32).init(allocator),
            .resolved_imports = std.AutoHashMap(u32, ResolvedImport).init(allocator),
            .pending_embargoes = std.AutoHashMap(u32, u32).init(allocator),
            .loopback_questions = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_yourself = std.AutoHashMap(u32, void).init(allocator),
            .send_results_to_third_party = std.AutoHashMap(u32, ?[]u8).init(allocator),
        };
    }

    pub fn deinit(self: *Peer) void {
        var it = self.pending_promises.valueIterator();
        while (it.next()) |list| {
            for (list.items) |*pending| {
                pending.caps.deinit();
                self.allocator.free(pending.frame);
            }
            list.deinit(self.allocator);
        }
        var export_it = self.pending_export_promises.valueIterator();
        while (export_it.next()) |list| {
            for (list.items) |*pending| {
                pending.caps.deinit();
                self.allocator.free(pending.frame);
            }
            list.deinit(self.allocator);
        }
        var answer_it = self.resolved_answers.valueIterator();
        while (answer_it.next()) |answer| {
            self.allocator.free(answer.frame);
        }
        self.questions.deinit();
        self.exports.deinit();
        self.resolved_answers.deinit();
        self.pending_promises.deinit();
        self.pending_export_promises.deinit();
        self.forwarded_questions.deinit();
        self.forwarded_tail_questions.deinit();
        var provide_it = self.provides_by_question.valueIterator();
        while (provide_it.next()) |entry| {
            self.allocator.free(entry.recipient_key);
            entry.target.deinit(self.allocator);
        }
        self.provides_by_question.deinit();
        self.provides_by_key.deinit();

        var pending_join_it = self.pending_joins.valueIterator();
        while (pending_join_it.next()) |join_state| {
            join_state.deinit(self.allocator);
        }
        self.pending_joins.deinit();
        self.pending_join_questions.deinit();

        var pending_it = self.pending_accepts_by_embargo.iterator();
        while (pending_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.pending_accepts_by_embargo.deinit();

        var pending_question_it = self.pending_accept_embargo_by_question.valueIterator();
        while (pending_question_it.next()) |embargo_key| {
            self.allocator.free(embargo_key.*);
        }
        self.pending_accept_embargo_by_question.deinit();

        var pending_third_await_it = self.pending_third_party_awaits.iterator();
        while (pending_third_await_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.pending_third_party_awaits.deinit();

        var pending_third_answer_it = self.pending_third_party_answers.iterator();
        while (pending_third_answer_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.pending_third_party_answers.deinit();

        var pending_third_return_it = self.pending_third_party_returns.valueIterator();
        while (pending_third_return_it.next()) |frame| {
            self.allocator.free(frame.*);
        }
        self.pending_third_party_returns.deinit();
        self.adopted_third_party_answers.deinit();

        self.resolved_imports.deinit();
        self.pending_embargoes.deinit();
        self.loopback_questions.deinit();
        self.send_results_to_yourself.deinit();
        var third_party_it = self.send_results_to_third_party.valueIterator();
        while (third_party_it.next()) |entry| {
            if (entry.*) |payload| self.allocator.free(payload);
        }
        self.send_results_to_third_party.deinit();
        if (self.last_remote_abort_reason) |reason| {
            self.allocator.free(reason);
            self.last_remote_abort_reason = null;
        }
        self.caps.deinit();
    }

    pub fn start(
        self: *Peer,
        on_error: ?*const fn (peer: *Peer, err: anyerror) void,
        on_close: ?*const fn (peer: *Peer) void,
    ) void {
        self.on_error = on_error;
        self.on_close = on_close;
        self.conn.start(self, onConnectionMessage, onConnectionError, onConnectionClose);
    }

    pub fn setSendFrameOverride(self: *Peer, ctx: ?*anyopaque, callback: ?SendFrameOverride) void {
        self.send_frame_ctx = ctx;
        self.send_frame_override = callback;
    }

    pub fn getLastInboundTag(self: *const Peer) ?protocol.MessageTag {
        return self.last_inbound_tag;
    }

    pub fn getLastRemoteAbortReason(self: *const Peer) ?[]const u8 {
        return self.last_remote_abort_reason;
    }

    pub fn addExport(self: *Peer, exported: Export) !u32 {
        const id = self.caps.allocExportId();
        try self.exports.put(id, .{
            .handler = exported,
            .ref_count = 0,
            .is_promise = false,
            .resolved = null,
        });
        return id;
    }

    pub fn addPromiseExport(self: *Peer) !u32 {
        const id = self.caps.allocExportId();
        try self.exports.put(id, .{
            .handler = null,
            .ref_count = 0,
            .is_promise = true,
            .resolved = null,
        });
        try self.caps.markExportPromise(id);
        return id;
    }

    pub fn setBootstrap(self: *Peer, exported: Export) !u32 {
        const id = try self.addExport(exported);
        self.bootstrap_export_id = id;
        return id;
    }

    pub fn sendBootstrap(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        const question_id = try self.allocateQuestion(ctx, on_return);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        try builder.buildBootstrap(question_id);
        try self.sendBuilder(&builder);
        return question_id;
    }

    pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        const descriptor = protocol.CapDescriptor{
            .tag = if (self.caps.isExportPromise(export_id)) .sender_promise else .sender_hosted,
            .id = export_id,
            .promised_answer = null,
            .attached_fd = null,
        };
        try self.sendResolveCap(promise_id, descriptor);

        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        try self.sendResolveException(promise_id, reason);
        promise_entry.value_ptr.resolved = .none;
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, .none);
    }

    pub fn sendCall(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        if (self.resolved_imports.get(target_id)) |entry| {
            if (!entry.embargoed and entry.cap != null) {
                return self.sendCallResolved(entry.cap.?, interface_id, method_id, ctx, build, on_return);
            }
        }

        return self.sendCallToImport(target_id, interface_id, method_id, ctx, build, on_return);
    }

    pub fn sendCallResolved(
        self: *Peer,
        target: cap_table.ResolvedCap,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        return switch (target) {
            .imported => |cap| self.sendCallToImport(cap.id, interface_id, method_id, ctx, build, on_return),
            .promised => |promised| self.sendCallPromised(promised, interface_id, method_id, ctx, build, on_return),
            .exported => |cap| self.sendCallToExport(cap.id, interface_id, method_id, ctx, build, on_return),
            .none => error.CapabilityUnavailable,
        };
    }

    fn sendCallToImport(
        self: *Peer,
        target_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        const question_id = try self.allocateQuestion(ctx, on_return);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var call = try builder.beginCall(question_id, interface_id, method_id);
        try call.setTargetImportedCap(target_id);

        if (build) |build_fn| {
            try build_fn(ctx, &call);
        }

        try cap_table.encodeCallPayloadCaps(&self.caps, &call, self, onOutboundCap);
        try self.sendBuilder(&builder);
        return question_id;
    }

    fn sendCallToExport(
        self: *Peer,
        export_id: u32,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        const question_id = try self.allocateQuestion(ctx, on_return);
        errdefer _ = self.questions.remove(question_id);
        if (self.questions.getEntry(question_id)) |question| {
            question.value_ptr.is_loopback = true;
        }

        try self.loopback_questions.put(question_id, {});
        errdefer _ = self.loopback_questions.remove(question_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var call = try builder.beginCall(question_id, interface_id, method_id);
        try call.setTargetImportedCap(export_id);

        if (build) |build_fn| {
            try build_fn(ctx, &call);
        }

        try cap_table.encodeCallPayloadCaps(&self.caps, &call, self, onOutboundCap);
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);
        try self.handleFrame(bytes);
        return question_id;
    }

    pub fn sendCallPromised(
        self: *Peer,
        promised: protocol.PromisedAnswer,
        interface_id: u64,
        method_id: u16,
        ctx: *anyopaque,
        build: ?CallBuildFn,
        on_return: QuestionCallback,
    ) !u32 {
        const question_id = try self.allocateQuestion(ctx, on_return);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var call = try builder.beginCall(question_id, interface_id, method_id);
        try call.setTargetPromisedAnswerFrom(promised);

        if (build) |build_fn| {
            try build_fn(ctx, &call);
        }

        try cap_table.encodeCallPayloadCaps(&self.caps, &call, self, onOutboundCap);
        try self.sendBuilder(&builder);
        return question_id;
    }

    fn forwardResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        target: cap_table.ResolvedCap,
        mode: ForwardReturnMode,
    ) !void {
        const ctx = try self.allocator.create(ForwardCallContext);
        errdefer self.allocator.destroy(ctx);

        ctx.* = .{
            .peer = self,
            .payload = call.params,
            .inbound_caps = inbound_caps,
            .send_results_to = switch (mode) {
                .translate_to_caller => .caller,
                .sent_elsewhere, .propagate_results_sent_elsewhere => .yourself,
                .propagate_accept_from_third_party => .third_party,
            },
            .send_results_to_third_party_payload = if (mode == .propagate_accept_from_third_party) try captureAnyPointerPayload(
                self.allocator,
                call.send_results_to.third_party,
            ) else null,
            .answer_id = call.question_id,
            .mode = mode,
        };

        const forwarded_question_id = try self.sendCallResolved(
            target,
            call.interface_id,
            call.method_id,
            ctx,
            buildForwardedCall,
            onForwardedReturn,
        );
        try self.forwarded_questions.put(forwarded_question_id, call.question_id);

        if (mode == .sent_elsewhere) {
            try self.forwarded_tail_questions.put(call.question_id, forwarded_question_id);
            if (self.questions.getEntry(forwarded_question_id)) |question| {
                question.value_ptr.suppress_auto_finish = true;
            }
            try self.sendReturnTakeFromOtherQuestion(call.question_id, forwarded_question_id);
        }
    }

    fn buildForwardedCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
        switch (ctx.send_results_to) {
            .caller => call_builder.setSendResultsToCaller(),
            .yourself => call_builder.setSendResultsToYourself(),
            .third_party => {
                if (ctx.send_results_to_third_party_payload) |payload| {
                    var msg = try message.Message.init(ctx.peer.allocator, payload);
                    defer msg.deinit();
                    const third_party = try msg.getRootAnyPointer();
                    try call_builder.setSendResultsToThirdParty(third_party);
                } else {
                    try call_builder.setSendResultsToThirdPartyNull();
                }
            },
        }

        const payload_builder = try call_builder.payloadBuilder();
        try ctx.peer.clonePayloadWithRemappedCaps(
            call_builder.call.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn onForwardedReturn(
        ctx_ptr: *anyopaque,
        peer: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        const ctx: *ForwardCallContext = @ptrCast(@alignCast(ctx_ptr));
        defer {
            if (ctx.send_results_to_third_party_payload) |payload| peer.allocator.free(payload);
            peer.allocator.destroy(ctx);
        }
        _ = peer.forwarded_questions.remove(ret.answer_id);

        switch (ctx.mode) {
            .translate_to_caller => switch (ret.tag) {
                .results => {
                    const payload = ret.results orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded return missing payload");
                        return;
                    };
                    var build_ctx = ForwardReturnBuildContext{
                        .peer = peer,
                        .payload = payload,
                        .inbound_caps = inbound_caps,
                    };
                    peer.sendReturnResults(ctx.answer_id, &build_ctx, buildForwardedReturn) catch |err| {
                        try peer.sendReturnException(ctx.answer_id, @errorName(err));
                    };
                },
                .exception => {
                    const ex = ret.exception orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded return missing exception");
                        return;
                    };
                    try peer.sendReturnException(ctx.answer_id, ex.reason);
                },
                .canceled => try peer.sendReturnTag(ctx.answer_id, .canceled),
                .results_sent_elsewhere => {
                    try peer.sendReturnException(ctx.answer_id, "forwarded resultsSentElsewhere unsupported");
                },
                .take_from_other_question => {
                    const other_local_id = ret.take_from_other_question orelse return error.MissingQuestionId;
                    const translated = peer.forwarded_questions.get(other_local_id) orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded takeFromOtherQuestion missing mapping");
                        return;
                    };
                    try peer.sendReturnTakeFromOtherQuestion(ctx.answer_id, translated);
                },
                .accept_from_third_party => {
                    const await_ptr = ret.accept_from_third_party;
                    const await_payload = try captureAnyPointerPayload(peer.allocator, await_ptr);
                    defer if (await_payload) |payload| peer.allocator.free(payload);
                    try peer.sendReturnAcceptFromThirdParty(ctx.answer_id, await_payload);
                },
            },
            .sent_elsewhere => switch (ret.tag) {
                .results_sent_elsewhere, .canceled => {},
                else => return error.UnexpectedForwardedTailReturn,
            },
            .propagate_results_sent_elsewhere => switch (ret.tag) {
                .results_sent_elsewhere => try peer.sendReturnTag(ctx.answer_id, .results_sent_elsewhere),
                .canceled => try peer.sendReturnTag(ctx.answer_id, .canceled),
                .exception => {
                    const ex = ret.exception orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded return missing exception");
                        return;
                    };
                    try peer.sendReturnException(ctx.answer_id, ex.reason);
                },
                .take_from_other_question => {
                    try peer.sendReturnException(ctx.answer_id, "forwarded takeFromOtherQuestion unsupported");
                },
                .accept_from_third_party => {
                    try peer.sendReturnTag(ctx.answer_id, .results_sent_elsewhere);
                },
                .results => {
                    // For `sendResultsTo.yourself`, successful results should not be forwarded directly.
                    try peer.sendReturnTag(ctx.answer_id, .results_sent_elsewhere);
                },
            },
            .propagate_accept_from_third_party => switch (ret.tag) {
                .results_sent_elsewhere => {
                    try peer.sendReturnAcceptFromThirdParty(ctx.answer_id, ctx.send_results_to_third_party_payload);
                },
                .accept_from_third_party => {
                    const await_ptr = ret.accept_from_third_party;
                    const await_payload = try captureAnyPointerPayload(peer.allocator, await_ptr);
                    defer if (await_payload) |payload| peer.allocator.free(payload);
                    try peer.sendReturnAcceptFromThirdParty(ctx.answer_id, await_payload);
                },
                .canceled => try peer.sendReturnTag(ctx.answer_id, .canceled),
                .exception => {
                    const ex = ret.exception orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded return missing exception");
                        return;
                    };
                    try peer.sendReturnException(ctx.answer_id, ex.reason);
                },
                .take_from_other_question => {
                    try peer.sendReturnException(ctx.answer_id, "forwarded takeFromOtherQuestion unsupported");
                },
                .results => {
                    const payload = ret.results orelse {
                        try peer.sendReturnException(ctx.answer_id, "forwarded return missing payload");
                        return;
                    };
                    var build_ctx = ForwardReturnBuildContext{
                        .peer = peer,
                        .payload = payload,
                        .inbound_caps = inbound_caps,
                    };
                    peer.sendReturnResults(ctx.answer_id, &build_ctx, buildForwardedReturn) catch |err| {
                        try peer.sendReturnException(ctx.answer_id, @errorName(err));
                    };
                },
            },
        }
    }

    fn buildForwardedReturn(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *const ForwardReturnBuildContext = @ptrCast(@alignCast(ctx_ptr));
        const payload_builder = try ret_builder.payloadBuilder();
        try ctx.peer.clonePayloadWithRemappedCaps(
            ret_builder.ret.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn clonePayloadWithRemappedCaps(
        self: *Peer,
        builder: *message.MessageBuilder,
        payload_builder: message.StructBuilder,
        source: protocol.Payload,
        inbound_caps: *const cap_table.InboundCapTable,
    ) !void {
        const any_builder = try payload_builder.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
        try message.cloneAnyPointer(source.content, any_builder);
        try self.remapPayloadCapabilities(builder, any_builder, inbound_caps);
    }

    fn remapPayloadCapabilities(
        self: *Peer,
        builder: *message.MessageBuilder,
        root: message.AnyPointerBuilder,
        inbound_caps: *const cap_table.InboundCapTable,
    ) !void {
        const view = try buildMessageView(self.allocator, builder);
        defer self.allocator.free(view.segments);

        if (root.segment_id >= view.msg.segments.len) return error.InvalidSegmentId;
        const segment = view.msg.segments[root.segment_id];
        if (root.pointer_pos + 8 > segment.len) return error.OutOfBounds;
        const root_word = std.mem.readInt(u64, segment[root.pointer_pos..][0..8], .little);
        try self.remapPayloadCapabilityPointer(&view.msg, builder, inbound_caps, root.segment_id, root.pointer_pos, root_word);
    }

    fn remapPayloadCapabilityPointer(
        self: *Peer,
        msg: *const message.Message,
        builder: *message.MessageBuilder,
        inbound_caps: *const cap_table.InboundCapTable,
        segment_id: u32,
        pointer_pos: usize,
        pointer_word: u64,
    ) !void {
        if (pointer_word == 0) return;
        const resolved = try msg.resolvePointer(segment_id, pointer_pos, pointer_word, 8);
        if (resolved.pointer_word == 0) return;

        const pointer_type: u2 = @truncate(resolved.pointer_word & 0x3);
        switch (pointer_type) {
            0 => {
                const struct_reader = try msg.resolveStructPointer(
                    resolved.segment_id,
                    resolved.pointer_pos,
                    resolved.pointer_word,
                );
                const pointer_base = struct_reader.offset + @as(usize, struct_reader.data_size) * 8;
                var idx: usize = 0;
                while (idx < struct_reader.pointer_count) : (idx += 1) {
                    const child_pos = pointer_base + idx * 8;
                    const child_word = std.mem.readInt(
                        u64,
                        msg.segments[struct_reader.segment_id][child_pos..][0..8],
                        .little,
                    );
                    try self.remapPayloadCapabilityPointer(
                        msg,
                        builder,
                        inbound_caps,
                        struct_reader.segment_id,
                        child_pos,
                        child_word,
                    );
                }
            },
            1 => {
                const list = try msg.resolveListPointer(
                    resolved.segment_id,
                    resolved.pointer_pos,
                    resolved.pointer_word,
                );
                if (list.element_size == 6) {
                    var idx: u32 = 0;
                    while (idx < list.element_count) : (idx += 1) {
                        const child_pos = list.content_offset + @as(usize, idx) * 8;
                        const child_word = std.mem.readInt(
                            u64,
                            msg.segments[list.segment_id][child_pos..][0..8],
                            .little,
                        );
                        try self.remapPayloadCapabilityPointer(
                            msg,
                            builder,
                            inbound_caps,
                            list.segment_id,
                            child_pos,
                            child_word,
                        );
                    }
                } else if (list.element_size == 7) {
                    const inline_list = try msg.resolveInlineCompositeList(
                        resolved.segment_id,
                        resolved.pointer_pos,
                        resolved.pointer_word,
                    );
                    const stride = (@as(usize, inline_list.data_words) + @as(usize, inline_list.pointer_words)) * 8;
                    var elem_idx: u32 = 0;
                    while (elem_idx < inline_list.element_count) : (elem_idx += 1) {
                        const element_offset = inline_list.elements_offset + @as(usize, elem_idx) * stride;
                        const pointer_base = element_offset + @as(usize, inline_list.data_words) * 8;
                        var pointer_idx: usize = 0;
                        while (pointer_idx < inline_list.pointer_words) : (pointer_idx += 1) {
                            const child_pos = pointer_base + pointer_idx * 8;
                            const child_word = std.mem.readInt(
                                u64,
                                msg.segments[inline_list.segment_id][child_pos..][0..8],
                                .little,
                            );
                            try self.remapPayloadCapabilityPointer(
                                msg,
                                builder,
                                inbound_caps,
                                inline_list.segment_id,
                                child_pos,
                                child_word,
                            );
                        }
                    }
                }
            },
            3 => {
                const cap_index = try decodeCapabilityPointerWord(resolved.pointer_word);
                if (try self.mapInboundCapForForward(inbound_caps, cap_index)) |cap_id| {
                    const cap_word = try capabilityPointerWord(cap_id);
                    try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, cap_word);
                } else {
                    try writePointerWord(builder, resolved.segment_id, resolved.pointer_pos, 0);
                }
            },
            else => return error.InvalidPointer,
        }
    }

    fn mapInboundCapForForward(
        self: *Peer,
        inbound_caps: *const cap_table.InboundCapTable,
        cap_index: u32,
    ) !?u32 {
        const entry = try inbound_caps.get(cap_index);
        return switch (entry) {
            .none => null,
            .imported => |cap| cap.id,
            .exported => |cap| cap.id,
            .promised => |promised| try self.caps.noteReceiverAnswer(promised),
        };
    }

    fn capabilityPointerWord(cap_id: u32) !u64 {
        if (cap_id >= (@as(u32, 1) << 30)) return error.CapabilityIdTooLarge;
        return 3 | (@as(u64, cap_id) << 2);
    }

    fn decodeCapabilityPointerWord(pointer_word: u64) !u32 {
        if ((pointer_word & 0x3) != 3) return error.InvalidPointer;
        if ((pointer_word >> 32) != 0) return error.InvalidPointer;
        return @as(u32, @intCast((pointer_word >> 2) & 0x3FFFFFFF));
    }

    fn writePointerWord(builder: *message.MessageBuilder, segment_id: u32, pointer_pos: usize, word: u64) !void {
        if (segment_id >= builder.segments.items.len) return error.InvalidSegmentId;
        var segment = &builder.segments.items[segment_id];
        if (pointer_pos + 8 > segment.items.len) return error.OutOfBounds;
        std.mem.writeInt(u64, segment.items[pointer_pos..][0..8], word, .little);
    }

    fn buildMessageView(
        allocator: std.mem.Allocator,
        builder: *message.MessageBuilder,
    ) !struct { msg: message.Message, segments: []const []const u8 } {
        const segment_count = builder.segments.items.len;
        const segments = try allocator.alloc([]const u8, segment_count);
        errdefer allocator.free(segments);

        for (builder.segments.items, 0..) |segment, idx| {
            segments[idx] = segment.items;
        }

        const msg = message.Message{
            .allocator = allocator,
            .segments = segments,
            .segments_owned = false,
            .backing_data = null,
        };
        return .{ .msg = msg, .segments = segments };
    }

    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            _ = self.send_results_to_yourself.remove(answer_id);
            defer if (entry.value) |payload| self.allocator.free(payload);
            try self.sendReturnAcceptFromThirdParty(answer_id, entry.value);
            return;
        }

        if (self.send_results_to_yourself.remove(answer_id)) {
            try self.sendReturnTag(answer_id, .results_sent_elsewhere);
            return;
        }

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(answer_id, .results);
        try build(ctx, &ret);
        _ = try cap_table.encodeReturnPayloadCaps(&self.caps, &ret, self, onOutboundCap);

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        if (self.loopback_questions.remove(answer_id)) {
            try self.deliverLoopbackReturn(bytes);
            return;
        }
        try self.sendFrame(bytes);

        const copy = try self.allocator.alloc(u8, bytes.len);
        std.mem.copyForwards(u8, copy, bytes);
        try self.recordResolvedAnswer(answer_id, copy);
    }

    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        _ = self.send_results_to_yourself.remove(answer_id);
        self.clearSendResultsToThirdParty(answer_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(answer_id, .exception);
        try ret.setException(reason);
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        if (self.loopback_questions.remove(answer_id)) {
            try self.deliverLoopbackReturn(bytes);
            return;
        }
        try self.sendFrame(bytes);
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        _ = self.send_results_to_yourself.remove(answer_id);
        self.clearSendResultsToThirdParty(answer_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        _ = try builder.beginReturn(answer_id, tag);
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        if (self.loopback_questions.remove(answer_id)) {
            try self.deliverLoopbackReturn(bytes);
            return;
        }
        try self.sendFrame(bytes);
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        _ = self.send_results_to_yourself.remove(answer_id);
        self.clearSendResultsToThirdParty(answer_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(answer_id, .take_from_other_question);
        try ret.setTakeFromOtherQuestion(other_question_id);

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        if (self.loopback_questions.remove(answer_id)) {
            try self.deliverLoopbackReturn(bytes);
            return;
        }
        try self.sendFrame(bytes);
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        _ = self.send_results_to_yourself.remove(answer_id);
        self.clearSendResultsToThirdParty(answer_id);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(answer_id, .accept_from_third_party);
        if (await_payload) |payload| {
            var await_msg = try message.Message.init(self.allocator, payload);
            defer await_msg.deinit();
            const await_ptr = try await_msg.getRootAnyPointer();
            try ret.setAcceptFromThirdParty(await_ptr);
        } else {
            try ret.setAcceptFromThirdPartyNull();
        }

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        if (self.loopback_questions.remove(answer_id)) {
            try self.deliverLoopbackReturn(bytes);
            return;
        }
        try self.sendFrame(bytes);
    }

    fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        const BuildCtx = struct {
            peer: *Peer,
            target: *const ProvideTarget,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
                var any = try ret.getResultsAnyPointer();

                const cap_id = switch (ctx.target.*) {
                    .cap_id => |id| id,
                    .promised => |promised| try ctx.peer.caps.noteReceiverAnswerOps(
                        promised.question_id,
                        promised.ops,
                    ),
                };
                try any.setCapability(.{ .id = cap_id });
            }
        };

        var ctx = BuildCtx{
            .peer = self,
            .target = target,
        };
        try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
    }

    fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
        if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
            if (entry.value) |payload| self.allocator.free(payload);
        }
    }

    fn clearProvide(self: *Peer, question_id: u32) void {
        if (self.provides_by_question.fetchRemove(question_id)) |removed| {
            _ = self.provides_by_key.remove(removed.value.recipient_key);
            self.allocator.free(removed.value.recipient_key);

            var target = removed.value.target;
            target.deinit(self.allocator);
        }
    }

    fn parseJoinKeyPart(join_key_part: ?message.AnyPointerReader) !JoinKeyPart {
        const key_part_ptr = join_key_part orelse return error.MissingJoinKeyPart;
        if (key_part_ptr.isNull()) return error.MissingJoinKeyPart;

        const key_struct = key_part_ptr.getStruct() catch return error.InvalidJoinKeyPart;
        const part_count = key_struct.readU16(4);
        const part_num = key_struct.readU16(6);

        if (part_count == 0 or part_num >= part_count) return error.InvalidJoinKeyPart;
        return .{
            .join_id = key_struct.readU32(0),
            .part_count = part_count,
            .part_num = part_num,
        };
    }

    fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
        return switch (a.*) {
            .cap_id => |cap_id| switch (b.*) {
                .cap_id => |other_cap_id| cap_id == other_cap_id,
                else => false,
            },
            .promised => |promised| switch (b.*) {
                .promised => |other_promised| blk: {
                    if (promised.question_id != other_promised.question_id) break :blk false;
                    if (promised.ops.len != other_promised.ops.len) break :blk false;
                    for (promised.ops, 0..) |op, idx| {
                        const other = other_promised.ops[idx];
                        if (op.tag != other.tag or op.pointer_index != other.pointer_index) break :blk false;
                    }
                    break :blk true;
                },
                else => false,
            },
        };
    }

    fn clearPendingJoinQuestion(self: *Peer, question_id: u32) void {
        const pending_question = self.pending_join_questions.fetchRemove(question_id) orelse return;
        const key = pending_question.value;

        var remove_state = false;
        if (self.pending_joins.getPtr(key.join_id)) |join_state| {
            if (join_state.parts.fetchRemove(key.part_num)) |removed_part| {
                var target = removed_part.value.target;
                target.deinit(self.allocator);
            }
            remove_state = join_state.parts.count() == 0;
        }

        if (remove_state) {
            if (self.pending_joins.fetchRemove(key.join_id)) |removed_state| {
                var state = removed_state.value;
                state.deinit(self.allocator);
            }
        }
    }

    fn completeJoin(self: *Peer, join_id: u32) !void {
        const removed = self.pending_joins.fetchRemove(join_id) orelse return;
        var join_state = removed.value;
        defer join_state.deinit(self.allocator);

        if (join_state.parts.count() == 0) return;

        var first_target: ?*const ProvideTarget = null;
        var all_equal = true;

        var part_it = join_state.parts.iterator();
        while (part_it.next()) |entry| {
            if (first_target) |target| {
                if (!provideTargetsEqual(target, &entry.value_ptr.target)) {
                    all_equal = false;
                    break;
                }
            } else {
                first_target = &entry.value_ptr.target;
            }
        }

        var send_it = join_state.parts.iterator();
        while (send_it.next()) |entry| {
            _ = self.pending_join_questions.remove(entry.value_ptr.question_id);

            if (all_equal) {
                const target = first_target orelse &entry.value_ptr.target;
                self.sendReturnProvidedTarget(entry.value_ptr.question_id, target) catch |err| {
                    try self.sendReturnException(entry.value_ptr.question_id, @errorName(err));
                };
            } else {
                try self.sendReturnException(entry.value_ptr.question_id, "join target mismatch");
            }
        }
    }

    fn queueEmbargoedAccept(self: *Peer, answer_id: u32, provided_question_id: u32, embargo: []const u8) !void {
        const embargo_copy = try self.allocator.alloc(u8, embargo.len);
        errdefer self.allocator.free(embargo_copy);
        std.mem.copyForwards(u8, embargo_copy, embargo);

        if (self.pending_accepts_by_embargo.getPtr(embargo)) |pending| {
            try pending.append(self.allocator, .{
                .answer_id = answer_id,
                .provided_question_id = provided_question_id,
            });
        } else {
            const key = try self.allocator.alloc(u8, embargo.len);
            errdefer self.allocator.free(key);
            std.mem.copyForwards(u8, key, embargo);

            var pending = std.ArrayList(PendingEmbargoedAccept){};
            errdefer pending.deinit(self.allocator);
            try pending.append(self.allocator, .{
                .answer_id = answer_id,
                .provided_question_id = provided_question_id,
            });
            try self.pending_accepts_by_embargo.put(key, pending);
        }

        try self.pending_accept_embargo_by_question.put(answer_id, embargo_copy);
    }

    fn clearPendingAcceptQuestion(self: *Peer, question_id: u32) void {
        const embargo_entry = self.pending_accept_embargo_by_question.fetchRemove(question_id) orelse return;
        const embargo_key = embargo_entry.value;
        defer self.allocator.free(embargo_key);

        if (self.pending_accepts_by_embargo.getEntry(embargo_key)) |entry| {
            const pending = entry.value_ptr;
            var idx: usize = 0;
            while (idx < pending.items.len) : (idx += 1) {
                if (pending.items[idx].answer_id == question_id) {
                    _ = pending.swapRemove(idx);
                    break;
                }
            }

            if (pending.items.len == 0) {
                if (self.pending_accepts_by_embargo.fetchRemove(embargo_key)) |removed| {
                    self.allocator.free(removed.key);
                    var removed_list = removed.value;
                    removed_list.deinit(self.allocator);
                }
            }
        }
    }

    fn releaseEmbargoedAccepts(self: *Peer, embargo: []const u8) !void {
        const pending_entry = self.pending_accepts_by_embargo.fetchRemove(embargo) orelse return;
        var pending_list = pending_entry.value;
        defer {
            self.allocator.free(pending_entry.key);
            pending_list.deinit(self.allocator);
        }

        for (pending_list.items) |pending| {
            if (self.pending_accept_embargo_by_question.fetchRemove(pending.answer_id)) |embargo_key| {
                self.allocator.free(embargo_key.value);
            }

            const provided = self.provides_by_question.getPtr(pending.provided_question_id) orelse {
                try self.sendReturnException(pending.answer_id, "unknown provision");
                continue;
            };

            self.sendReturnProvidedTarget(pending.answer_id, &provided.target) catch |err| {
                try self.sendReturnException(pending.answer_id, @errorName(err));
            };
        }
    }

    fn captureAnyPointerPayload(
        allocator: std.mem.Allocator,
        ptr: ?message.AnyPointerReader,
    ) !?[]u8 {
        const any = ptr orelse return null;
        if (any.isNull()) return null;

        var builder = message.MessageBuilder.init(allocator);
        defer builder.deinit();

        const root = try builder.initRootAnyPointer();
        try message.cloneAnyPointer(any, root);

        const bytes = try builder.toBytes();
        return @constCast(bytes);
    }

    fn noteSendResultsToYourself(self: *Peer, answer_id: u32) !void {
        self.clearSendResultsToThirdParty(answer_id);
        _ = try self.send_results_to_yourself.getOrPut(answer_id);
    }

    fn noteSendResultsToThirdParty(
        self: *Peer,
        answer_id: u32,
        ptr: ?message.AnyPointerReader,
    ) !void {
        _ = self.send_results_to_yourself.remove(answer_id);

        const payload = try captureAnyPointerPayload(self.allocator, ptr);
        errdefer if (payload) |bytes| self.allocator.free(bytes);

        const entry = try self.send_results_to_third_party.getOrPut(answer_id);
        if (entry.found_existing) {
            if (entry.value_ptr.*) |existing| self.allocator.free(existing);
        }
        entry.value_ptr.* = payload;
    }

    pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
        if (count == 0) return;
        var remaining = count;
        var removed = false;
        while (remaining > 0) : (remaining -= 1) {
            if (self.caps.releaseImport(import_id)) removed = true;
        }
        if (removed) {
            try self.releaseResolvedImport(import_id);
        }
        try self.sendRelease(import_id, count);
    }

    fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
        const bytes = try builder.finish();
        defer self.allocator.free(bytes);
        try self.sendFrame(bytes);
    }

    fn sendFrame(self: *Peer, frame: []const u8) !void {
        if (self.send_frame_override) |cb| {
            const ctx = self.send_frame_ctx orelse return error.MissingCallbackContext;
            try cb(ctx, frame);
            return;
        }
        try self.conn.sendFrame(frame);
    }

    fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        const peer: *Peer = @ptrCast(@alignCast(ctx));
        switch (tag) {
            .sender_hosted, .sender_promise => try peer.noteExportRef(id),
            else => {},
        }
    }

    fn noteExportRef(self: *Peer, id: u32) !void {
        var entry = self.exports.getEntry(id) orelse return error.UnknownExport;
        entry.value_ptr.ref_count +%= 1;
    }

    fn releaseExport(self: *Peer, id: u32, count: u32) void {
        if (count == 0) return;
        var entry = self.exports.getEntry(id) orelse return;
        if (entry.value_ptr.ref_count <= count) {
            _ = self.exports.remove(id);
            self.caps.clearExportPromise(id);
            if (self.pending_export_promises.fetchRemove(id)) |removed| {
                var pending = removed.value;
                for (pending.items) |*pending_call| {
                    pending_call.caps.deinit();
                    self.allocator.free(pending_call.frame);
                }
                pending.deinit(self.allocator);
            }
        } else {
            entry.value_ptr.ref_count -= count;
        }
    }

    fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        var releases = try self.collectReleaseCounts(inbound);
        defer releases.deinit();

        var it = releases.iterator();
        while (it.next()) |entry| {
            try self.sendRelease(entry.key_ptr.*, entry.value_ptr.*);
        }
    }

    fn collectReleaseCounts(self: *Peer, inbound: *cap_table.InboundCapTable) !std.AutoHashMap(u32, u32) {
        var releases = std.AutoHashMap(u32, u32).init(self.allocator);
        errdefer releases.deinit();

        var idx: u32 = 0;
        while (idx < inbound.len()) : (idx += 1) {
            if (inbound.isRetained(idx)) continue;
            const entry = try inbound.get(idx);
            switch (entry) {
                .imported => |cap| {
                    const removed = self.caps.releaseImport(cap.id);
                    if (removed) {
                        try self.releaseResolvedImport(cap.id);
                    }
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

    fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        if (self.resolved_imports.fetchRemove(promise_id)) |existing| {
            if (existing.value.embargo_id) |id| {
                _ = self.pending_embargoes.remove(id);
            }
            if (existing.value.cap) |old_cap| {
                try self.releaseResolvedCap(old_cap);
            }
        }
        try self.resolved_imports.put(promise_id, .{
            .cap = cap,
            .embargo_id = embargo_id,
            .embargoed = embargoed,
        });
    }

    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        if (self.resolved_imports.fetchRemove(promise_id)) |existing| {
            if (existing.value.embargo_id) |id| {
                _ = self.pending_embargoes.remove(id);
            }
            if (existing.value.cap) |resolved| {
                try self.releaseResolvedCap(resolved);
            }
        }
    }

    fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        switch (resolved) {
            .imported => |cap| try self.releaseImport(cap.id, 1),
            else => {},
        }
    }

    fn sendRelease(self: *Peer, import_id: u32, count: u32) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildRelease(import_id, count);
        try self.sendBuilder(&builder);
    }

    fn sendFinish(self: *Peer, question_id: u32, release_result_caps: bool) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildFinish(question_id, release_result_caps, false);
        try self.sendBuilder(&builder);
    }

    fn sendResolveCap(self: *Peer, promise_id: u32, descriptor: protocol.CapDescriptor) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildResolveCap(promise_id, descriptor);
        try self.sendBuilder(&builder);
    }

    fn sendResolveException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildResolveException(promise_id, reason);
        try self.sendBuilder(&builder);
    }

    fn sendAbort(self: *Peer, reason: []const u8) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildAbort(reason);
        try self.sendBuilder(&builder);
    }

    fn sendDisembargoSenderLoopback(self: *Peer, target: protocol.MessageTarget, embargo_id: u32) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildDisembargoSenderLoopback(target, embargo_id);
        try self.sendBuilder(&builder);
    }

    fn sendDisembargoReceiverLoopback(self: *Peer, target: protocol.MessageTarget, embargo_id: u32) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildDisembargoReceiverLoopback(target, embargo_id);
        try self.sendBuilder(&builder);
    }

    fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .return_) return error.UnexpectedMessage;
        try self.handleReturn(frame, try decoded.asReturn());
    }

    fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        const entry = self.resolved_answers.get(promised.question_id) orelse return error.PromiseUnresolved;
        var decoded = try protocol.DecodedMessage.init(self.allocator, entry.frame);
        defer decoded.deinit();
        const ret = try decoded.asReturn();
        if (ret.tag != .results or ret.results == null) return error.PromisedAnswerMissing;
        return cap_table.resolvePromisedAnswer(ret.results.?, promised.transform);
    }

    fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
        var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
        defer decoded.deinit();
        if (decoded.tag != .return_) return;
        const ret = try decoded.asReturn();
        if (ret.tag != .results or ret.results == null) return;
        const cap_list = ret.results.?.cap_table orelse return;
        var idx: u32 = 0;
        while (idx < cap_list.len()) : (idx += 1) {
            const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(idx));
            switch (desc.tag) {
                .sender_hosted, .sender_promise => {
                    if (desc.id) |id| {
                        self.releaseExport(id, 1);
                    }
                },
                else => {},
            }
        }
    }

    fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
        const id = self.next_question_id;
        self.next_question_id +%= 1;
        try self.questions.put(id, .{
            .ctx = ctx,
            .on_return = on_return,
            .is_loopback = false,
        });
        return id;
    }

    fn onConnectionMessage(conn: *Connection, frame: []const u8) anyerror!void {
        const peer = peerFromConnection(conn);
        try peer.handleFrame(frame);
    }

    fn onConnectionError(conn: *Connection, err: anyerror) void {
        const peer = peerFromConnection(conn);
        if (peer.on_error) |cb| cb(peer, err);
    }

    fn onConnectionClose(conn: *Connection) void {
        const peer = peerFromConnection(conn);
        if (peer.on_close) |cb| cb(peer);
    }

    fn peerFromConnection(conn: *Connection) *Peer {
        return @ptrCast(@alignCast(conn.ctx.?));
    }

    pub fn handleFrame(self: *Peer, frame: []const u8) !void {
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch |err| {
            if (err == error.InvalidMessageTag) {
                try self.sendUnimplementedForFrame(frame);
                return;
            }
            return err;
        };
        defer decoded.deinit();
        self.last_inbound_tag = decoded.tag;

        switch (decoded.tag) {
            .unimplemented => try self.handleUnimplemented(try decoded.asUnimplemented()),
            .abort => try self.handleAbort(try decoded.asAbort()),
            .bootstrap => try self.handleBootstrap(try decoded.asBootstrap()),
            .call => try self.handleCall(frame, try decoded.asCall()),
            .return_ => try self.handleReturn(frame, try decoded.asReturn()),
            .finish => try self.handleFinish(try decoded.asFinish()),
            .release => try self.handleRelease(try decoded.asRelease()),
            .resolve => try self.handleResolve(try decoded.asResolve()),
            .disembargo => try self.handleDisembargo(try decoded.asDisembargo()),
            .provide => try self.handleProvide(try decoded.asProvide()),
            .accept => try self.handleAccept(try decoded.asAccept()),
            .join => try self.handleJoin(try decoded.asJoin()),
            .third_party_answer => try self.handleThirdPartyAnswer(try decoded.asThirdPartyAnswer()),
            else => {
                const root = try decoded.msg.getRootAnyPointer();
                try self.sendUnimplemented(root);
            },
        }
    }

    fn sendUnimplemented(self: *Peer, original: message.AnyPointerReader) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildUnimplementedFromAnyPointer(original);
        try self.sendBuilder(&builder);
    }

    fn sendUnimplementedForFrame(self: *Peer, frame: []const u8) !void {
        var msg = try message.Message.init(self.allocator, frame);
        defer msg.deinit();
        const root = try msg.getRootAnyPointer();
        try self.sendUnimplemented(root);
    }

    fn handleUnimplemented(self: *Peer, unimplemented: protocol.Unimplemented) !void {
        const tag = unimplemented.message_tag orelse return;
        const question_id = unimplemented.question_id orelse return;
        switch (tag) {
            .bootstrap, .call => try self.handleUnimplementedQuestion(question_id),
            else => {},
        }
    }

    fn handleUnimplementedQuestion(self: *Peer, question_id: u32) !void {
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
        self.handleReturn(&.{}, ret) catch |err| switch (err) {
            error.UnknownQuestion => {},
            else => return err,
        };
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        if (self.last_remote_abort_reason) |existing| {
            self.allocator.free(existing);
            self.last_remote_abort_reason = null;
        }
        const reason_copy = try self.allocator.alloc(u8, abort.exception.reason.len);
        std.mem.copyForwards(u8, reason_copy, abort.exception.reason);
        self.last_remote_abort_reason = reason_copy;
        return error.RemoteAbort;
    }

    fn handleBootstrap(self: *Peer, bootstrap: protocol.Bootstrap) !void {
        const export_id = self.bootstrap_export_id orelse {
            try self.sendReturnException(bootstrap.question_id, "bootstrap not configured");
            return;
        };

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret = try builder.beginReturn(bootstrap.question_id, .results);
        var any = try ret.getResultsAnyPointer();
        try any.setCapability(.{ .id = 0 });

        var cap_list = try ret.initCapTable(1);
        const entry = try cap_list.get(0);
        protocol.CapDescriptor.writeSenderHosted(entry, export_id);
        try self.noteExportRef(export_id);

        try self.sendBuilder(&builder);
    }

    fn handleFinish(self: *Peer, finish: protocol.Finish) !void {
        _ = self.send_results_to_yourself.remove(finish.question_id);
        self.clearSendResultsToThirdParty(finish.question_id);
        self.clearProvide(finish.question_id);
        self.clearPendingJoinQuestion(finish.question_id);
        self.clearPendingAcceptQuestion(finish.question_id);

        if (self.forwarded_tail_questions.fetchRemove(finish.question_id)) |tail| {
            try self.sendFinish(tail.value, false);
        }

        if (self.resolved_answers.fetchRemove(finish.question_id)) |entry| {
            defer self.allocator.free(entry.value.frame);
            if (finish.release_result_caps) {
                try self.releaseResultCaps(entry.value.frame);
            }
        }
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        self.releaseExport(release.id, release.reference_count);
    }

    fn handleResolve(self: *Peer, resolve: protocol.Resolve) !void {
        const promise_id = resolve.promise_id;
        const known_promise = self.caps.imports.contains(promise_id);

        switch (resolve.tag) {
            .cap => {
                const descriptor = resolve.cap orelse return error.MissingResolveCap;
                const resolved = try cap_table.resolveCapDescriptor(&self.caps, descriptor);

                if (!known_promise) {
                    try self.releaseResolvedCap(resolved);
                    return;
                }

                var embargo_id: ?u32 = null;
                var embargoed = false;
                if (resolved == .exported or resolved == .promised) {
                    embargo_id = self.next_embargo_id;
                    self.next_embargo_id +%= 1;
                    embargoed = true;
                    try self.pending_embargoes.put(embargo_id.?, promise_id);
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
                    try self.sendDisembargoSenderLoopback(target, embargo_id.?);
                }

                try self.storeResolvedImport(promise_id, resolved, embargo_id, embargoed);
            },
            .exception => {
                if (!known_promise) return;
                try self.storeResolvedImport(promise_id, null, null, false);
            },
        }
    }

    fn handleDisembargo(self: *Peer, disembargo: protocol.Disembargo) !void {
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
                try self.sendDisembargoReceiverLoopback(disembargo.target, embargo_id);
            },
            .receiver_loopback => {
                const embargo_id = disembargo.embargo_id orelse return error.MissingEmbargoId;
                const entry = self.pending_embargoes.fetchRemove(embargo_id) orelse return;
                if (self.resolved_imports.getEntry(entry.value)) |resolved| {
                    resolved.value_ptr.embargoed = false;
                    resolved.value_ptr.embargo_id = null;
                }
            },
            .accept => {
                const accept_embargo = disembargo.accept orelse return;
                try self.releaseEmbargoedAccepts(accept_embargo);
            },
        }
    }

    fn resolveProvideTarget(self: *Peer, target: protocol.MessageTarget) !cap_table.ResolvedCap {
        return switch (target.tag) {
            .imported_cap => {
                const export_id = target.imported_cap orelse return error.MissingCallTarget;
                const exported_entry = self.exports.getEntry(export_id) orelse return error.UnknownExport;

                if (exported_entry.value_ptr.is_promise) {
                    const resolved = exported_entry.value_ptr.resolved orelse return error.PromiseUnresolved;
                    if (resolved == .none) return error.PromiseBroken;
                    return resolved;
                }
                return .{ .exported = .{ .id = export_id } };
            },
            .promised_answer => {
                const promised = target.promised_answer orelse return error.MissingPromisedAnswer;
                const resolved = try self.resolvePromisedAnswer(promised);
                if (resolved == .none) return error.PromisedAnswerMissing;
                return resolved;
            },
        };
    }

    fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
        return switch (resolved) {
            .none => error.PromisedAnswerMissing,
            .imported => |cap| .{ .cap_id = cap.id },
            .exported => |cap| .{ .cap_id = cap.id },
            .promised => |promised| .{
                .promised = try StoredPromisedAnswer.fromPromised(self.allocator, promised),
            },
        };
    }

    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        const key = try captureAnyPointerPayload(self.allocator, provide.recipient);
        const key_bytes = key orelse {
            try self.sendAbort("provide missing recipient");
            return error.MissingThirdPartyPayload;
        };
        errdefer self.allocator.free(key_bytes);

        if (self.provides_by_question.contains(provide.question_id)) {
            try self.sendAbort("duplicate provide question");
            return error.DuplicateProvideQuestionId;
        }
        if (self.provides_by_key.contains(key_bytes)) {
            try self.sendAbort("duplicate provide recipient");
            return error.DuplicateProvideRecipient;
        }

        const resolved = self.resolveProvideTarget(provide.target) catch |err| {
            try self.sendAbort(@errorName(err));
            return err;
        };
        const target = try self.makeProvideTarget(resolved);
        errdefer {
            var cleanup = target;
            cleanup.deinit(self.allocator);
        }

        try self.provides_by_question.put(provide.question_id, .{
            .recipient_key = key_bytes,
            .target = target,
        });
        errdefer self.clearProvide(provide.question_id);
        try self.provides_by_key.put(key_bytes, provide.question_id);
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        const key = try captureAnyPointerPayload(self.allocator, accept.provision);
        defer if (key) |bytes| self.allocator.free(bytes);
        const key_bytes = key orelse {
            try self.sendReturnException(accept.question_id, "unknown provision");
            return;
        };

        const provided_question_id = self.provides_by_key.get(key_bytes) orelse {
            try self.sendReturnException(accept.question_id, "unknown provision");
            return;
        };
        const entry = self.provides_by_question.getPtr(provided_question_id) orelse {
            try self.sendReturnException(accept.question_id, "unknown provision");
            return;
        };

        if (accept.embargo) |embargo| {
            try self.queueEmbargoedAccept(accept.question_id, provided_question_id, embargo);
            return;
        }

        self.sendReturnProvidedTarget(accept.question_id, &entry.target) catch |err| {
            try self.sendReturnException(accept.question_id, @errorName(err));
        };
    }

    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        if (self.pending_join_questions.contains(join.question_id)) {
            try self.sendAbort("duplicate join question");
            return error.DuplicateJoinQuestionId;
        }

        const join_key_part = parseJoinKeyPart(join.key_part) catch |err| {
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        const resolved = self.resolveProvideTarget(join.target) catch |err| {
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        const target = self.makeProvideTarget(resolved) catch |err| {
            try self.sendReturnException(join.question_id, @errorName(err));
            return;
        };

        const join_entry = try self.pending_joins.getOrPut(join_key_part.join_id);
        if (!join_entry.found_existing) {
            join_entry.value_ptr.* = JoinState.init(self.allocator, join_key_part.part_count);
        } else if (join_entry.value_ptr.part_count != join_key_part.part_count) {
            var cleanup = target;
            cleanup.deinit(self.allocator);
            try self.sendReturnException(join.question_id, "join partCount mismatch");
            return;
        }

        if (join_entry.value_ptr.parts.contains(join_key_part.part_num)) {
            var cleanup = target;
            cleanup.deinit(self.allocator);
            try self.sendReturnException(join.question_id, "duplicate join part");
            return;
        }

        try join_entry.value_ptr.parts.put(join_key_part.part_num, .{
            .question_id = join.question_id,
            .target = target,
        });
        errdefer {
            if (join_entry.value_ptr.parts.fetchRemove(join_key_part.part_num)) |removed| {
                var cleanup = removed.value.target;
                cleanup.deinit(self.allocator);
            }
        }
        try self.pending_join_questions.put(join.question_id, .{
            .join_id = join_key_part.join_id,
            .part_num = join_key_part.part_num,
        });
        errdefer _ = self.pending_join_questions.remove(join.question_id);

        if (join_entry.value_ptr.parts.count() == join_key_part.part_count) {
            try self.completeJoin(join_key_part.join_id);
        }
    }

    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        if (!isThirdPartyAnswerId(third_party_answer.answer_id)) {
            try self.sendAbort("invalid thirdPartyAnswer answerId");
            return error.InvalidThirdPartyAnswerId;
        }

        const completion_payload = try captureAnyPointerPayload(self.allocator, third_party_answer.completion);
        const completion_key = completion_payload orelse {
            try self.sendAbort("thirdPartyAnswer missing completion");
            return error.MissingThirdPartyPayload;
        };
        errdefer self.allocator.free(completion_key);

        if (self.pending_third_party_awaits.fetchRemove(completion_key)) |await_entry| {
            defer self.allocator.free(await_entry.key);
            self.allocator.free(completion_key);
            try self.adoptThirdPartyAnswer(
                await_entry.value.question_id,
                third_party_answer.answer_id,
                await_entry.value.question,
            );
            return;
        }

        if (self.pending_third_party_answers.get(completion_key)) |existing_id| {
            if (existing_id == third_party_answer.answer_id) {
                self.allocator.free(completion_key);
                return;
            }
            try self.sendAbort("conflicting thirdPartyAnswer completion");
            return error.ConflictingThirdPartyAnswer;
        }

        try self.pending_third_party_answers.put(completion_key, third_party_answer.answer_id);
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        switch (call.target.tag) {
            .imported_cap => {
                var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, call.params.cap_table, &self.caps);
                const export_id = call.target.imported_cap orelse return error.MissingCallTarget;
                const exported_entry = self.exports.getEntry(export_id) orelse {
                    try self.sendReturnException(call.question_id, "unknown capability");
                    inbound_caps.deinit();
                    return;
                };

                if (exported_entry.value_ptr.is_promise) {
                    const resolved = exported_entry.value_ptr.resolved;
                    if (resolved == null) {
                        try self.queuePromiseExportCall(export_id, frame, inbound_caps);
                        return;
                    }

                    defer inbound_caps.deinit();
                    defer self.releaseInboundCaps(&inbound_caps) catch |err| {
                        if (self.on_error) |cb| cb(self, err);
                    };

                    if (resolved.? == .none) {
                        try self.sendReturnException(call.question_id, "promise broken");
                        return;
                    }
                    try self.handleResolvedCall(call, &inbound_caps, resolved.?);
                    return;
                }

                defer inbound_caps.deinit();
                defer self.releaseInboundCaps(&inbound_caps) catch |err| {
                    if (self.on_error) |cb| cb(self, err);
                };

                switch (call.send_results_to.tag) {
                    .caller => {},
                    .yourself => {
                        try self.noteSendResultsToYourself(call.question_id);
                    },
                    .third_party => {
                        try self.noteSendResultsToThirdParty(call.question_id, call.send_results_to.third_party);
                    },
                }

                const handler = exported_entry.value_ptr.handler orelse {
                    try self.sendReturnException(call.question_id, "missing export handler");
                    return;
                };
                handler.on_call(handler.ctx, self, call, &inbound_caps) catch |err| {
                    try self.sendReturnException(call.question_id, @errorName(err));
                    return;
                };
            },
            .promised_answer => {
                const promised = call.target.promised_answer orelse return error.MissingCallTarget;
                var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, call.params.cap_table, &self.caps);
                const resolved = self.resolvePromisedAnswer(promised) catch |err| {
                    if (err == error.PromiseUnresolved) {
                        try self.queuePromisedCall(promised.question_id, frame, inbound_caps);
                        return;
                    }
                    inbound_caps.deinit();
                    try self.sendReturnException(call.question_id, @errorName(err));
                    return;
                };

                if (resolved == .exported) {
                    const export_id = resolved.exported.id;
                    if (self.exports.getEntry(export_id)) |entry| {
                        if (entry.value_ptr.is_promise and entry.value_ptr.resolved == null) {
                            try self.queuePromiseExportCall(export_id, frame, inbound_caps);
                            return;
                        }
                    }
                }

                defer inbound_caps.deinit();
                defer self.releaseInboundCaps(&inbound_caps) catch |err| {
                    if (self.on_error) |cb| cb(self, err);
                };
                try self.handleResolvedCall(call, &inbound_caps, resolved);
            },
        }
    }

    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        switch (resolved) {
            .exported => |cap| {
                switch (call.send_results_to.tag) {
                    .caller => {},
                    .yourself => {
                        try self.noteSendResultsToYourself(call.question_id);
                    },
                    .third_party => {
                        try self.noteSendResultsToThirdParty(call.question_id, call.send_results_to.third_party);
                    },
                }

                const exported_entry = self.exports.getEntry(cap.id) orelse {
                    try self.sendReturnException(call.question_id, "unknown promised capability");
                    return;
                };

                if (exported_entry.value_ptr.is_promise) {
                    const next = exported_entry.value_ptr.resolved orelse {
                        try self.sendReturnException(call.question_id, "promised capability unresolved");
                        return;
                    };
                    if (next == .none) {
                        try self.sendReturnException(call.question_id, "promise broken");
                        return;
                    }
                    try self.handleResolvedCall(call, inbound_caps, next);
                    return;
                }

                const handler = exported_entry.value_ptr.handler orelse {
                    try self.sendReturnException(call.question_id, "missing promised capability handler");
                    return;
                };
                handler.on_call(handler.ctx, self, call, inbound_caps) catch |err| {
                    try self.sendReturnException(call.question_id, @errorName(err));
                };
            },
            .imported => {
                const mode = switch (call.send_results_to.tag) {
                    .caller => ForwardReturnMode.sent_elsewhere,
                    .yourself => ForwardReturnMode.propagate_results_sent_elsewhere,
                    .third_party => ForwardReturnMode.propagate_accept_from_third_party,
                };
                self.forwardResolvedCall(call, inbound_caps, resolved, mode) catch |err| {
                    try self.sendReturnException(call.question_id, @errorName(err));
                };
            },
            .promised => {
                const mode = switch (call.send_results_to.tag) {
                    .caller => ForwardReturnMode.sent_elsewhere,
                    .yourself => ForwardReturnMode.propagate_results_sent_elsewhere,
                    .third_party => ForwardReturnMode.propagate_accept_from_third_party,
                };
                self.forwardResolvedCall(call, inbound_caps, resolved, mode) catch |err| {
                    try self.sendReturnException(call.question_id, @errorName(err));
                };
            },
            .none => {
                try self.sendReturnException(call.question_id, "promised answer missing");
            },
        }
    }

    fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
        if (self.resolved_answers.fetchRemove(question_id)) |existing| {
            self.allocator.free(existing.value.frame);
        }
        _ = try self.resolved_answers.put(question_id, .{ .frame = frame });

        var pending = self.pending_promises.fetchRemove(question_id) orelse return;
        defer pending.value.deinit(self.allocator);

        for (pending.value.items) |*pending_call| {
            defer pending_call.caps.deinit();
            defer self.allocator.free(pending_call.frame);

            var decoded = try protocol.DecodedMessage.init(self.allocator, pending_call.frame);
            defer decoded.deinit();
            if (decoded.tag != .call) continue;
            const call = try decoded.asCall();
            const promised = call.target.promised_answer orelse continue;
            const resolved = self.resolvePromisedAnswer(promised) catch |err| {
                try self.sendReturnException(call.question_id, @errorName(err));
                continue;
            };
            self.handleResolvedCall(call, &pending_call.caps, resolved) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
            self.releaseInboundCaps(&pending_call.caps) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
        }
    }

    fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        const copy = try self.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);

        var entry = try self.pending_promises.getOrPut(question_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(PendingCall){};
        }
        try entry.value_ptr.append(self.allocator, .{ .frame = copy, .caps = inbound_caps });
    }

    fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        const copy = try self.allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, copy, frame);

        var entry = try self.pending_export_promises.getOrPut(export_id);
        if (!entry.found_existing) {
            entry.value_ptr.* = std.ArrayList(PendingCall){};
        }
        try entry.value_ptr.append(self.allocator, .{ .frame = copy, .caps = inbound_caps });
    }

    fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        var pending = self.pending_export_promises.fetchRemove(export_id) orelse return;
        defer pending.value.deinit(self.allocator);

        for (pending.value.items) |*pending_call| {
            defer pending_call.caps.deinit();
            defer self.allocator.free(pending_call.frame);

            var decoded = try protocol.DecodedMessage.init(self.allocator, pending_call.frame);
            defer decoded.deinit();
            if (decoded.tag != .call) continue;
            const call = try decoded.asCall();

            if (resolved == .none) {
                try self.sendReturnException(call.question_id, "promise broken");
            } else {
                self.handleResolvedCall(call, &pending_call.caps, resolved) catch |err| {
                    if (self.on_error) |cb| cb(self, err);
                };
            }

            self.releaseInboundCaps(&pending_call.caps) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
        }
    }

    fn isThirdPartyAnswerId(answer_id: u32) bool {
        return (answer_id & 0x4000_0000) != 0 and (answer_id & 0x8000_0000) == 0;
    }

    fn adoptThirdPartyAnswer(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) anyerror!void {
        if (!isThirdPartyAnswerId(adopted_answer_id)) {
            try self.sendAbort("invalid thirdPartyAnswer answerId");
            return error.InvalidThirdPartyAnswerId;
        }
        if (self.questions.contains(adopted_answer_id) or self.adopted_third_party_answers.contains(adopted_answer_id)) {
            try self.sendAbort("duplicate thirdPartyAnswer answerId");
            return error.DuplicateThirdPartyAnswerId;
        }

        try self.questions.put(adopted_answer_id, question);
        errdefer _ = self.questions.remove(adopted_answer_id);
        try self.adopted_third_party_answers.put(adopted_answer_id, question_id);
        errdefer _ = self.adopted_third_party_answers.remove(adopted_answer_id);

        if (self.pending_third_party_returns.fetchRemove(adopted_answer_id)) |pending| {
            defer self.allocator.free(pending.value);
            var decoded = try protocol.DecodedMessage.init(self.allocator, pending.value);
            defer decoded.deinit();
            if (decoded.tag != .return_) return error.UnexpectedMessage;
            try self.handleReturn(pending.value, try decoded.asReturn());
        }
    }

    fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        const entry = self.questions.fetchRemove(ret.answer_id) orelse {
            if (isThirdPartyAnswerId(ret.answer_id)) {
                if (self.pending_third_party_returns.contains(ret.answer_id)) {
                    return error.DuplicateThirdPartyReturn;
                }
                const copy = try self.allocator.alloc(u8, frame.len);
                errdefer self.allocator.free(copy);
                std.mem.copyForwards(u8, copy, frame);
                try self.pending_third_party_returns.put(ret.answer_id, copy);
                return;
            }
            return error.UnknownQuestion;
        };

        const cap_list = if (ret.tag == .results and ret.results != null) ret.results.?.cap_table else null;
        var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, cap_list, &self.caps);
        defer inbound_caps.deinit();

        if (ret.tag == .accept_from_third_party) {
            const completion_payload = try captureAnyPointerPayload(self.allocator, ret.accept_from_third_party);
            const completion_key = completion_payload orelse return error.MissingThirdPartyPayload;
            errdefer self.allocator.free(completion_key);

            if (self.pending_third_party_awaits.contains(completion_key)) {
                try self.sendAbort("duplicate awaitFromThirdParty completion");
                return error.DuplicateThirdPartyAwait;
            }

            if (self.pending_third_party_answers.fetchRemove(completion_key)) |pending_answer| {
                self.allocator.free(pending_answer.key);
                self.allocator.free(completion_key);
                try self.adoptThirdPartyAnswer(ret.answer_id, pending_answer.value, entry.value);
            } else {
                try self.pending_third_party_awaits.put(completion_key, .{
                    .question_id = ret.answer_id,
                    .question = entry.value,
                });
            }

            if (!entry.value.is_loopback and !entry.value.suppress_auto_finish and !ret.no_finish_needed) {
                self.sendFinish(ret.answer_id, false) catch |err| {
                    if (self.on_error) |cb| cb(self, err);
                };
            }
            return;
        }

        var callback_ret = ret;
        if (self.adopted_third_party_answers.fetchRemove(ret.answer_id)) |original| {
            callback_ret.answer_id = original.value;
        }

        entry.value.on_return(entry.value.ctx, self, callback_ret, &inbound_caps) catch |err| {
            if (self.on_error) |cb| cb(self, err);
        };

        if (ret.tag == .results and ret.results != null) {
            self.releaseInboundCaps(&inbound_caps) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
        }

        if (!entry.value.is_loopback and !entry.value.suppress_auto_finish and !ret.no_finish_needed) {
            self.sendFinish(ret.answer_id, false) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
        }
    }
};

test "release batching aggregates per import id" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    try peer.caps.noteImport(5);
    try peer.caps.noteImport(5);
    try peer.caps.noteImport(7);
    try peer.caps.noteImport(9);

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 4),
        .retained = try allocator.alloc(bool, 4),
    };
    defer inbound.deinit();

    inbound.entries[0] = .{ .imported = .{ .id = 5 } };
    inbound.entries[1] = .{ .imported = .{ .id = 5 } };
    inbound.entries[2] = .{ .imported = .{ .id = 7 } };
    inbound.entries[3] = .{ .imported = .{ .id = 9 } };
    @memset(inbound.retained, false);
    inbound.retained[3] = true;

    var releases = try peer.collectReleaseCounts(&inbound);
    defer releases.deinit();

    try std.testing.expectEqual(@as(usize, 2), releases.count());
    try std.testing.expectEqual(@as(u32, 2), releases.get(5).?);
    try std.testing.expectEqual(@as(u32, 1), releases.get(7).?);
    try std.testing.expectEqual(@as(usize, 1), peer.caps.imports.count());
    try std.testing.expect(peer.caps.imports.contains(9));
}

test "sendCallResolved routes exported target through local loopback" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
    };
    const Handlers = struct {
        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = @ptrCast(@alignCast(ctx));
            server.called = true;
            try peer.sendReturnException(call.question_id, "loopback");
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = @ptrCast(@alignCast(ctx));
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("loopback", ex.reason);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &client_ctx,
        null,
        Handlers.onReturn,
    );

    try std.testing.expect(server_ctx.called);
    try std.testing.expect(client_ctx.returned);
}

test "forwarded payload remaps capability index to local id" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{ .imported = .{ .id = 42 } };
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    var src_payload = try src_call.payloadBuilder();
    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    const dst_payload = try dst_call.payloadBuilder();
    try peer.clonePayloadWithRemappedCaps(
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    const cap = try parsed_dst_call.params.content.getCapability();
    try std.testing.expectEqual(@as(u32, 42), cap.id);
}

test "forwarded payload converts none capability to null pointer" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .none;
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    var src_payload = try src_call.payloadBuilder();
    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    const dst_payload = try dst_call.payloadBuilder();
    try peer.clonePayloadWithRemappedCaps(
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    try std.testing.expect(parsed_dst_call.params.content.isNull());
}

test "forwarded payload encodes promised capability descriptors as receiverAnswer" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var inbound = cap_table.InboundCapTable{
        .allocator = allocator,
        .entries = try allocator.alloc(cap_table.ResolvedCap, 1),
        .retained = try allocator.alloc(bool, 1),
    };
    defer inbound.deinit();
    inbound.entries[0] = .{
        .promised = .{
            .question_id = 9,
            .transform = .{ .list = null },
        },
    };
    inbound.retained[0] = false;

    var src_builder = protocol.MessageBuilder.init(allocator);
    defer src_builder.deinit();
    var src_call = try src_builder.beginCall(1, 0x01, 0x02);
    var src_payload = try src_call.payloadBuilder();
    var src_any = try src_payload.getAnyPointer(protocol.PAYLOAD_CONTENT_PTR);
    try src_any.setCapability(.{ .id = 0 });

    const src_bytes = try src_builder.finish();
    defer allocator.free(src_bytes);
    var src_decoded = try protocol.DecodedMessage.init(allocator, src_bytes);
    defer src_decoded.deinit();
    const parsed_src_call = try src_decoded.asCall();

    var dst_builder = protocol.MessageBuilder.init(allocator);
    defer dst_builder.deinit();
    var dst_call = try dst_builder.beginCall(7, 0x03, 0x04);
    const dst_payload = try dst_call.payloadBuilder();
    try peer.clonePayloadWithRemappedCaps(
        dst_call.call.builder,
        dst_payload,
        parsed_src_call.params,
        &inbound,
    );
    try cap_table.encodeCallPayloadCaps(&peer.caps, &dst_call, null, null);

    const dst_bytes = try dst_builder.finish();
    defer allocator.free(dst_bytes);
    var dst_decoded = try protocol.DecodedMessage.init(allocator, dst_bytes);
    defer dst_decoded.deinit();
    const parsed_dst_call = try dst_decoded.asCall();
    const cap = try parsed_dst_call.params.content.getCapability();
    try std.testing.expectEqual(@as(u32, 0), cap.id);

    const cap_table_reader = parsed_dst_call.params.cap_table orelse return error.MissingCapTable;
    const desc = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(0));
    try std.testing.expectEqual(protocol.CapDescriptorTag.receiver_answer, desc.tag);
    const promised = desc.promised_answer orelse return error.MissingPromisedAnswer;
    try std.testing.expectEqual(@as(u32, 9), promised.question_id);
    try std.testing.expectEqual(@as(u32, 0), promised.transform.len());
    try std.testing.expectEqual(@as(usize, 0), peer.caps.receiver_answers.count());
}

test "forwarded return passes through canceled tag" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const upstream_answer_id: u32 = 55;
    const local_forwarded_question_id: u32 = 99;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .canceled,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return translates takeFromOtherQuestion id" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        referenced_answer: u32 = 0,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, ret.tag);
            state.referenced_answer = ret.take_from_other_question orelse return error.MissingQuestionId;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const upstream_answer_id: u32 = 100;
    const local_forwarded_question_id: u32 = 200;
    const local_referenced_question_id: u32 = 201;
    const translated_upstream_answer_id: u32 = 77;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});

    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);
    try peer.forwarded_questions.put(local_referenced_question_id, translated_upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .take_from_other_question,
        .results = null,
        .exception = null,
        .take_from_other_question = local_referenced_question_id,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(translated_upstream_answer_id, callback_ctx.referenced_answer);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return converts resultsSentElsewhere to exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded resultsSentElsewhere unsupported", ex.reason);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const upstream_answer_id: u32 = 300;
    const local_forwarded_question_id: u32 = 301;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results_sent_elsewhere,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return forwards awaitFromThirdParty to caller" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            try std.testing.expect(ret.exception == null);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const upstream_answer_id: u32 = 400;
    const local_forwarded_question_id: u32 = 401;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .caller,
        .answer_id = upstream_answer_id,
        .mode = .translate_to_caller,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .accept_from_third_party,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return sentElsewhere mode accepts resultsSentElsewhere without upstream return" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = ret;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const upstream_answer_id: u32 = 500;
    const local_forwarded_question_id: u32 = 501;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(upstream_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(upstream_answer_id, {});
    try peer.forwarded_questions.put(local_forwarded_question_id, upstream_answer_id);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .yourself,
        .answer_id = upstream_answer_id,
        .mode = .sent_elsewhere,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results_sent_elsewhere,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return sentElsewhere mode rejects unexpected result payload" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const local_forwarded_question_id: u32 = 601;
    try peer.forwarded_questions.put(local_forwarded_question_id, 600);

    const forward_ctx = try allocator.create(ForwardCallContext);
    forward_ctx.* = .{
        .peer = &peer,
        .payload = undefined,
        .inbound_caps = undefined,
        .send_results_to = .yourself,
        .answer_id = 600,
        .mode = .sent_elsewhere,
    };

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const ret = protocol.Return{
        .answer_id = local_forwarded_question_id,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = protocol.Payload{
            .content = undefined,
            .cap_table = null,
        },
        .exception = null,
        .take_from_other_question = null,
    };

    try std.testing.expectError(error.UnexpectedForwardedTailReturn, Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound));
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "handleResolvedCall forwards sendResultsTo.yourself when forwarding imported target" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.results_sent_elsewhere, ret.tag);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(700, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(700, {});

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(700, 0x10, 1);
    try call.setTargetImportedCap(77);
    call.setSendResultsToYourself();
    try call.setEmptyCapTable();

    const bytes = try call_builder.finish();
    defer allocator.free(bytes);
    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer.handleResolvedCall(parsed, &inbound, .{ .imported = .{ .id = 77 } });
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var forwarded_call_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer forwarded_call_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, forwarded_call_msg.tag);
    const forwarded_call = try forwarded_call_msg.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.yourself, forwarded_call.send_results_to.tag);
    const forwarded_question_id = forwarded_call.question_id;

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
}

test "handleResolvedCall forwards sendResultsTo.thirdParty when forwarding promised target" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            const await_ptr = ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
            try std.testing.expectEqualStrings("third-party-destination", try await_ptr.getText());
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(800, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });
    try peer.loopback_questions.put(800, {});

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("third-party-destination");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(800, 0x10, 1);
    try call.setTargetImportedCap(77);
    try call.setSendResultsToThirdParty(third_ptr);
    try call.setEmptyCapTable();

    const bytes = try call_builder.finish();
    defer allocator.free(bytes);
    var decoded = try protocol.DecodedMessage.init(allocator, bytes);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer.handleResolvedCall(parsed, &inbound, .{
        .promised = .{
            .question_id = 1,
            .transform = .{ .list = null },
        },
    });
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var forwarded_call_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer forwarded_call_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, forwarded_call_msg.tag);
    const forwarded_call = try forwarded_call_msg.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.third_party, forwarded_call.send_results_to.tag);
    const forwarded_third_party = forwarded_call.send_results_to.third_party orelse return error.MissingThirdPartyPayload;
    try std.testing.expectEqualStrings("third-party-destination", try forwarded_third_party.getText());
    const forwarded_question_id = forwarded_call.question_id;

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
}

test "handleCall supports sendResultsTo.yourself for local export target" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
    };
    const BuildCtx = struct {};
    const Handlers = struct {
        fn buildCall(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            _ = ctx;
            call.setSendResultsToYourself();
        }

        fn buildResults(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            _ = ctx;
            _ = try ret.initResultsStruct(0, 0);
            try ret.setEmptyCapTable();
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = @ptrCast(@alignCast(ctx));
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = @ptrCast(@alignCast(ctx));
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.results_sent_elsewhere, ret.tag);
            try std.testing.expect(ret.results == null);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    client_ctx.returned = false;
    var build_ctx = BuildCtx{};
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &build_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );

    try std.testing.expect(client_ctx.returned);
    try std.testing.expect(server_ctx.called);
}

test "handleCall supports sendResultsTo.thirdParty for local export target" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const ClientCtx = struct {
        returned: bool = false,
    };
    const BuildCtx = struct {
        destination: message.AnyPointerReader,
    };
    const Handlers = struct {
        fn buildCall(ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void {
            const build: *const BuildCtx = @ptrCast(@alignCast(ctx));
            try call.setSendResultsToThirdParty(build.destination);
        }

        fn buildResults(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            _ = ctx;
            _ = try ret.initResultsStruct(0, 0);
            try ret.setEmptyCapTable();
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = @ptrCast(@alignCast(ctx));
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = @ptrCast(@alignCast(ctx));
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            try std.testing.expect(ret.results == null);
            const await_ptr = ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
            try std.testing.expectEqualStrings("local-third-party", try await_ptr.getText());
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var client_ctx = ClientCtx{};
    client_ctx.returned = false;

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("local-third-party");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();

    var build_ctx = BuildCtx{
        .destination = try third_msg.getRootAnyPointer(),
    };
    _ = try peer.sendCallResolved(
        .{ .exported = .{ .id = export_id } },
        0x99,
        0,
        &build_ctx,
        Handlers.buildCall,
        Handlers.onReturn,
    );

    try std.testing.expect(client_ctx.returned);
    try std.testing.expect(server_ctx.called);
}

test "handleReturn adopts thirdPartyAnswer when await arrives first" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        answer_id: u32 = 0,
        reason: []const u8 = "",
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            state.reason = ex.reason;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const original_answer_id: u32 = 1100;
    const adopted_answer_id: u32 = 0x4000_0011;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = false,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("await-first-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(original_answer_id, .accept_from_third_party);
    try await_ret.setAcceptFromThirdParty(completion_ptr);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
    defer third_party_answer_builder.deinit();
    try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
    const third_party_answer_frame = try third_party_answer_builder.finish();
    defer allocator.free(third_party_answer_frame);
    try peer.handleFrame(third_party_answer_frame);

    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expect(peer.questions.contains(adopted_answer_id));

    var final_builder = protocol.MessageBuilder.init(allocator);
    defer final_builder.deinit();
    var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
    try final_ret.setException("done-through-third-party");
    const final_frame = try final_builder.finish();
    defer allocator.free(final_frame);
    try peer.handleFrame(final_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(original_answer_id, callback_ctx.answer_id);
    try std.testing.expectEqualStrings("done-through-third-party", callback_ctx.reason);
    try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var finish0 = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer finish0.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish0.tag);
    const finish0_body = try finish0.asFinish();
    try std.testing.expectEqual(original_answer_id, finish0_body.question_id);

    var finish1 = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer finish1.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish1.tag);
    const finish1_body = try finish1.asFinish();
    try std.testing.expectEqual(adopted_answer_id, finish1_body.question_id);
}

test "handleReturn replays buffered thirdPartyAnswer return when await arrives later" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
        answer_id: u32 = 0,
        reason: []const u8 = "",
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            state.reason = ex.reason;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    const original_answer_id: u32 = 1200;
    const adopted_answer_id: u32 = 0x4000_0012;

    var callback_ctx = CallbackCtx{};
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = false,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("answer-first-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
    defer third_party_answer_builder.deinit();
    try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
    const third_party_answer_frame = try third_party_answer_builder.finish();
    defer allocator.free(third_party_answer_frame);
    try peer.handleFrame(third_party_answer_frame);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());

    var early_ret_builder = protocol.MessageBuilder.init(allocator);
    defer early_ret_builder.deinit();
    var early_ret = try early_ret_builder.beginReturn(adopted_answer_id, .exception);
    try early_ret.setException("replayed-from-buffer");
    const early_ret_frame = try early_ret_builder.finish();
    defer allocator.free(early_ret_frame);
    try peer.handleFrame(early_ret_frame);

    try std.testing.expect(!callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_returns.count());

    var await_builder = protocol.MessageBuilder.init(allocator);
    defer await_builder.deinit();
    var await_ret = try await_builder.beginReturn(original_answer_id, .accept_from_third_party);
    try await_ret.setAcceptFromThirdParty(completion_ptr);
    const await_frame = try await_builder.finish();
    defer allocator.free(await_frame);
    try peer.handleFrame(await_frame);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expectEqual(original_answer_id, callback_ctx.answer_id);
    try std.testing.expectEqualStrings("replayed-from-buffer", callback_ctx.reason);
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
    try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
    try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var finish0 = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer finish0.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish0.tag);
    const finish0_body = try finish0.asFinish();
    try std.testing.expectEqual(adopted_answer_id, finish0_body.question_id);

    var finish1 = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer finish1.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, finish1.tag);
    const finish1_body = try finish1.asFinish();
    try std.testing.expectEqual(original_answer_id, finish1_body.question_id);
}

test "thirdPartyAnswer stress race keeps pending state empty" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: usize = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen += 1;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("stress-third-party", ex.reason);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("stress-completion");
    const completion_bytes = try completion_builder.toBytes();
    defer allocator.free(completion_bytes);
    var completion_msg = try message.Message.init(allocator, completion_bytes);
    defer completion_msg.deinit();
    const completion_ptr = try completion_msg.getRootAnyPointer();

    var callback_ctx = CallbackCtx{};
    const rounds: u32 = 96;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const original_answer_id: u32 = 1400 + round;
        const adopted_answer_id: u32 = 0x4000_1000 + round;
        try peer.questions.put(original_answer_id, .{
            .ctx = &callback_ctx,
            .on_return = Handlers.onReturn,
            .is_loopback = false,
        });

        if ((round % 2) == 0) {
            var await_builder = protocol.MessageBuilder.init(allocator);
            defer await_builder.deinit();
            var await_ret = try await_builder.beginReturn(original_answer_id, .accept_from_third_party);
            try await_ret.setAcceptFromThirdParty(completion_ptr);
            const await_frame = try await_builder.finish();
            defer allocator.free(await_frame);
            try peer.handleFrame(await_frame);

            var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
            defer third_party_answer_builder.deinit();
            try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
            const third_party_answer_frame = try third_party_answer_builder.finish();
            defer allocator.free(third_party_answer_frame);
            try peer.handleFrame(third_party_answer_frame);

            var final_builder = protocol.MessageBuilder.init(allocator);
            defer final_builder.deinit();
            var final_ret = try final_builder.beginReturn(adopted_answer_id, .exception);
            try final_ret.setException("stress-third-party");
            const final_frame = try final_builder.finish();
            defer allocator.free(final_frame);
            try peer.handleFrame(final_frame);
        } else {
            var third_party_answer_builder = protocol.MessageBuilder.init(allocator);
            defer third_party_answer_builder.deinit();
            try third_party_answer_builder.buildThirdPartyAnswer(adopted_answer_id, completion_ptr);
            const third_party_answer_frame = try third_party_answer_builder.finish();
            defer allocator.free(third_party_answer_frame);
            try peer.handleFrame(third_party_answer_frame);

            var early_builder = protocol.MessageBuilder.init(allocator);
            defer early_builder.deinit();
            var early_ret = try early_builder.beginReturn(adopted_answer_id, .exception);
            try early_ret.setException("stress-third-party");
            const early_frame = try early_builder.finish();
            defer allocator.free(early_frame);
            try peer.handleFrame(early_frame);

            var await_builder = protocol.MessageBuilder.init(allocator);
            defer await_builder.deinit();
            var await_ret = try await_builder.beginReturn(original_answer_id, .accept_from_third_party);
            try await_ret.setAcceptFromThirdParty(completion_ptr);
            const await_frame = try await_builder.finish();
            defer allocator.free(await_frame);
            try peer.handleFrame(await_frame);
        }

        try std.testing.expectEqual(@as(usize, @intCast(round + 1)), callback_ctx.seen);
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_returns.count());
        try std.testing.expectEqual(@as(usize, 0), peer.adopted_third_party_answers.count());
        try std.testing.expect(!peer.questions.contains(adopted_answer_id));
    }

    try std.testing.expectEqual(@as(usize, rounds), callback_ctx.seen);
    try std.testing.expectEqual(@as(usize, rounds * 2), capture.frames.items.len);
}

test "peer deinit releases pending embargo and promised-call queues under load" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(ctx: *anyopaque, called_peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    var conn: Connection = undefined;
    {
        var peer = Peer.init(allocator, &conn);
        defer peer.deinit();

        var handler_state: u8 = 0;
        const export_id = try peer.addExport(.{
            .ctx = &handler_state,
            .on_call = Handlers.onCall,
        });

        var recipient_builder = message.MessageBuilder.init(allocator);
        defer recipient_builder.deinit();
        const recipient_root = try recipient_builder.initRootAnyPointer();
        try recipient_root.setText("deinit-pending-recipient");
        const recipient_bytes = try recipient_builder.toBytes();
        defer allocator.free(recipient_bytes);
        var recipient_msg = try message.Message.init(allocator, recipient_bytes);
        defer recipient_msg.deinit();
        const recipient_ptr = try recipient_msg.getRootAnyPointer();

        var provide_builder = protocol.MessageBuilder.init(allocator);
        defer provide_builder.deinit();
        try provide_builder.buildProvide(
            6000,
            .{
                .tag = .imported_cap,
                .imported_cap = export_id,
                .promised_answer = null,
            },
            recipient_ptr,
        );
        const provide_frame = try provide_builder.finish();
        defer allocator.free(provide_frame);
        try peer.handleFrame(provide_frame);

        const rounds: u32 = 80;
        var round: u32 = 0;
        while (round < rounds) : (round += 1) {
            const accept_qid: u32 = 6100 + (round * 2);
            const call_qid: u32 = accept_qid + 1;

            var accept_builder = protocol.MessageBuilder.init(allocator);
            defer accept_builder.deinit();
            try accept_builder.buildAccept(accept_qid, recipient_ptr, "deinit-embargo");
            const accept_frame = try accept_builder.finish();
            defer allocator.free(accept_frame);
            try peer.handleFrame(accept_frame);

            var call_builder = protocol.MessageBuilder.init(allocator);
            defer call_builder.deinit();
            var call = try call_builder.beginCall(call_qid, 0xA1, 0);
            try call.setTargetPromisedAnswer(accept_qid);
            try call.setEmptyCapTable();
            const call_frame = try call_builder.finish();
            defer allocator.free(call_frame);
            try peer.handleFrame(call_frame);
        }

        try std.testing.expectEqual(rounds, @as(u32, @intCast(peer.pending_promises.count())));
        try std.testing.expectEqual(@as(usize, 1), peer.pending_accepts_by_embargo.count());
        try std.testing.expectEqual(rounds, @as(u32, @intCast(peer.pending_accept_embargo_by_question.count())));
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_answers.count());
        try std.testing.expectEqual(@as(usize, 0), peer.pending_third_party_awaits.count());
    }
}

test "handleFinish forwards mapped tail finish question id" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);
    try peer.forwarded_tail_questions.put(10, 20);

    try peer.handleFinish(.{
        .question_id = 10,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expect(!peer.forwarded_tail_questions.contains(10));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, decoded.tag);
    const finish = try decoded.asFinish();
    try std.testing.expectEqual(@as(u32, 20), finish.question_id);
    try std.testing.expect(!finish.release_result_caps);
}

test "handleFinish without tail mapping does not send finish" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        count: usize = 0,

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            _ = frame;
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            ctx.count += 1;
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{};
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    try peer.handleFinish(.{
        .question_id = 1234,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expectEqual(@as(usize, 0), capture.count);
}

test "forwarded caller tail call emits yourself call, takeFromOtherQuestion, and propagated finish" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const upstream_question_id: u32 = 900;
    const interface_id: u64 = 0x01020304;
    const method_id: u16 = 7;
    const target_import_id: u32 = 77;

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(upstream_question_id, interface_id, method_id);
    try call.setTargetImportedCap(999);
    call.setSendResultsToCaller();
    try call.setEmptyCapTable();

    const call_bytes = try call_builder.finish();
    defer allocator.free(call_bytes);
    var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
    defer decoded_call.deinit();
    const parsed = try decoded_call.asCall();

    try peer.handleResolvedCall(parsed, &inbound, .{ .imported = .{ .id = target_import_id } });

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.call, out_call_decoded.tag);
    const forwarded_call = try out_call_decoded.asCall();
    try std.testing.expectEqual(protocol.SendResultsToTag.yourself, forwarded_call.send_results_to.tag);
    try std.testing.expectEqual(protocol.MessageTargetTag.imported_cap, forwarded_call.target.tag);
    try std.testing.expectEqual(target_import_id, forwarded_call.target.imported_cap.?);
    try std.testing.expectEqual(interface_id, forwarded_call.interface_id);
    try std.testing.expectEqual(method_id, forwarded_call.method_id);
    const forwarded_question_id = forwarded_call.question_id;

    var out_ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer out_ret_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, out_ret_decoded.tag);
    const tail_ret = try out_ret_decoded.asReturn();
    try std.testing.expectEqual(upstream_question_id, tail_ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, tail_ret.tag);
    try std.testing.expectEqual(forwarded_question_id, tail_ret.take_from_other_question.?);

    try std.testing.expectEqual(upstream_question_id, peer.forwarded_questions.get(forwarded_question_id).?);
    try std.testing.expectEqual(forwarded_question_id, peer.forwarded_tail_questions.get(upstream_question_id).?);
    const question_entry = peer.questions.getEntry(forwarded_question_id) orelse return error.UnknownQuestion;
    try std.testing.expect(question_entry.value_ptr.suppress_auto_finish);

    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));

    try peer.handleFinish(.{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    var out_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer out_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, out_finish_decoded.tag);
    const forwarded_finish = try out_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);
}

test "forwarded tail finish before forwarded return still emits single finish and drains state" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }

    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const upstream_question_id: u32 = 1000;
    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(upstream_question_id, 0x44, 3);
    try call.setTargetImportedCap(111);
    call.setSendResultsToCaller();
    try call.setEmptyCapTable();

    const call_bytes = try call_builder.finish();
    defer allocator.free(call_bytes);
    var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
    defer decoded_call.deinit();
    const parsed = try decoded_call.asCall();

    try peer.handleResolvedCall(parsed, &inbound, .{ .imported = .{ .id = 222 } });
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_call_decoded.deinit();
    const forwarded_call = try out_call_decoded.asCall();
    const forwarded_question_id = forwarded_call.question_id;

    try peer.handleFinish(.{
        .question_id = upstream_question_id,
        .release_result_caps = false,
        .require_early_cancellation = false,
    });

    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));

    var out_finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[2]);
    defer out_finish_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.finish, out_finish_decoded.tag);
    const forwarded_finish = try out_finish_decoded.asFinish();
    try std.testing.expectEqual(forwarded_question_id, forwarded_finish.question_id);

    var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
    defer fwd_ret_builder.deinit();
    _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
    const fwd_ret_frame = try fwd_ret_builder.finish();
    defer allocator.free(fwd_ret_frame);
    try peer.handleFrame(fwd_ret_frame);

    try std.testing.expectEqual(@as(usize, 3), capture.frames.items.len);
    try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
    try std.testing.expect(!peer.questions.contains(forwarded_question_id));
}

test "forwarded tail cleanup stays stable under repeated finish/return ordering races" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    const rounds: u32 = 64;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const frame_start = capture.frames.items.len;
        const upstream_question_id: u32 = 2000 + round;

        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(upstream_question_id, 0x44, 3);
        try call.setTargetImportedCap(111);
        call.setSendResultsToCaller();
        try call.setEmptyCapTable();

        const call_bytes = try call_builder.finish();
        defer allocator.free(call_bytes);
        var decoded_call = try protocol.DecodedMessage.init(allocator, call_bytes);
        defer decoded_call.deinit();
        const parsed = try decoded_call.asCall();

        try peer.handleResolvedCall(parsed, &inbound, .{ .imported = .{ .id = 222 } });
        try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);

        var out_call_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start]);
        defer out_call_decoded.deinit();
        const forwarded_call = try out_call_decoded.asCall();
        const forwarded_question_id = forwarded_call.question_id;

        var out_ret_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
        defer out_ret_decoded.deinit();
        const tail_ret = try out_ret_decoded.asReturn();
        try std.testing.expectEqual(upstream_question_id, tail_ret.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, tail_ret.tag);
        try std.testing.expectEqual(forwarded_question_id, tail_ret.take_from_other_question.?);

        if ((round & 1) == 0) {
            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);

            try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
            try std.testing.expect(!peer.questions.contains(forwarded_question_id));

            try peer.handleFinish(.{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });
        } else {
            try peer.handleFinish(.{
                .question_id = upstream_question_id,
                .release_result_caps = false,
                .require_early_cancellation = false,
            });

            var fwd_ret_builder = protocol.MessageBuilder.init(allocator);
            defer fwd_ret_builder.deinit();
            _ = try fwd_ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
            const fwd_ret_frame = try fwd_ret_builder.finish();
            defer allocator.free(fwd_ret_frame);
            try peer.handleFrame(fwd_ret_frame);
        }

        try std.testing.expectEqual(frame_start + 3, capture.frames.items.len);
        try std.testing.expect(!peer.forwarded_tail_questions.contains(upstream_question_id));
        try std.testing.expect(!peer.forwarded_questions.contains(forwarded_question_id));
        try std.testing.expect(!peer.questions.contains(forwarded_question_id));

        var finish_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 2]);
        defer finish_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.finish, finish_decoded.tag);
        const finish = try finish_decoded.asFinish();
        try std.testing.expectEqual(forwarded_question_id, finish.question_id);
    }
}

test "promisedAnswer target queues when resolved cap is unresolved promise export and replays on resolve" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
        question_id: u32 = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = @ptrCast(@alignCast(ctx_ptr));
            ctx.called = true;
            ctx.question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "resolved");
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |frame| allocator.free(frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var server_ctx = ServerCtx{};
    const concrete_export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });
    const promise_export_id = try peer.addPromiseExport();

    const promised_answer_id: u32 = 300;
    {
        var ret_builder = protocol.MessageBuilder.init(allocator);
        defer ret_builder.deinit();
        var ret = try ret_builder.beginReturn(promised_answer_id, .results);
        var any = try ret.getResultsAnyPointer();
        try any.setCapability(.{ .id = 0 });
        var cap_list = try ret.initCapTable(1);
        const entry = try cap_list.get(0);
        protocol.CapDescriptor.writeSenderPromise(entry, promise_export_id);

        const frame = try ret_builder.finish();
        defer allocator.free(frame);
        const stored = try allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, stored, frame);
        try peer.resolved_answers.put(promised_answer_id, .{ .frame = stored });
    }

    const queued_question_id: u32 = 301;
    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(queued_question_id, 0xABCD, 2);
    try call.setTargetPromisedAnswer(promised_answer_id);
    try call.setEmptyCapTable();

    const frame = try call_builder.finish();
    defer allocator.free(frame);

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const parsed = try decoded.asCall();

    try peer.handleCall(frame, parsed);
    try std.testing.expect(!server_ctx.called);
    try std.testing.expect(peer.pending_export_promises.contains(promise_export_id));
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    try peer.resolvePromiseExportToExport(promise_export_id, concrete_export_id);

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(queued_question_id, server_ctx.question_id);
    try std.testing.expect(!peer.pending_export_promises.contains(promise_export_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var resolve_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer resolve_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.resolve, resolve_msg.tag);
    const resolve = try resolve_msg.asResolve();
    try std.testing.expectEqual(promise_export_id, resolve.promise_id);
    try std.testing.expectEqual(protocol.ResolveTag.cap, resolve.tag);
    const cap = resolve.cap orelse return error.MissingResolveCap;
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, cap.tag);
    try std.testing.expectEqual(concrete_export_id, cap.id.?);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(queued_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("resolved", ex.reason);
}

test "handleFrame unimplemented call converts outstanding question to exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = @ptrCast(@alignCast(ctx));
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("unimplemented", ex.reason);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    const question_id: u32 = 420;
    var callback_ctx = CallbackCtx{};
    try peer.questions.put(question_id, .{
        .ctx = &callback_ctx,
        .on_return = Handlers.onReturn,
        .is_loopback = true,
    });

    var inner_builder = protocol.MessageBuilder.init(allocator);
    defer inner_builder.deinit();
    var inner_call = try inner_builder.beginCall(question_id, 0x44, 3);
    try inner_call.setTargetImportedCap(1);
    try inner_call.setEmptyCapTable();
    const inner_bytes = try inner_builder.finish();
    defer allocator.free(inner_bytes);

    var inner_msg = try message.Message.init(allocator, inner_bytes);
    defer inner_msg.deinit();
    const inner_root = try inner_msg.getRootAnyPointer();

    var outer_builder = protocol.MessageBuilder.init(allocator);
    defer outer_builder.deinit();
    try outer_builder.buildUnimplementedFromAnyPointer(inner_root);
    const outer_bytes = try outer_builder.finish();
    defer allocator.free(outer_bytes);

    try peer.handleFrame(outer_bytes);
    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.questions.contains(question_id));
}

test "handleFrame abort returns remote abort error" {
    const allocator = std.testing.allocator;

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort("fatal");
    const frame = try builder.finish();
    defer allocator.free(frame);

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(frame));
}

test "handleFrame provide stores provision without immediate return" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("vat-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        900,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);
    try std.testing.expect(peer.provides_by_question.contains(900));
    try std.testing.expectEqual(@as(usize, 1), peer.provides_by_key.count());
}

test "handleFrame duplicate provide recipient sends abort" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("same-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        901,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const first_frame = try in_builder.finish();
    defer allocator.free(first_frame);
    try peer.handleFrame(first_frame);

    var duplicate_builder = protocol.MessageBuilder.init(allocator);
    defer duplicate_builder.deinit();
    try duplicate_builder.buildProvide(
        902,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const duplicate_frame = try duplicate_builder.finish();
    defer allocator.free(duplicate_frame);

    try std.testing.expectError(error.DuplicateProvideRecipient, peer.handleFrame(duplicate_frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, out_decoded.tag);
    const abort = try out_decoded.asAbort();
    try std.testing.expectEqualStrings("duplicate provide recipient", abort.exception.reason);
}

test "handleFrame accept returns provided capability" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("accept-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildProvide(
        902,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try in_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(903, recipient_ptr, null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);

    try peer.handleFrame(accept_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 903), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "handleFrame embargoed accept + promised calls preserve ordering under stress" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: usize = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = caps;
            const state: *ServerCtx = @ptrCast(@alignCast(ctx));
            state.called += 1;
            try called_peer.sendReturnException(call.question_id, "stress-ordered");
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var server_ctx = ServerCtx{};
    const export_id = try peer.addExport(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("stress-accept-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        1200,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    const embargo = "stress-accept-embargo";
    const rounds: u32 = 64;
    var round: u32 = 0;
    while (round < rounds) : (round += 1) {
        const frame_start = capture.frames.items.len;
        const accept_qid: u32 = 1300 + round * 2;
        const call_qid: u32 = accept_qid + 1;

        var accept_builder = protocol.MessageBuilder.init(allocator);
        defer accept_builder.deinit();
        try accept_builder.buildAccept(accept_qid, recipient_ptr, embargo);
        const accept_frame = try accept_builder.finish();
        defer allocator.free(accept_frame);
        try peer.handleFrame(accept_frame);

        var pipelined_builder = protocol.MessageBuilder.init(allocator);
        defer pipelined_builder.deinit();
        var pipelined_call = try pipelined_builder.beginCall(call_qid, 0x1234, 1);
        try pipelined_call.setTargetPromisedAnswer(accept_qid);
        try pipelined_call.setEmptyCapTable();
        const pipelined_frame = try pipelined_builder.finish();
        defer allocator.free(pipelined_frame);
        try peer.handleFrame(pipelined_frame);

        try std.testing.expect(peer.pending_promises.contains(accept_qid));
        try std.testing.expectEqual(frame_start, capture.frames.items.len);
        try std.testing.expectEqual(round, @as(u32, @intCast(server_ctx.called)));

        var disembargo_builder = protocol.MessageBuilder.init(allocator);
        defer disembargo_builder.deinit();
        try disembargo_builder.buildDisembargoAccept(
            .{
                .tag = .imported_cap,
                .imported_cap = export_id,
                .promised_answer = null,
            },
            embargo,
        );
        const disembargo_frame = try disembargo_builder.finish();
        defer allocator.free(disembargo_frame);
        try peer.handleFrame(disembargo_frame);

        try std.testing.expectEqual(frame_start + 2, capture.frames.items.len);
        try std.testing.expect(!peer.pending_promises.contains(accept_qid));
        try std.testing.expectEqual(round + 1, @as(u32, @intCast(server_ctx.called)));

        var accept_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start]);
        defer accept_ret.deinit();
        try std.testing.expectEqual(protocol.MessageTag.return_, accept_ret.tag);
        const accept_return = try accept_ret.asReturn();
        try std.testing.expectEqual(accept_qid, accept_return.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.results, accept_return.tag);

        var pipelined_ret = try protocol.DecodedMessage.init(allocator, capture.frames.items[frame_start + 1]);
        defer pipelined_ret.deinit();
        try std.testing.expectEqual(protocol.MessageTag.return_, pipelined_ret.tag);
        const replayed_return = try pipelined_ret.asReturn();
        try std.testing.expectEqual(call_qid, replayed_return.answer_id);
        try std.testing.expectEqual(protocol.ReturnTag.exception, replayed_return.tag);
        const ex = replayed_return.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings("stress-ordered", ex.reason);

        var accept_finish_builder = protocol.MessageBuilder.init(allocator);
        defer accept_finish_builder.deinit();
        try accept_finish_builder.buildFinish(accept_qid, false, false);
        const accept_finish_frame = try accept_finish_builder.finish();
        defer allocator.free(accept_finish_frame);
        try peer.handleFrame(accept_finish_frame);

        var call_finish_builder = protocol.MessageBuilder.init(allocator);
        defer call_finish_builder.deinit();
        try call_finish_builder.buildFinish(call_qid, false, false);
        const call_finish_frame = try call_finish_builder.finish();
        defer allocator.free(call_finish_frame);
        try peer.handleFrame(call_finish_frame);
    }

    try std.testing.expectEqual(rounds, @as(u32, @intCast(server_ctx.called)));
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 0), peer.pending_accept_embargo_by_question.count());
}

test "handleFrame accept unknown provision returns exception" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildAccept(904, null, null);
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 904), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("unknown provision", ex.reason);
}

test "handleFrame finish clears stored provide entry" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("finish-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        905,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        recipient_ptr,
    );
    const provide_frame = try provide_builder.finish();
    defer allocator.free(provide_frame);
    try peer.handleFrame(provide_frame);

    var finish_builder = protocol.MessageBuilder.init(allocator);
    defer finish_builder.deinit();
    try finish_builder.buildFinish(905, false, false);
    const finish_frame = try finish_builder.finish();
    defer allocator.free(finish_frame);
    try peer.handleFrame(finish_frame);

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(906, recipient_ptr, null);
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);
    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 906), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("unknown provision", ex.reason);
}

test "handleFrame join returns capability" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key_part_builder = message.MessageBuilder.init(allocator);
    defer key_part_builder.deinit();
    const key_part_root = try key_part_builder.initRootAnyPointer();
    var key_part_struct = try key_part_root.initStruct(1, 0);
    key_part_struct.writeU32(0, 0xA1);
    key_part_struct.writeU16(4, 1);
    key_part_struct.writeU16(6, 0);
    const key_part_bytes = try key_part_builder.toBytes();
    defer allocator.free(key_part_bytes);
    var key_part_msg = try message.Message.init(allocator, key_part_bytes);
    defer key_part_msg.deinit();
    const key_part_ptr = try key_part_msg.getRootAnyPointer();

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildJoin(
        907,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        key_part_ptr,
    );
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
    const ret = try out_decoded.asReturn();
    try std.testing.expectEqual(@as(u32, 907), ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);
    const payload = ret.results orelse return error.MissingPayload;
    const cap = try payload.content.getCapability();
    const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
    const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
    try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
    try std.testing.expectEqual(export_id, descriptor.id.?);
}

test "handleFrame join aggregates parts and returns capability for each part" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key0_builder = message.MessageBuilder.init(allocator);
    defer key0_builder.deinit();
    const key0_root = try key0_builder.initRootAnyPointer();
    var key0_struct = try key0_root.initStruct(1, 0);
    key0_struct.writeU32(0, 0xB2);
    key0_struct.writeU16(4, 2);
    key0_struct.writeU16(6, 0);
    const key0_bytes = try key0_builder.toBytes();
    defer allocator.free(key0_bytes);
    var key0_msg = try message.Message.init(allocator, key0_bytes);
    defer key0_msg.deinit();
    const key0_ptr = try key0_msg.getRootAnyPointer();

    var join0_builder = protocol.MessageBuilder.init(allocator);
    defer join0_builder.deinit();
    try join0_builder.buildJoin(
        910,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        key0_ptr,
    );
    const join0_frame = try join0_builder.finish();
    defer allocator.free(join0_frame);
    try peer.handleFrame(join0_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var key1_builder = message.MessageBuilder.init(allocator);
    defer key1_builder.deinit();
    const key1_root = try key1_builder.initRootAnyPointer();
    var key1_struct = try key1_root.initStruct(1, 0);
    key1_struct.writeU32(0, 0xB2);
    key1_struct.writeU16(4, 2);
    key1_struct.writeU16(6, 1);
    const key1_bytes = try key1_builder.toBytes();
    defer allocator.free(key1_bytes);
    var key1_msg = try message.Message.init(allocator, key1_bytes);
    defer key1_msg.deinit();
    const key1_ptr = try key1_msg.getRootAnyPointer();

    var join1_builder = protocol.MessageBuilder.init(allocator);
    defer join1_builder.deinit();
    try join1_builder.buildJoin(
        911,
        .{
            .tag = .imported_cap,
            .imported_cap = export_id,
            .promised_answer = null,
        },
        key1_ptr,
    );
    const join1_frame = try join1_builder.finish();
    defer allocator.free(join1_frame);
    try peer.handleFrame(join1_frame);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var seen_910 = false;
    var seen_911 = false;
    for (capture.frames.items) |out_frame| {
        var out_decoded = try protocol.DecodedMessage.init(allocator, out_frame);
        defer out_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
        const ret = try out_decoded.asReturn();
        try std.testing.expectEqual(protocol.ReturnTag.results, ret.tag);

        if (ret.answer_id == 910) {
            seen_910 = true;
        } else if (ret.answer_id == 911) {
            seen_911 = true;
        } else {
            return error.UnexpectedQuestionId;
        }

        const payload = ret.results orelse return error.MissingPayload;
        const cap = try payload.content.getCapability();
        const cap_table_reader = payload.cap_table orelse return error.MissingCapTable;
        const descriptor = try protocol.CapDescriptor.fromReader(try cap_table_reader.get(cap.id));
        try std.testing.expectEqual(protocol.CapDescriptorTag.sender_hosted, descriptor.tag);
        try std.testing.expectEqual(export_id, descriptor.id.?);
    }
    try std.testing.expect(seen_910);
    try std.testing.expect(seen_911);
}

test "handleFrame join returns exceptions when targets mismatch across parts" {
    const allocator = std.testing.allocator;

    const Handlers = struct {
        fn onCall(
            ctx: *anyopaque,
            called_peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = ctx;
            _ = called_peer;
            _ = call;
            _ = caps;
        }
    };

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var handler_a: u8 = 0;
    const export_a = try peer.addExport(.{
        .ctx = &handler_a,
        .on_call = Handlers.onCall,
    });
    var handler_b: u8 = 0;
    const export_b = try peer.addExport(.{
        .ctx = &handler_b,
        .on_call = Handlers.onCall,
    });

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var key0_builder = message.MessageBuilder.init(allocator);
    defer key0_builder.deinit();
    const key0_root = try key0_builder.initRootAnyPointer();
    var key0_struct = try key0_root.initStruct(1, 0);
    key0_struct.writeU32(0, 0xC3);
    key0_struct.writeU16(4, 2);
    key0_struct.writeU16(6, 0);
    const key0_bytes = try key0_builder.toBytes();
    defer allocator.free(key0_bytes);
    var key0_msg = try message.Message.init(allocator, key0_bytes);
    defer key0_msg.deinit();
    const key0_ptr = try key0_msg.getRootAnyPointer();

    var join0_builder = protocol.MessageBuilder.init(allocator);
    defer join0_builder.deinit();
    try join0_builder.buildJoin(
        920,
        .{
            .tag = .imported_cap,
            .imported_cap = export_a,
            .promised_answer = null,
        },
        key0_ptr,
    );
    const join0_frame = try join0_builder.finish();
    defer allocator.free(join0_frame);
    try peer.handleFrame(join0_frame);
    try std.testing.expectEqual(@as(usize, 0), capture.frames.items.len);

    var key1_builder = message.MessageBuilder.init(allocator);
    defer key1_builder.deinit();
    const key1_root = try key1_builder.initRootAnyPointer();
    var key1_struct = try key1_root.initStruct(1, 0);
    key1_struct.writeU32(0, 0xC3);
    key1_struct.writeU16(4, 2);
    key1_struct.writeU16(6, 1);
    const key1_bytes = try key1_builder.toBytes();
    defer allocator.free(key1_bytes);
    var key1_msg = try message.Message.init(allocator, key1_bytes);
    defer key1_msg.deinit();
    const key1_ptr = try key1_msg.getRootAnyPointer();

    var join1_builder = protocol.MessageBuilder.init(allocator);
    defer join1_builder.deinit();
    try join1_builder.buildJoin(
        921,
        .{
            .tag = .imported_cap,
            .imported_cap = export_b,
            .promised_answer = null,
        },
        key1_ptr,
    );
    const join1_frame = try join1_builder.finish();
    defer allocator.free(join1_frame);
    try peer.handleFrame(join1_frame);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var seen_920 = false;
    var seen_921 = false;
    for (capture.frames.items) |out_frame| {
        var out_decoded = try protocol.DecodedMessage.init(allocator, out_frame);
        defer out_decoded.deinit();
        try std.testing.expectEqual(protocol.MessageTag.return_, out_decoded.tag);
        const ret = try out_decoded.asReturn();
        try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
        const ex = ret.exception orelse return error.MissingException;
        try std.testing.expectEqualStrings("join target mismatch", ex.reason);

        if (ret.answer_id == 920) {
            seen_920 = true;
        } else if (ret.answer_id == 921) {
            seen_921 = true;
        } else {
            return error.UnexpectedQuestionId;
        }
    }
    try std.testing.expect(seen_920);
    try std.testing.expect(seen_921);
}

test "handleFrame thirdPartyAnswer rejects missing completion" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = protocol.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    try in_builder.buildThirdPartyAnswer(0x4000_004D, null);
    const in_frame = try in_builder.finish();
    defer allocator.free(in_frame);

    try std.testing.expectError(error.MissingThirdPartyPayload, peer.handleFrame(in_frame));
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.abort, out_decoded.tag);
    const abort = try out_decoded.asAbort();
    try std.testing.expectEqualStrings("thirdPartyAnswer missing completion", abort.exception.reason);
}

test "handleFrame unknown message tag sends unimplemented" {
    const allocator = std.testing.allocator;

    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var conn: Connection = undefined;
    var peer = Peer.init(allocator, &conn);
    defer peer.deinit();

    var capture = Capture{
        .allocator = allocator,
        .frames = std.ArrayList([]u8){},
    };
    defer {
        for (capture.frames.items) |out_frame| allocator.free(out_frame);
        capture.frames.deinit(allocator);
    }
    peer.setSendFrameOverride(&capture, Capture.onFrame);

    var in_builder = message.MessageBuilder.init(allocator);
    defer in_builder.deinit();
    var root = try in_builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xFFFF);
    const in_frame = try in_builder.toBytes();
    defer allocator.free(in_frame);

    try peer.handleFrame(in_frame);
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    var out_decoded = try protocol.DecodedMessage.init(allocator, capture.frames.items[0]);
    defer out_decoded.deinit();
    try std.testing.expectEqual(protocol.MessageTag.unimplemented, out_decoded.tag);
    const unimplemented = try out_decoded.asUnimplemented();
    try std.testing.expect(unimplemented.message_tag == null);
    try std.testing.expect(unimplemented.question_id == null);
}
