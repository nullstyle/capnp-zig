const std = @import("std");
const protocol = @import("protocol.zig");
const cap_table = @import("cap_table.zig");
const message = @import("../message.zig");
const peer_dispatch = @import("peer/peer_dispatch.zig");
const peer_control = @import("peer/peer_control.zig");
const peer_call_targets = @import("peer/call/peer_call_targets.zig");
const peer_call_sender = @import("peer/call/peer_call_sender.zig");
const payload_remap = @import("payload_remap.zig");
const peer_promises = @import("peer/peer_promises.zig");
const peer_inbound_release = @import("peer/peer_inbound_release.zig");
const peer_embargo_accepts = @import("peer/peer_embargo_accepts.zig");
const peer_join_state = @import("peer/provide/peer_join_state.zig");
const peer_provides_state = @import("peer/provide/peer_provides_state.zig");
const peer_provide_join_orchestration = @import("peer/provide/peer_provide_join_orchestration.zig");
const peer_forward_orchestration = @import("peer/forward/peer_forward_orchestration.zig");
const peer_forward_return_callbacks = @import("peer/forward/peer_forward_return_callbacks.zig");
const peer_cap_lifecycle = @import("peer/peer_cap_lifecycle.zig");
const peer_outbound_control = @import("peer/peer_outbound_control.zig");
const peer_call_orchestration = @import("peer/call/peer_call_orchestration.zig");
const peer_return_orchestration = @import("peer/return/peer_return_orchestration.zig");
const peer_third_party_adoption = @import("peer/third_party/peer_third_party_adoption.zig");
const peer_return_dispatch = @import("peer/return/peer_return_dispatch.zig");
const peer_third_party_returns = @import("peer/third_party/peer_third_party_returns.zig");
const peer_state_types = @import("peer/peer_state_types.zig");

pub const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
pub const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
pub const CallHandler = *const fn (ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void;
pub const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
pub const SendFrameOverride = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;
pub const TransportStartFn = *const fn (ctx: *anyopaque, peer: *Peer) void;
pub const TransportSendFn = *const fn (ctx: *anyopaque, frame: []const u8) anyerror!void;
pub const TransportCloseFn = *const fn (ctx: *anyopaque) void;
pub const TransportIsClosingFn = *const fn (ctx: *anyopaque) bool;

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

const ProvideTarget = peer_state_types.ProvideTarget;
const ProvideEntry = peer_state_types.ProvideEntry;
const JoinKeyPart = peer_state_types.JoinKeyPart;
const JoinPartEntry = peer_state_types.JoinPartEntry;
const JoinState = peer_state_types.JoinState;
const PendingJoinQuestion = peer_state_types.PendingJoinQuestion;
const PendingEmbargoedAccept = peer_state_types.PendingEmbargoedAccept;

const ForwardReturnMode = peer_forward_orchestration.ForwardReturnMode;

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

fn castCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
    return @ptrCast(@alignCast(ctx));
}

pub const Peer = struct {
    allocator: std.mem.Allocator,
    transport_ctx: ?*anyopaque = null,
    transport_start: ?TransportStartFn = null,
    transport_send: ?TransportSendFn = null,
    transport_close: ?TransportCloseFn = null,
    transport_is_closing: ?TransportIsClosingFn = null,
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

    pub fn init(allocator: std.mem.Allocator, conn: anytype) Peer {
        var peer = initDetached(allocator);
        peer.attachConnection(conn);
        return peer;
    }

    pub fn initDetached(allocator: std.mem.Allocator) Peer {
        return .{
            .allocator = allocator,
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

    pub fn attachConnection(self: *Peer, conn: anytype) void {
        const ConnPtr = @TypeOf(conn);
        comptime {
            const info = @typeInfo(ConnPtr);
            if (info != .pointer) @compileError("attachConnection expects a pointer type");
        }

        self.attachTransport(
            conn,
            struct {
                fn call(ctx: *anyopaque, peer: *Peer) void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    typed.start(
                        peer,
                        onConnectionMessageFor(ConnPtr),
                        onConnectionErrorFor(ConnPtr),
                        onConnectionCloseFor(ConnPtr),
                    );
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque, frame: []const u8) anyerror!void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    try typed.sendFrame(frame);
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque) void {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    typed.close();
                }
            }.call,
            struct {
                fn call(ctx: *anyopaque) bool {
                    const typed: ConnPtr = castCtx(ConnPtr, ctx);
                    return typed.isClosing();
                }
            }.call,
        );
    }

    pub fn detachConnection(self: *Peer) void {
        self.detachTransport();
    }

    pub fn attachTransport(
        self: *Peer,
        ctx: *anyopaque,
        start_fn: ?TransportStartFn,
        send_fn: ?TransportSendFn,
        close_fn: ?TransportCloseFn,
        is_closing: ?TransportIsClosingFn,
    ) void {
        self.transport_ctx = ctx;
        self.transport_start = start_fn;
        self.transport_send = send_fn;
        self.transport_close = close_fn;
        self.transport_is_closing = is_closing;
    }

    pub fn detachTransport(self: *Peer) void {
        self.transport_ctx = null;
        self.transport_start = null;
        self.transport_send = null;
        self.transport_close = null;
        self.transport_is_closing = null;
    }

    pub fn hasAttachedTransport(self: *const Peer) bool {
        return self.transport_ctx != null and self.transport_send != null;
    }

    pub fn closeAttachedTransport(self: *Peer) void {
        if (self.transport_ctx) |ctx| {
            if (self.transport_close) |close| close(ctx);
        }
    }

    pub fn isAttachedTransportClosing(self: *const Peer) bool {
        if (self.transport_ctx) |ctx| {
            if (self.transport_is_closing) |is_closing| return is_closing(ctx);
        }
        return false;
    }

    pub fn takeAttachedConnection(self: *Peer, comptime ConnPtr: type) ?ConnPtr {
        const conn = self.getAttachedConnection(ConnPtr);
        self.detachTransport();
        return conn;
    }

    pub fn getAttachedConnection(self: *const Peer, comptime ConnPtr: type) ?ConnPtr {
        const ctx = self.transport_ctx orelse return null;
        return castCtx(ConnPtr, ctx);
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
        if (self.transport_start) |start_fn| {
            const ctx = self.transport_ctx orelse return;
            start_fn(ctx, self);
        }
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
        try peer_outbound_control.sendResolveCapViaSendFrame(
            Peer,
            self,
            promise_id,
            descriptor,
            Peer.sendFrame,
        );

        promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
        self.caps.clearExportPromise(promise_id);
        try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
    }

    pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
        var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
        if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
        if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

        try peer_outbound_control.sendResolveExceptionViaSendFrame(
            Peer,
            self,
            promise_id,
            reason,
            Peer.sendFrame,
        );
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

        return peer_call_sender.sendCallToImport(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            self,
            target_id,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.sendBuilder,
        );
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
            .imported => |cap| peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                self,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestion,
                Peer.sendBuilder,
            ),
            .promised => |promised| self.sendCallPromised(promised, interface_id, method_id, ctx, build, on_return),
            .exported => |cap| peer_call_sender.sendCallToExport(
                Peer,
                Question,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                onOutboundCap,
                self,
                &self.questions,
                &self.loopback_questions,
                cap.id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestion,
                Peer.handleFrame,
            ),
            .none => error.CapabilityUnavailable,
        };
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
        return peer_call_sender.sendCallPromised(
            Peer,
            CallBuildFn,
            QuestionCallback,
            self.allocator,
            &self.caps,
            self,
            onOutboundCap,
            self,
            promised,
            interface_id,
            method_id,
            ctx,
            build,
            on_return,
            Peer.allocateQuestion,
            Peer.sendBuilder,
        );
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

        const forwarded_plan = try peer_forward_orchestration.buildForwardCallPlan(
            Peer,
            self,
            mode,
            call.send_results_to.third_party,
            peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
        );

        ctx.* = .{
            .peer = self,
            .payload = call.params,
            .inbound_caps = inbound_caps,
            .send_results_to = forwarded_plan.send_results_to,
            .send_results_to_third_party_payload = forwarded_plan.send_results_to_third_party_payload,
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
        const completion = try peer_forward_orchestration.finishForwardResolvedCall(
            Question,
            mode,
            call.question_id,
            forwarded_question_id,
            &self.forwarded_questions,
            &self.forwarded_tail_questions,
            &self.questions,
        );
        if (completion.send_take_from_other_question) {
            try self.sendReturnTakeFromOtherQuestion(call.question_id, forwarded_question_id);
        }
    }

    fn buildForwardedCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
        const ctx: *const ForwardCallContext = castCtx(*const ForwardCallContext, ctx_ptr);
        try peer_control.applyForwardedCallSendResults(
            Peer,
            ctx.peer,
            call_builder,
            ctx.send_results_to,
            ctx.send_results_to_third_party_payload,
            peer_control.setForwardedCallThirdPartyFromPayloadForPeerFn(Peer),
        );

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
        const ctx: *ForwardCallContext = castCtx(*ForwardCallContext, ctx_ptr);
        defer {
            if (ctx.send_results_to_third_party_payload) |payload| peer.allocator.free(payload);
            peer.allocator.destroy(ctx);
        }
        peer_forward_orchestration.removeForwardedQuestionForPeer(Peer, peer, ret.answer_id);
        try peer_forward_return_callbacks.handleForwardedReturnWithPeerCallbacks(
            Peer,
            cap_table.InboundCapTable,
            ForwardReturnBuildContext,
            buildForwardedReturn,
            peer,
            peer_forward_orchestration.toControlMode(ctx.mode),
            ctx.answer_id,
            ret,
            inbound_caps,
            peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            ctx.send_results_to_third_party_payload,
            sendReturnTag,
            peer_forward_orchestration.lookupForwardedQuestionForPeerFn(Peer),
            sendReturnTakeFromOtherQuestion,
            sendReturnAcceptFromThirdParty,
        );
    }

    fn buildForwardedReturn(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
        const ctx: *const ForwardReturnBuildContext = castCtx(*const ForwardReturnBuildContext, ctx_ptr);
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
        try payload_remap.clonePayloadWithRemappedCaps(
            Peer,
            self.allocator,
            self,
            builder,
            payload_builder,
            source,
            inbound_caps,
            mapInboundCapForForward,
        );
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

    pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
        // sendResultsTo routing is resolved in precedence order:
        // third-party handoff > local results-sent-elsewhere marker > normal results payload.
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

        try self.sendReturnFrameWithLoopback(answer_id, bytes);

        const copy = try self.allocator.alloc(u8, bytes.len);
        std.mem.copyForwards(u8, copy, bytes);
        try self.recordResolvedAnswer(answer_id, copy);
    }

    pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
        try peer_return_dispatch.sendReturnExceptionForPeer(
            Peer,
            self,
            answer_id,
            reason,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        try peer_return_dispatch.sendReturnTagForPeer(
            Peer,
            self,
            answer_id,
            tag,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        try peer_return_dispatch.sendReturnTakeFromOtherQuestionForPeer(
            Peer,
            self,
            answer_id,
            other_question_id,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        try peer_return_dispatch.sendReturnAcceptFromThirdPartyForPeer(
            Peer,
            self,
            answer_id,
            await_payload,
            clearSendResultsRouting,
            sendReturnFrameWithLoopback,
        );
    }

    fn clearSendResultsRouting(self: *Peer, answer_id: u32) void {
        _ = self.send_results_to_yourself.remove(answer_id);
        self.clearSendResultsToThirdParty(answer_id);
    }

    fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
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
                const ctx: *const @This() = castCtx(*const @This(), ctx_ptr);
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
        try peer_cap_lifecycle.releaseImport(
            Peer,
            self,
            import_id,
            count,
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrame),
        );
    }

    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        try peer_outbound_control.sendReleaseViaSendFrame(
            Peer,
            self,
            import_id,
            count,
            Peer.sendFrame,
        );
    }

    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
            Peer,
            self,
            question_id,
            release_result_caps,
            require_early_cancellation,
            Peer.sendFrame,
        );
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
        const send = self.transport_send orelse return error.TransportNotAttached;
        const ctx = self.transport_ctx orelse return error.TransportNotAttached;
        try send(ctx, frame);
    }

    fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
        const peer: *Peer = castCtx(*Peer, ctx);
        switch (tag) {
            .sender_hosted, .sender_promise => try peer.noteExportRef(id),
            else => {},
        }
    }

    fn noteExportRef(self: *Peer, id: u32) !void {
        try peer_cap_lifecycle.noteExportRef(
            ExportEntry,
            &self.exports,
            id,
        );
    }

    fn releaseExport(self: *Peer, id: u32, count: u32) void {
        peer_cap_lifecycle.releaseExport(
            Peer,
            ExportEntry,
            PendingCall,
            self,
            self.allocator,
            &self.exports,
            &self.pending_export_promises,
            self.bootstrap_export_id,
            id,
            count,
            peer_cap_lifecycle.clearExportPromiseForPeerFn(Peer),
            peer_promises.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
        );
    }

    fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        try peer_inbound_release.releaseInboundCaps(
            Peer,
            self.allocator,
            self,
            inbound,
            peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
            Peer.releaseResolvedImport,
            peer_outbound_control.sendReleaseViaSendFrameForPeerFn(Peer, Peer.sendFrame),
        );
    }

    fn storeResolvedImport(
        self: *Peer,
        promise_id: u32,
        cap: ?cap_table.ResolvedCap,
        embargo_id: ?u32,
        embargoed: bool,
    ) !void {
        try peer_cap_lifecycle.storeResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            cap,
            embargo_id,
            embargoed,
            Peer.releaseResolvedCap,
        );
    }

    fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
        try peer_cap_lifecycle.releaseResolvedImport(
            Peer,
            ResolvedImport,
            cap_table.ResolvedCap,
            self,
            &self.resolved_imports,
            &self.pending_embargoes,
            promise_id,
            Peer.releaseResolvedCap,
        );
    }

    fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
        switch (resolved) {
            .imported => |cap| try self.releaseImport(cap.id, 1),
            else => {},
        }
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
        try peer_cap_lifecycle.releaseResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            Peer.releaseExport,
        );
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

    fn onConnectionMessageFor(comptime ConnPtr: type) *const fn (conn: ConnPtr, frame: []const u8) anyerror!void {
        return struct {
            fn call(conn: ConnPtr, frame: []const u8) anyerror!void {
                const peer = peerFromConnection(ConnPtr, conn);
                try peer.handleFrame(frame);
            }
        }.call;
    }

    fn onConnectionErrorFor(comptime ConnPtr: type) *const fn (conn: ConnPtr, err: anyerror) void {
        return struct {
            fn call(conn: ConnPtr, err: anyerror) void {
                const peer = peerFromConnection(ConnPtr, conn);
                if (peer.on_error) |cb| cb(peer, err);
            }
        }.call;
    }

    fn onConnectionCloseFor(comptime ConnPtr: type) *const fn (conn: ConnPtr) void {
        return struct {
            fn call(conn: ConnPtr) void {
                const peer = peerFromConnection(ConnPtr, conn);
                if (peer.on_close) |cb| cb(peer);
            }
        }.call;
    }

    fn peerFromConnection(comptime ConnPtr: type, conn: ConnPtr) *Peer {
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

        try peer_dispatch.dispatchDecodedForPeer(
            Peer,
            self,
            frame,
            &decoded,
            Peer.handleUnimplemented,
            Peer.handleAbort,
            Peer.handleBootstrap,
            Peer.handleCall,
            Peer.handleReturn,
            Peer.handleFinish,
            Peer.handleRelease,
            Peer.handleResolve,
            Peer.handleDisembargo,
            Peer.handleProvide,
            Peer.handleAccept,
            Peer.handleJoin,
            Peer.handleThirdPartyAnswer,
            Peer.sendUnimplemented,
        );
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
        try peer_control.handleUnimplemented(
            Peer,
            self,
            unimplemented,
            peer_control.handleUnimplementedQuestionForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        try peer_control.handleAbort(self.allocator, &self.last_remote_abort_reason, abort);
    }

    fn handleBootstrap(self: *Peer, bootstrap: protocol.Bootstrap) !void {
        try peer_control.handleBootstrap(
            Peer,
            self,
            self.allocator,
            bootstrap,
            self.bootstrap_export_id,
            noteExportRef,
            sendReturnException,
            sendFrame,
            recordResolvedAnswer,
        );
    }

    fn handleFinish(self: *Peer, finish: protocol.Finish) !void {
        try peer_control.handleFinish(
            Peer,
            self,
            finish.question_id,
            finish.release_result_caps,
            peer_forward_orchestration.removeSendResultsToYourselfForPeerFn(Peer),
            clearSendResultsToThirdParty,
            peer_provides_state.clearProvideForPeerFn(
                Peer,
                ProvideEntry,
                ProvideTarget,
                ProvideTarget.deinit,
            ),
            peer_join_state.clearPendingJoinQuestionForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                ProvideTarget.deinit,
                JoinState.deinit,
            ),
            peer_embargo_accepts.clearPendingAcceptQuestionForPeerFn(
                Peer,
                PendingEmbargoedAccept,
            ),
            peer_forward_orchestration.takeForwardedTailQuestionForPeerFn(Peer),
            peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.takeResolvedAnswerFrameForPeerFn(Peer),
            releaseResultCaps,
            peer_control.freeOwnedFrameForPeerFn(Peer),
        );
    }

    fn handleRelease(self: *Peer, release: protocol.Release) !void {
        peer_control.handleRelease(Peer, self, release, releaseExport);
    }

    fn handleResolve(self: *Peer, resolve: protocol.Resolve) !void {
        try peer_control.handleResolve(
            Peer,
            self,
            resolve,
            peer_control.hasKnownResolvePromiseForPeerFn(Peer),
            peer_control.resolveCapDescriptorForPeerFn(Peer),
            releaseResolvedCap,
            peer_control.allocateEmbargoIdForPeerFn(Peer),
            peer_control.rememberPendingEmbargoForPeerFn(Peer),
            peer_outbound_control.sendDisembargoSenderLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            storeResolvedImport,
        );
    }

    fn handleDisembargo(self: *Peer, disembargo: protocol.Disembargo) !void {
        try peer_control.handleDisembargo(
            Peer,
            self,
            disembargo,
            peer_outbound_control.sendDisembargoReceiverLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.takePendingEmbargoPromiseForPeerFn(Peer),
            peer_control.clearResolvedImportEmbargoForPeerFn(Peer),
            peer_embargo_accepts.releaseEmbargoedAcceptsForPeerFn(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            ),
        );
    }

    fn makeProvideTarget(self: *Peer, resolved: cap_table.ResolvedCap) !ProvideTarget {
        return switch (resolved) {
            .none => error.PromisedAnswerMissing,
            .imported => |cap| .{ .cap_id = cap.id },
            .exported => |cap| .{ .cap_id = cap.id },
            .promised => |promised| .{
                .promised = try cap_table.OwnedPromisedAnswer.fromPromised(self.allocator, promised),
            },
        };
    }

    fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
        try peer_provide_join_orchestration.handleProvide(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            self.allocator,
            provide,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureProvideRecipientForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.resolveProvideTargetForPeerFn(
                Peer,
                peer_control.resolveProvideImportedCapForPeerFn(Peer),
                peer_control.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
        );
    }

    fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
        try peer_provide_join_orchestration.handleAccept(
            Peer,
            ProvideEntry,
            ProvideTarget,
            self,
            accept,
            &self.provides_by_question,
            &self.provides_by_key,
            peer_provide_join_orchestration.captureAcceptProvisionForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_embargo_accepts.queueEmbargoedAcceptForPeerFn(Peer, PendingEmbargoedAccept),
            Peer.sendReturnProvidedTarget,
            Peer.sendReturnException,
        );
    }

    fn handleJoin(self: *Peer, join: protocol.Join) !void {
        try peer_provide_join_orchestration.handleJoin(
            Peer,
            JoinKeyPart,
            JoinState,
            PendingJoinQuestion,
            ProvideTarget,
            self,
            self.allocator,
            join,
            &self.pending_joins,
            &self.pending_join_questions,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.resolveProvideTargetForPeerFn(
                Peer,
                peer_control.resolveProvideImportedCapForPeerFn(Peer),
                peer_control.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
            ),
            makeProvideTarget,
            ProvideTarget.deinit,
            JoinState.init,
            peer_join_state.completeJoinForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                provideTargetsEqual,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
                JoinState.deinit,
            ),
            Peer.sendReturnException,
        );
    }

    fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
        try peer_third_party_adoption.handleThirdPartyAnswer(
            Peer,
            PendingThirdPartyAwait,
            self.allocator,
            self,
            third_party_answer,
            &self.pending_third_party_awaits,
            &self.pending_third_party_answers,
            peer_third_party_adoption.captureThirdPartyCompletionForPeerFn(
                Peer,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            ),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_third_party_adoption.adoptPendingAwaitEntryForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                adoptThirdPartyAnswer,
            ),
        );
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        try peer_call_orchestration.handleCallForPeer(
            Peer,
            self,
            frame,
            call,
            peer_call_orchestration.handleCallImportedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.queuePromiseExportCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
                peer_control.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.sendReturnException,
                Peer.handleResolvedCall,
            ),
            peer_call_orchestration.handleCallPromisedTargetForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.resolvePromisedAnswer,
                peer_call_targets.hasUnresolvedPromiseExportForPeerFn(Peer),
                Peer.queuePromisedCall,
                Peer.queuePromiseExportCall,
                Peer.sendReturnException,
                Peer.handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            ),
        );
    }

    fn handleResolvedCall(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try peer_control.handleResolvedCall(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            inbound_caps,
            resolved,
            peer_call_orchestration.handleResolvedExportedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                peer_control.noteCallSendResultsForPeerFn(
                    Peer,
                    Peer.noteSendResultsToYourself,
                    Peer.noteSendResultsToThirdParty,
                ),
                Peer.handleResolvedCall,
                Peer.sendReturnException,
            ),
            peer_forward_orchestration.forwardResolvedCallForPeerFn(
                Peer,
                cap_table.InboundCapTable,
                Peer.forwardResolvedCall,
            ),
            Peer.sendReturnException,
        );
    }

    fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
        try peer_promises.recordResolvedAnswer(
            Peer,
            ResolvedAnswer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            question_id,
            frame,
            &self.resolved_answers,
            &self.pending_promises,
            Peer.resolvePromisedAnswer,
            Peer.sendReturnException,
            Peer.handleResolvedCall,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try peer_promises.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_promises,
            question_id,
            frame,
            inbound_caps,
        );
    }

    fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
        try peer_promises.queuePendingCall(
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            &self.pending_export_promises,
            export_id,
            frame,
            inbound_caps,
        );
    }

    fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
        try peer_promises.replayResolvedPromiseExport(
            Peer,
            PendingCall,
            cap_table.InboundCapTable,
            self.allocator,
            self,
            export_id,
            resolved,
            &self.pending_export_promises,
            Peer.handleResolvedCall,
            Peer.sendReturnException,
            Peer.releaseInboundCaps,
            peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
        );
    }

    fn adoptThirdPartyAnswer(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) anyerror!void {
        try peer_third_party_adoption.adoptThirdPartyAnswer(
            Peer,
            Question,
            self,
            question_id,
            adopted_answer_id,
            question,
            &self.questions,
            &self.adopted_third_party_answers,
            &self.pending_third_party_returns,
            peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            peer_control.freeOwnedFrameForPeerFn(Peer),
            peer_third_party_returns.handlePendingReturnFrameForPeerFn(
                Peer,
                Peer.handleReturn,
            ),
        );
    }

    fn handleReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        try peer_return_orchestration.handleReturn(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            frame,
            ret,
            peer_return_orchestration.fetchRemoveQuestionForPeerFn(Peer, Question),
            peer_return_orchestration.handleMissingReturnQuestionForPeerFn(Peer),
            peer_return_orchestration.initInboundCapsForPeerFn(Peer, cap_table.InboundCapTable),
            peer_return_orchestration.deinitInboundCapsForTypeFn(cap_table.InboundCapTable),
            peer_third_party_adoption.handleReturnAcceptFromThirdPartyForPeerFn(
                Peer,
                Question,
                PendingThirdPartyAwait,
                cap_table.InboundCapTable,
                peer_control.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                peer_control.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrame),
                adoptThirdPartyAnswer,
            ),
            peer_return_dispatch.maybeSendAutoFinishForPeerFn(
                Peer,
                Question,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            ),
            peer_return_orchestration.handleReturnRegularForPeerFn(
                Peer,
                Question,
                cap_table.InboundCapTable,
                Peer.releaseInboundCaps,
                peer_outbound_control.sendFinishViaSendFrameForPeerFn(Peer, Peer.sendFrame),
            ),
        );
    }
};
