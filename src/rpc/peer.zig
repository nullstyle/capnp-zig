const std = @import("std");
const protocol = @import("protocol.zig");
const cap_table = @import("cap_table.zig");
const message = @import("../message.zig");
const promised_answer_copy = @import("promised_answer_copy.zig");
const peer_dispatch = @import("peer/peer_dispatch.zig");
const peer_control = @import("peer/peer_control.zig");
const peer_call_targets = @import("peer/call/peer_call_targets.zig");
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
const peer_call_orchestration = @import("peer/call/peer_call_orchestration.zig");
const peer_return_frames = @import("peer/return/peer_return_frames.zig");
const peer_return_orchestration = @import("peer/return/peer_return_orchestration.zig");
const peer_third_party_adoption = @import("peer/third_party/peer_third_party_adoption.zig");
const peer_return_dispatch = @import("peer/return/peer_return_dispatch.zig");
const peer_third_party_returns = @import("peer/third_party/peer_third_party_returns.zig");

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

const StoredPromisedAnswer = struct {
    question_id: u32,
    ops: []protocol.PromisedAnswerOp,

    fn fromPromised(allocator: std.mem.Allocator, promised: protocol.PromisedAnswer) !StoredPromisedAnswer {
        const copied_ops = try promised_answer_copy.cloneOpsFromPromised(allocator, promised);

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

        const forwarded_plan = try peer_forward_orchestration.buildForwardCallPlan(
            Peer,
            self,
            mode,
            call.send_results_to.third_party,
            captureAnyPointerPayloadForControl,
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
            setForwardedCallThirdPartyFromPayloadForControl,
        );

        const payload_builder = try call_builder.payloadBuilder();
        try ctx.peer.clonePayloadWithRemappedCaps(
            call_builder.call.builder,
            payload_builder,
            ctx.payload,
            ctx.inbound_caps,
        );
    }

    fn setForwardedCallThirdPartyFromPayloadForControl(
        self: *Peer,
        call_builder: *protocol.CallBuilder,
        payload: []const u8,
    ) !void {
        var msg = try message.Message.init(self.allocator, payload);
        defer msg.deinit();
        const third_party = try msg.getRootAnyPointer();
        try call_builder.setSendResultsToThirdParty(third_party);
    }

    fn captureAnyPointerPayloadForControl(self: *Peer, ptr: ?message.AnyPointerReader) !?[]u8 {
        return captureAnyPointerPayload(self.allocator, ptr);
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
            captureAnyPointerPayloadForControl,
            freeCapturedPayloadForControl,
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
        self.clearSendResultsRouting(answer_id);
        const bytes = try peer_return_frames.buildReturnExceptionFrame(self.allocator, answer_id, reason);
        defer self.allocator.free(bytes);
        try self.sendReturnFrameWithLoopback(answer_id, bytes);
    }

    fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
        self.clearSendResultsRouting(answer_id);
        const bytes = try peer_return_frames.buildReturnTagFrame(self.allocator, answer_id, tag);
        defer self.allocator.free(bytes);
        try self.sendReturnFrameWithLoopback(answer_id, bytes);
    }

    fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
        self.clearSendResultsRouting(answer_id);
        const bytes = try peer_return_frames.buildReturnTakeFromOtherQuestionFrame(
            self.allocator,
            answer_id,
            other_question_id,
        );
        defer self.allocator.free(bytes);
        try self.sendReturnFrameWithLoopback(answer_id, bytes);
    }

    fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
        self.clearSendResultsRouting(answer_id);
        const bytes = try peer_return_frames.buildReturnAcceptFromThirdPartyFrame(
            self.allocator,
            answer_id,
            await_payload,
        );
        defer self.allocator.free(bytes);
        try self.sendReturnFrameWithLoopback(answer_id, bytes);
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
            releaseImportRefForCapLifecycle,
            releaseResolvedImportForCapLifecycle,
            sendReleaseForCapLifecycle,
        );
    }

    pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
        try self.sendRelease(import_id, count);
    }

    pub fn sendFinishForHost(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        try self.sendFinishWithFlags(question_id, release_result_caps, require_early_cancellation);
    }

    fn releaseImportRefForCapLifecycle(self: *Peer, import_id: u32) bool {
        return self.caps.releaseImport(import_id);
    }

    fn releaseResolvedImportForCapLifecycle(self: *Peer, promise_id: u32) !void {
        try self.releaseResolvedImport(promise_id);
    }

    fn sendReleaseForCapLifecycle(self: *Peer, import_id: u32, count: u32) !void {
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
            clearExportPromiseForCapLifecycle,
            deinitPendingCallForCapLifecycle,
        );
    }

    fn clearExportPromiseForCapLifecycle(self: *Peer, id: u32) void {
        self.caps.clearExportPromise(id);
    }

    fn deinitPendingCallForCapLifecycle(self: *Peer, pending_call: *PendingCall, allocator: std.mem.Allocator) void {
        _ = self;
        pending_call.caps.deinit();
        allocator.free(pending_call.frame);
    }

    fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
        try peer_inbound_release.releaseInboundCaps(
            Peer,
            self.allocator,
            self,
            inbound,
            releaseImportForInboundReleaseControl,
            releaseResolvedImportForInboundReleaseControl,
            sendReleaseForInboundReleaseControl,
        );
    }

    fn releaseImportForInboundReleaseControl(self: *Peer, import_id: u32) bool {
        return self.caps.releaseImport(import_id);
    }

    fn releaseResolvedImportForInboundReleaseControl(self: *Peer, promise_id: u32) !void {
        try self.releaseResolvedImport(promise_id);
    }

    fn sendReleaseForInboundReleaseControl(self: *Peer, import_id: u32, count: u32) !void {
        try self.sendRelease(import_id, count);
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
            releaseResolvedCapForCapLifecycle,
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
            releaseResolvedCapForCapLifecycle,
        );
    }

    fn releaseResolvedCapForCapLifecycle(self: *Peer, resolved: cap_table.ResolvedCap) !void {
        try self.releaseResolvedCap(resolved);
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
        try self.sendFinishWithFlags(question_id, release_result_caps, false);
    }

    fn sendFinishWithFlags(
        self: *Peer,
        question_id: u32,
        release_result_caps: bool,
        require_early_cancellation: bool,
    ) !void {
        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();
        try builder.buildFinish(question_id, release_result_caps, require_early_cancellation);
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
        try peer_cap_lifecycle.releaseResultCaps(
            Peer,
            self,
            self.allocator,
            frame,
            releaseExportForCapLifecycleById,
        );
    }

    fn releaseExportForCapLifecycleById(self: *Peer, id: u32, count: u32) void {
        self.releaseExport(id, count);
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

        switch (peer_dispatch.route(decoded.tag)) {
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
            .unknown => {
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
        try peer_control.handleUnimplemented(Peer, self, unimplemented, handleUnimplementedQuestion);
    }

    fn handleUnimplementedQuestion(self: *Peer, question_id: u32) !void {
        try peer_control.handleUnimplementedQuestion(Peer, self, question_id, handleControlReturn);
    }

    fn handleAbort(self: *Peer, abort: protocol.Abort) !void {
        try peer_control.handleAbort(self.allocator, &self.last_remote_abort_reason, abort);
    }

    fn handleControlReturn(self: *Peer, frame: []const u8, ret: protocol.Return) anyerror!void {
        try self.handleReturn(frame, ret);
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
            sendFinish,
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
            sendDisembargoSenderLoopback,
            storeResolvedImport,
        );
    }

    fn handleDisembargo(self: *Peer, disembargo: protocol.Disembargo) !void {
        try peer_control.handleDisembargo(
            Peer,
            self,
            disembargo,
            sendDisembargoReceiverLoopback,
            peer_control.takePendingEmbargoPromiseForPeerFn(Peer),
            peer_control.clearResolvedImportEmbargoForPeerFn(Peer),
            peer_embargo_accepts.releaseEmbargoedAcceptsForPeerFn(
                Peer,
                PendingEmbargoedAccept,
                ProvideEntry,
                ProvideTarget,
                sendReturnProvidedTargetForControl,
                sendReturnExceptionForControl,
            ),
        );
    }

    fn resolveProvideTarget(self: *Peer, target: protocol.MessageTarget) !cap_table.ResolvedCap {
        return peer_control.resolveProvideTarget(
            Peer,
            self,
            target,
            resolveProvideImportedCapForControl,
            resolveProvidePromisedAnswerForControl,
        );
    }

    fn resolveProvideImportedCapForControl(self: *Peer, export_id: u32) !cap_table.ResolvedCap {
        const exported_entry = self.exports.getEntry(export_id) orelse return error.UnknownExport;

        if (exported_entry.value_ptr.is_promise) {
            const resolved = exported_entry.value_ptr.resolved orelse return error.PromiseUnresolved;
            if (resolved == .none) return error.PromiseBroken;
            return resolved;
        }
        return .{ .exported = .{ .id = export_id } };
    }

    fn resolveProvidePromisedAnswerForControl(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        const resolved = try self.resolvePromisedAnswer(promised);
        if (resolved == .none) return error.PromisedAnswerMissing;
        return resolved;
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

    fn captureProvideRecipientForControl(self: *Peer, provide: protocol.Provide) !?[]u8 {
        return captureAnyPointerPayload(self.allocator, provide.recipient);
    }

    fn captureAcceptProvisionForControl(self: *Peer, accept: protocol.Accept) !?[]u8 {
        return captureAnyPointerPayload(self.allocator, accept.provision);
    }

    fn freeCapturedPayloadForControl(self: *Peer, payload: []u8) void {
        self.allocator.free(payload);
    }

    fn sendAbortForControl(self: *Peer, reason: []const u8) !void {
        try self.sendAbort(reason);
    }

    fn sendReturnProvidedTargetForControl(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
        try self.sendReturnProvidedTarget(answer_id, target);
    }

    fn sendReturnExceptionForControl(self: *Peer, question_id: u32, reason: []const u8) !void {
        try self.sendReturnException(question_id, reason);
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
            captureProvideRecipientForControl,
            freeCapturedPayloadForControl,
            sendAbortForControl,
            resolveProvideTarget,
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
            captureAcceptProvisionForControl,
            freeCapturedPayloadForControl,
            peer_embargo_accepts.queueEmbargoedAcceptForPeerFn(Peer, PendingEmbargoedAccept),
            sendReturnProvidedTargetForControl,
            sendReturnExceptionForControl,
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
            sendAbortForControl,
            resolveProvideTarget,
            makeProvideTarget,
            ProvideTarget.deinit,
            JoinState.init,
            peer_join_state.completeJoinForPeerFn(
                Peer,
                JoinState,
                PendingJoinQuestion,
                ProvideTarget,
                provideTargetsEqual,
                sendReturnProvidedTargetForControl,
                sendReturnExceptionForControl,
                JoinState.deinit,
            ),
            sendReturnExceptionForControl,
        );
    }

    fn captureThirdPartyCompletionForControl(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !?[]u8 {
        return captureAnyPointerPayload(self.allocator, third_party_answer.completion);
    }

    fn adoptPendingThirdPartyAwaitEntryForThirdPartyAdoption(
        self: *Peer,
        adopted_answer_id: u32,
        pending_await: PendingThirdPartyAwait,
    ) !void {
        try self.adoptThirdPartyAnswer(
            pending_await.question_id,
            adopted_answer_id,
            pending_await.question,
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
            captureThirdPartyCompletionForControl,
            freeCapturedPayloadForControl,
            sendAbortForControl,
            adoptPendingThirdPartyAwaitEntryForThirdPartyAdoption,
        );
    }

    fn handleResolvedExportedForControl(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        export_id: u32,
    ) !void {
        const exported_entry = self.exports.getEntry(export_id) orelse {
            try self.sendReturnException(call.question_id, "unknown promised capability");
            return;
        };
        const handler = exported_entry.value_ptr.handler;
        try peer_call_orchestration.handleResolvedExportedCall(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            inbound_caps,
            true,
            exported_entry.value_ptr.is_promise,
            exported_entry.value_ptr.resolved,
            if (handler) |h| h.ctx else null,
            if (handler) |h| h.on_call else null,
            noteCallSendResultsForCallOrchestration,
            handleResolvedCallForCallOrchestration,
            sendReturnExceptionForControl,
        );
    }

    fn forwardResolvedCallForControl(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
        mode: peer_control.ForwardResolvedMode,
    ) !void {
        const forward_mode: ForwardReturnMode = switch (mode) {
            .sent_elsewhere => .sent_elsewhere,
            .propagate_results_sent_elsewhere => .propagate_results_sent_elsewhere,
            .propagate_accept_from_third_party => .propagate_accept_from_third_party,
        };
        try self.forwardResolvedCall(call, inbound_caps, resolved, forward_mode);
    }

    fn resolvePromisedAnswerForCallTargetControl(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        return self.resolvePromisedAnswer(promised);
    }

    fn hasUnresolvedPromiseExportForCallTargetControl(self: *Peer, export_id: u32) bool {
        if (self.exports.getEntry(export_id)) |entry| {
            return entry.value_ptr.is_promise and entry.value_ptr.resolved == null;
        }
        return false;
    }

    fn noteCallSendResultsForCallOrchestration(self: *Peer, call: protocol.Call) !void {
        try peer_control.noteCallSendResults(
            Peer,
            self,
            call,
            noteSendResultsToYourself,
            noteSendResultsToThirdParty,
        );
    }

    fn handleResolvedCallForCallOrchestration(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try self.handleResolvedCall(call, inbound_caps, resolved);
    }

    fn handleCallImportedTarget(self: *Peer, frame: []const u8, call: protocol.Call, export_id: u32) !void {
        var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, call.params.cap_table, &self.caps);
        const exported_entry = self.exports.getEntry(export_id);
        const target_plan = peer_call_targets.planImportedTarget(
            exported_entry != null,
            if (exported_entry) |entry| entry.value_ptr.is_promise else false,
            if (exported_entry) |entry| entry.value_ptr.resolved else null,
            if (exported_entry) |entry| entry.value_ptr.handler != null else false,
        );

        switch (target_plan) {
            .unknown_capability => {
                try self.sendReturnException(call.question_id, "unknown capability");
                inbound_caps.deinit();
                return;
            },
            .queue_promise_export => {
                try self.queuePromiseExportCall(export_id, frame, inbound_caps);
                return;
            },
            else => {},
        }

        defer inbound_caps.deinit();
        defer self.releaseInboundCaps(&inbound_caps) catch |err| {
            if (self.on_error) |cb| cb(self, err);
        };

        const handler = if (exported_entry) |entry| entry.value_ptr.handler else null;
        try peer_call_orchestration.dispatchImportedTargetPlan(
            Peer,
            cap_table.InboundCapTable,
            self,
            call,
            &inbound_caps,
            target_plan,
            if (handler) |h| h.ctx else null,
            if (handler) |h| h.on_call else null,
            noteCallSendResultsForCallOrchestration,
            sendReturnExceptionForControl,
            handleResolvedCallForCallOrchestration,
        );
    }

    fn handleCallPromisedTarget(self: *Peer, frame: []const u8, call: protocol.Call, promised: protocol.PromisedAnswer) !void {
        var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, call.params.cap_table, &self.caps);
        const target_plan = peer_call_targets.planPromisedTarget(
            Peer,
            self,
            promised,
            resolvePromisedAnswerForCallTargetControl,
            hasUnresolvedPromiseExportForCallTargetControl,
        );

        switch (target_plan) {
            .queue_promised_call => {
                try self.queuePromisedCall(promised.question_id, frame, inbound_caps);
                return;
            },
            .queue_export_promise => |export_id| {
                try self.queuePromiseExportCall(export_id, frame, inbound_caps);
                return;
            },
            .send_exception => |err| {
                inbound_caps.deinit();
                try self.sendReturnException(call.question_id, @errorName(err));
                return;
            },
            .handle_resolved => |resolved| {
                defer inbound_caps.deinit();
                defer self.releaseInboundCaps(&inbound_caps) catch |err| {
                    if (self.on_error) |cb| cb(self, err);
                };
                try self.handleResolvedCall(call, &inbound_caps, resolved);
            },
        }
    }

    fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
        const target = try peer_call_orchestration.routeCallTarget(call);
        switch (target) {
            .imported => |export_id| {
                try self.handleCallImportedTarget(frame, call, export_id);
            },
            .promised => |promised| {
                try self.handleCallPromisedTarget(frame, call, promised);
            },
        }
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
            handleResolvedExportedForControl,
            forwardResolvedCallForControl,
            sendReturnExceptionForControl,
        );
    }

    fn resolvePromisedAnswerForPromiseControl(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
        return self.resolvePromisedAnswer(promised);
    }

    fn handleResolvedCallForPromiseControl(
        self: *Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
        resolved: cap_table.ResolvedCap,
    ) !void {
        try self.handleResolvedCall(call, inbound_caps, resolved);
    }

    fn releaseInboundCapsForPromiseControl(self: *Peer, inbound_caps: *cap_table.InboundCapTable) !void {
        try self.releaseInboundCaps(inbound_caps);
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
            resolvePromisedAnswerForPromiseControl,
            sendReturnExceptionForControl,
            handleResolvedCallForPromiseControl,
            releaseInboundCapsForPromiseControl,
            reportNonfatalErrorForControl,
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
            handleResolvedCallForPromiseControl,
            sendReturnExceptionForControl,
            releaseInboundCapsForPromiseControl,
            reportNonfatalErrorForControl,
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
            sendAbortForControl,
            peer_control.freeOwnedFrameForPeerFn(Peer),
            handlePendingThirdPartyReturnFrameForControl,
        );
    }

    fn handlePendingThirdPartyReturnFrameForControl(self: *Peer, frame: []const u8) !void {
        try peer_third_party_returns.handlePendingReturnFrame(
            Peer,
            self.allocator,
            self,
            frame,
            handleReturnFrameForThirdPartyControl,
        );
    }

    fn handleReturnFrameForThirdPartyControl(self: *Peer, frame: []const u8, ret: protocol.Return) !void {
        try self.handleReturn(frame, ret);
    }

    fn hasPendingThirdPartyReturnForControl(self: *Peer, answer_id: u32) bool {
        return peer_third_party_returns.hasPendingReturn(
            &self.pending_third_party_returns,
            answer_id,
        );
    }

    fn bufferPendingThirdPartyReturnForControl(self: *Peer, answer_id: u32, frame: []const u8) !void {
        try peer_third_party_returns.bufferPendingReturn(
            self.allocator,
            &self.pending_third_party_returns,
            answer_id,
            frame,
        );
    }

    fn captureReturnAcceptCompletionForControl(self: *Peer, await_ptr: ?message.AnyPointerReader) !?[]u8 {
        return captureAnyPointerPayload(self.allocator, await_ptr);
    }

    fn adoptThirdPartyAnswerForThirdPartyAdoption(
        self: *Peer,
        question_id: u32,
        adopted_answer_id: u32,
        question: Question,
    ) !void {
        try self.adoptThirdPartyAnswer(question_id, adopted_answer_id, question);
    }

    fn makePendingThirdPartyAwaitForThirdPartyAdoption(
        question_id: u32,
        question: Question,
    ) PendingThirdPartyAwait {
        return .{
            .question_id = question_id,
            .question = question,
        };
    }

    fn maybeSendAutoFinishForReturn(self: *Peer, question: Question, answer_id: u32, no_finish_needed: bool) void {
        if (!question.is_loopback and !question.suppress_auto_finish and !no_finish_needed) {
            self.sendFinish(answer_id, false) catch |err| {
                if (self.on_error) |cb| cb(self, err);
            };
        }
    }

    fn takeAdoptedThirdPartyAnswerOriginalForControl(self: *Peer, answer_id: u32) ?u32 {
        return peer_return_dispatch.takeAdoptedAnswerOriginal(
            &self.adopted_third_party_answers,
            answer_id,
        );
    }

    fn reportNonfatalErrorForControl(self: *Peer, err: anyerror) void {
        if (self.on_error) |cb| cb(self, err);
    }

    fn dispatchQuestionReturnForControl(
        self: *Peer,
        question: Question,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) void {
        peer_return_dispatch.dispatchQuestionReturn(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            question,
            ret,
            inbound_caps,
            invokeQuestionReturnForControl,
            reportNonfatalErrorForControl,
        );
    }

    fn invokeQuestionReturnForControl(
        question: Question,
        self: *Peer,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        try question.on_return(question.ctx, self, ret, inbound_caps);
    }

    fn releaseInboundCapsForControl(self: *Peer, inbound_caps: *const cap_table.InboundCapTable) !void {
        try peer_return_dispatch.releaseInboundCaps(
            Peer,
            cap_table.InboundCapTable,
            self,
            inbound_caps,
            releaseInboundCapsMutableForControl,
        );
    }

    fn releaseInboundCapsMutableForControl(self: *Peer, inbound_caps: *cap_table.InboundCapTable) !void {
        try self.releaseInboundCaps(inbound_caps);
    }

    fn maybeSendAutoFinishForControl(self: *Peer, question: Question, answer_id: u32, no_finish_needed: bool) void {
        self.maybeSendAutoFinishForReturn(question, answer_id, no_finish_needed);
    }

    fn fetchRemoveQuestionForReturnOrchestration(self: *Peer, answer_id: u32) ?Question {
        const entry = self.questions.fetchRemove(answer_id) orelse return null;
        return entry.value;
    }

    fn handleMissingReturnQuestionForReturnOrchestration(self: *Peer, frame: []const u8, answer_id: u32) !void {
        try peer_control.handleMissingReturnQuestion(
            Peer,
            self,
            frame,
            answer_id,
            peer_control.isThirdPartyAnswerId,
            hasPendingThirdPartyReturnForControl,
            bufferPendingThirdPartyReturnForControl,
        );
    }

    fn initInboundCapsForReturnOrchestration(self: *Peer, ret: protocol.Return) !cap_table.InboundCapTable {
        const cap_list = if (ret.tag == .results and ret.results != null) ret.results.?.cap_table else null;
        return cap_table.InboundCapTable.init(self.allocator, cap_list, &self.caps);
    }

    fn deinitInboundCapsForReturnOrchestration(inbound_caps: *cap_table.InboundCapTable) void {
        inbound_caps.deinit();
    }

    fn handleReturnAcceptFromThirdPartyForReturnOrchestration(
        self: *Peer,
        answer_id: u32,
        question: Question,
        accept_from_third_party: ?message.AnyPointerReader,
        inbound_caps: *const cap_table.InboundCapTable,
    ) !void {
        _ = inbound_caps;
        try peer_third_party_adoption.handleReturnAcceptFromThirdParty(
            Peer,
            Question,
            PendingThirdPartyAwait,
            self.allocator,
            self,
            answer_id,
            question,
            accept_from_third_party,
            &self.pending_third_party_awaits,
            &self.pending_third_party_answers,
            captureReturnAcceptCompletionForControl,
            freeCapturedPayloadForControl,
            sendAbortForControl,
            adoptThirdPartyAnswerForThirdPartyAdoption,
            makePendingThirdPartyAwaitForThirdPartyAdoption,
        );
    }

    fn handleReturnRegularForReturnOrchestration(
        self: *Peer,
        question: Question,
        ret: protocol.Return,
        inbound_caps: *const cap_table.InboundCapTable,
    ) void {
        peer_control.handleReturnRegular(
            Peer,
            Question,
            cap_table.InboundCapTable,
            self,
            question,
            ret,
            inbound_caps,
            takeAdoptedThirdPartyAnswerOriginalForControl,
            dispatchQuestionReturnForControl,
            releaseInboundCapsForControl,
            reportNonfatalErrorForControl,
            maybeSendAutoFinishForControl,
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
            fetchRemoveQuestionForReturnOrchestration,
            handleMissingReturnQuestionForReturnOrchestration,
            initInboundCapsForReturnOrchestration,
            deinitInboundCapsForReturnOrchestration,
            handleReturnAcceptFromThirdPartyForReturnOrchestration,
            maybeSendAutoFinishForControl,
            handleReturnRegularForReturnOrchestration,
        );
    }
};

test "peer initDetached starts without attached transport" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    peer.start(null, null);
    try std.testing.expect(!peer.hasAttachedTransport());
}

test "peer detached sendFrame requires override or attached transport" {
    var peer = Peer.initDetached(std.testing.allocator);
    defer peer.deinit();

    try std.testing.expectError(error.TransportNotAttached, peer.sendFrame(&[_]u8{ 0x01, 0x02 }));
}

test "release batching aggregates per import id" {
    const allocator = std.testing.allocator;

    var peer = Peer.initDetached(allocator);
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
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnException(call.question_id, "loopback");
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = castCtx(*ClientCtx, ctx);
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("loopback", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
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

    var peer = Peer.initDetached(allocator);
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

    var peer = Peer.initDetached(allocator);
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

    var peer = Peer.initDetached(allocator);
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.canceled, ret.tag);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.take_from_other_question, ret.tag);
            state.referenced_answer = ret.take_from_other_question orelse return error.MissingQuestionId;
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded resultsSentElsewhere unsupported", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
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

test "forwarded return translate mode missing payload sends exception" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded return missing payload", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 350;
    const local_forwarded_question_id: u32 = 351;

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
        .tag = .results,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };
    try Peer.onForwardedReturn(forward_ctx, &peer, ret, &inbound);

    try std.testing.expect(callback_ctx.seen);
    try std.testing.expect(!peer.forwarded_questions.contains(local_forwarded_question_id));
}

test "forwarded return propagate-results mode rejects takeFromOtherQuestion" {
    const allocator = std.testing.allocator;

    const CallbackCtx = struct {
        seen: bool = false,
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("forwarded takeFromOtherQuestion unsupported", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const upstream_answer_id: u32 = 352;
    const local_forwarded_question_id: u32 = 353;

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
        .mode = .propagate_results_sent_elsewhere,
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
        .take_from_other_question = 900,
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            try std.testing.expect(ret.exception == null);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
        }
    };

    var peer = Peer.initDetached(allocator);
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

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.results_sent_elsewhere, ret.tag);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            const await_ptr = ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
            try std.testing.expectEqualStrings("third-party-destination", try await_ptr.getText());
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = castCtx(*ClientCtx, ctx);
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.results_sent_elsewhere, ret.tag);
            try std.testing.expect(ret.results == null);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const build: *const BuildCtx = castCtx(*const BuildCtx, ctx);
            try call.setSendResultsToThirdParty(build.destination);
        }

        fn buildResults(ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
            _ = ctx;
            _ = try ret.initResultsStruct(0, 0);
            try ret.setEmptyCapTable();
        }

        fn onCall(ctx: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const server: *ServerCtx = castCtx(*ServerCtx, ctx);
            server.called = true;
            try peer.sendReturnResults(call.question_id, server, buildResults);
        }

        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const client: *ClientCtx = castCtx(*ClientCtx, ctx);
            client.returned = true;
            try std.testing.expectEqual(protocol.ReturnTag.accept_from_third_party, ret.tag);
            try std.testing.expect(ret.results == null);
            const await_ptr = ret.accept_from_third_party orelse return error.MissingThirdPartyPayload;
            try std.testing.expectEqualStrings("local-third-party", try await_ptr.getText());
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            state.reason = ex.reason;
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            state.answer_id = ret.answer_id;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            state.reason = ex.reason;
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onReturn(ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = peer;
            _ = caps;
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen += 1;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("stress-third-party", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
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

    {
        var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            ctx.count += 1;
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            ctx.question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "resolved");
        }
    };

    var peer = Peer.initDetached(allocator);
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

test "bootstrap return is recorded for promisedAnswer pipelined calls" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
        question_id: u32 = 0,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            ctx.question_id = call.question_id;
            try peer.sendReturnException(call.question_id, "ok");
        }
    };

    var peer = Peer.initDetached(allocator);
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
    _ = try peer.setBootstrap(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    const bootstrap_question_id: u32 = 41;
    {
        var bootstrap_builder = protocol.MessageBuilder.init(allocator);
        defer bootstrap_builder.deinit();
        try bootstrap_builder.buildBootstrap(bootstrap_question_id);

        const bootstrap_frame = try bootstrap_builder.finish();
        defer allocator.free(bootstrap_frame);
        try peer.handleFrame(bootstrap_frame);
    }
    try std.testing.expectEqual(@as(usize, 1), capture.frames.items.len);

    const pipelined_question_id: u32 = 42;
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(pipelined_question_id, 0xABCD, 7);
        try call.setTargetPromisedAnswer(bootstrap_question_id);
        try call.setEmptyCapTable();

        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try peer.handleFrame(call_frame);
    }

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(pipelined_question_id, server_ctx.question_id);
    try std.testing.expect(!peer.pending_promises.contains(bootstrap_question_id));
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(pipelined_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
}

test "bootstrap promisedAnswer call still resolves after bootstrap export release" {
    const allocator = std.testing.allocator;

    const ServerCtx = struct {
        called: bool = false,
    };
    const Capture = struct {
        allocator: std.mem.Allocator,
        frames: std.ArrayList([]u8),

        fn onFrame(ctx_ptr: *anyopaque, frame: []const u8) anyerror!void {
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };
    const Handlers = struct {
        fn onCall(ctx_ptr: *anyopaque, peer: *Peer, call: protocol.Call, caps: *const cap_table.InboundCapTable) anyerror!void {
            _ = caps;
            const ctx: *ServerCtx = castCtx(*ServerCtx, ctx_ptr);
            ctx.called = true;
            try peer.sendReturnException(call.question_id, "ok");
        }
    };

    var peer = Peer.initDetached(allocator);
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
    const bootstrap_export_id = try peer.setBootstrap(.{
        .ctx = &server_ctx,
        .on_call = Handlers.onCall,
    });

    const bootstrap_question_id: u32 = 101;
    {
        var bootstrap_builder = protocol.MessageBuilder.init(allocator);
        defer bootstrap_builder.deinit();
        try bootstrap_builder.buildBootstrap(bootstrap_question_id);

        const bootstrap_frame = try bootstrap_builder.finish();
        defer allocator.free(bootstrap_frame);
        try peer.handleFrame(bootstrap_frame);
    }

    {
        var release_builder = protocol.MessageBuilder.init(allocator);
        defer release_builder.deinit();
        try release_builder.buildRelease(bootstrap_export_id, 1);
        const release_frame = try release_builder.finish();
        defer allocator.free(release_frame);
        try peer.handleFrame(release_frame);
    }

    const pipelined_question_id: u32 = 102;
    {
        var call_builder = protocol.MessageBuilder.init(allocator);
        defer call_builder.deinit();
        var call = try call_builder.beginCall(pipelined_question_id, 0xCCDD, 7);
        try call.setTargetPromisedAnswer(bootstrap_question_id);
        try call.setEmptyCapTable();

        const call_frame = try call_builder.finish();
        defer allocator.free(call_frame);
        try peer.handleFrame(call_frame);
    }

    try std.testing.expect(server_ctx.called);
    try std.testing.expectEqual(@as(usize, 2), capture.frames.items.len);

    var ret_msg = try protocol.DecodedMessage.init(allocator, capture.frames.items[1]);
    defer ret_msg.deinit();
    try std.testing.expectEqual(protocol.MessageTag.return_, ret_msg.tag);
    const ret = try ret_msg.asReturn();
    try std.testing.expectEqual(pipelined_question_id, ret.answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
    const ex = ret.exception orelse return error.MissingException;
    try std.testing.expectEqualStrings("ok", ex.reason);
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
            const state: *CallbackCtx = castCtx(*CallbackCtx, ctx);
            state.seen = true;
            try std.testing.expectEqual(protocol.ReturnTag.exception, ret.tag);
            const ex = ret.exception orelse return error.MissingException;
            try std.testing.expectEqualStrings("unimplemented", ex.reason);
        }
    };

    var peer = Peer.initDetached(allocator);
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

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
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
            const state: *ServerCtx = castCtx(*ServerCtx, ctx);
            state.called += 1;
            try called_peer.sendReturnException(call.question_id, "stress-ordered");
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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
            const ctx: *@This() = castCtx(*@This(), ctx_ptr);
            const copy = try ctx.allocator.alloc(u8, frame.len);
            std.mem.copyForwards(u8, copy, frame);
            try ctx.frames.append(ctx.allocator, copy);
        }
    };

    var peer = Peer.initDetached(allocator);
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

fn queuePromisedCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(100, 0xAA55, 1);
    try call.setTargetPromisedAnswer(77);
    try call.setEmptyCapTable();
    const frame = try call_builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);
    try std.testing.expect(peer.pending_promises.contains(77));
}

test "peer queuePromisedCall path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromisedCallOomImpl, .{});
}

fn queuePromiseExportCallOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

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

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(301, 0xABCD, 2);
    try call.setTargetPromisedAnswer(promised_answer_id);
    try call.setEmptyCapTable();
    const frame = try call_builder.finish();
    defer allocator.free(frame);

    try peer.handleFrame(frame);
    try std.testing.expect(peer.pending_export_promises.contains(promise_export_id));
}

test "peer queuePromiseExportCall path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, queuePromiseExportCallOomImpl, .{});
}

fn embargoAcceptQueueOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var recipient_builder = message.MessageBuilder.init(allocator);
    defer recipient_builder.deinit();
    const recipient_root = try recipient_builder.initRootAnyPointer();
    try recipient_root.setText("oom-accept-recipient");
    const recipient_bytes = try recipient_builder.toBytes();
    defer allocator.free(recipient_bytes);

    var recipient_msg = try message.Message.init(allocator, recipient_bytes);
    defer recipient_msg.deinit();
    const recipient_ptr = try recipient_msg.getRootAnyPointer();

    var provide_builder = protocol.MessageBuilder.init(allocator);
    defer provide_builder.deinit();
    try provide_builder.buildProvide(
        910,
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

    var accept_builder = protocol.MessageBuilder.init(allocator);
    defer accept_builder.deinit();
    try accept_builder.buildAccept(911, recipient_ptr, "oom-accept-embargo");
    const accept_frame = try accept_builder.finish();
    defer allocator.free(accept_frame);
    try peer.handleFrame(accept_frame);

    try std.testing.expectEqual(@as(usize, 1), peer.pending_accepts_by_embargo.count());
    try std.testing.expectEqual(@as(usize, 1), peer.pending_accept_embargo_by_question.count());
}

test "peer embargo accept queue path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, embargoAcceptQueueOomImpl, .{});
}

fn sendResultsToThirdPartyLocalExportOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var destination_builder = message.MessageBuilder.init(allocator);
    defer destination_builder.deinit();
    const destination_root = try destination_builder.initRootAnyPointer();
    try destination_root.setText("oom-send-results-third-party");
    const destination_bytes = try destination_builder.toBytes();
    defer allocator.free(destination_bytes);
    var destination_msg = try message.Message.init(allocator, destination_bytes);
    defer destination_msg.deinit();
    const destination_ptr = try destination_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(920, 0xBEEF, 9);
    try call.setTargetImportedCap(export_id);
    try call.setSendResultsToThirdParty(destination_ptr);
    try call.setEmptyCapTable();
    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    try peer.handleFrame(call_frame);
    try std.testing.expect(peer.send_results_to_third_party.contains(920));
}

test "peer local sendResultsTo.thirdParty path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        sendResultsToThirdPartyLocalExportOomImpl,
        .{},
    );
}

fn sendResultsToYourselfLocalExportOomImpl(allocator: std.mem.Allocator) !void {
    const NoopHandler = struct {
        fn onCall(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Call,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var handler_state: u8 = 0;
    const export_id = try peer.addExport(.{
        .ctx = &handler_state,
        .on_call = NoopHandler.onCall,
    });

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(921, 0xBEEF, 10);
    try call.setTargetImportedCap(export_id);
    try call.setSendResultsToYourself();
    try call.setEmptyCapTable();
    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    try peer.handleFrame(call_frame);
    try std.testing.expect(peer.send_results_to_yourself.contains(921));
}

test "peer local sendResultsTo.yourself path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        sendResultsToYourselfLocalExportOomImpl,
        .{},
    );
}

fn bufferThirdPartyReturnOomImpl(allocator: std.mem.Allocator) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const adopted_answer_id: u32 = 0x4000_0301;

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    var ret = try ret_builder.beginReturn(adopted_answer_id, .exception);
    try ret.setException("oom-buffer-third-party-return");
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);

    try peer.handleFrame(ret_frame);
    try std.testing.expect(peer.pending_third_party_returns.contains(adopted_answer_id));
}

test "peer buffer thirdParty return path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        bufferThirdPartyReturnOomImpl,
        .{},
    );
}

fn acceptFromThirdPartyAwaitQueueOomImpl(allocator: std.mem.Allocator) !void {
    const Callback = struct {
        fn onReturn(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const original_answer_id: u32 = 930;
    var callback_ctx: u8 = 0;
    try peer.questions.put(original_answer_id, .{
        .ctx = &callback_ctx,
        .on_return = Callback.onReturn,
        .is_loopback = true,
    });

    var completion_builder = message.MessageBuilder.init(allocator);
    defer completion_builder.deinit();
    const completion_root = try completion_builder.initRootAnyPointer();
    try completion_root.setText("oom-await-queue");
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
    try std.testing.expectEqual(@as(usize, 1), peer.pending_third_party_awaits.count());
    try std.testing.expect(!peer.questions.contains(original_answer_id));
}

test "peer awaitFromThirdParty queue path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        acceptFromThirdPartyAwaitQueueOomImpl,
        .{},
    );
}

fn forwardResolvedCallThirdPartyContextOomImpl(allocator: std.mem.Allocator) !void {
    const Sink = struct {
        fn onFrame(_: *anyopaque, _: []const u8) anyerror!void {}
    };

    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    var sink_ctx: u8 = 0;
    peer.setSendFrameOverride(&sink_ctx, Sink.onFrame);

    var inbound = try cap_table.InboundCapTable.init(allocator, null, &peer.caps);
    defer inbound.deinit();

    var third_builder = message.MessageBuilder.init(allocator);
    defer third_builder.deinit();
    const third_root = try third_builder.initRootAnyPointer();
    try third_root.setText("oom-forward-context-third-party");
    const third_bytes = try third_builder.toBytes();
    defer allocator.free(third_bytes);
    var third_msg = try message.Message.init(allocator, third_bytes);
    defer third_msg.deinit();
    const third_ptr = try third_msg.getRootAnyPointer();

    var call_builder = protocol.MessageBuilder.init(allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(940, 0xCAFE, 1);
    try call.setTargetImportedCap(77);
    try call.setSendResultsToThirdParty(third_ptr);
    try call.setEmptyCapTable();
    const call_frame = try call_builder.finish();
    defer allocator.free(call_frame);

    var call_decoded = try protocol.DecodedMessage.init(allocator, call_frame);
    defer call_decoded.deinit();
    const parsed_call = try call_decoded.asCall();

    try peer.handleResolvedCall(parsed_call, &inbound, .{ .imported = .{ .id = 77 } });

    const forwarded_question_id = blk: {
        var it = peer.forwarded_questions.iterator();
        const entry = it.next() orelse return error.UnknownQuestion;
        break :blk entry.key_ptr.*;
    };
    const question = peer.questions.get(forwarded_question_id) orelse return error.UnknownQuestion;
    const fwd_ctx: *const ForwardCallContext = @ptrCast(@alignCast(question.ctx));
    try std.testing.expectEqual(protocol.SendResultsToTag.third_party, fwd_ctx.send_results_to);
    try std.testing.expect(fwd_ctx.send_results_to_third_party_payload != null);

    var ret_builder = protocol.MessageBuilder.init(allocator);
    defer ret_builder.deinit();
    _ = try ret_builder.beginReturn(forwarded_question_id, .results_sent_elsewhere);
    const ret_frame = try ret_builder.finish();
    defer allocator.free(ret_frame);
    try peer.handleFrame(ret_frame);
}

test "peer forwardResolvedCall third-party context path propagates OOM without leaks" {
    try std.testing.checkAllAllocationFailures(
        std.testing.allocator,
        forwardResolvedCallThirdPartyContextOomImpl,
        .{},
    );
}
