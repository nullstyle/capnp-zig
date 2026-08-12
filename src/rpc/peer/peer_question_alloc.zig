const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const events = @import("../events.zig");
const peer_question_state = @import("./peer_question_state.zig");
const retained_question_state = @import("./retained_questions.zig");

/// Question allocation and release, extracted from `peer/mod.zig` and made
/// generic over the peer type (the JoinCoordinator extraction contract): the
/// allocateQuestion family (plain / no-restore / retained / loopback
/// variants and the shared allocateQuestionFrom core), retained-question
/// admission, question removal, and question param-export recording.
/// `peer/mod.zig` keeps every caller-visible name as a thunk on `Peer` — the
/// test_hooks-pinned removeQuestion stays a Peer method.
pub fn QuestionAlloc(comptime Peer: type) type {
    return struct {
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ensureCountLimit = Peer.ensureCountLimit;
        const msToNs = Peer.msToNsHelper;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        pub fn allocateQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            return allocateQuestionWithRestore(self, ctx, on_return, true);
        }

        /// Allocate and register a retained question as one atomic pre-send step.
        /// The outbound-question allocator can synchronously notify observers, so
        /// retained admission is checked both before allocation and immediately
        /// before registration. No Call is emitted until registration completes.
        pub fn allocateRetainedQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            try checkRetainedQuestionAdmission(self);

            // A retained callback is delivered at most once. If it reports an
            // error after observing the Return, the answer remains explicitly
            // finishable, but the question callback is not restored and replayed.
            const question_id = try allocateQuestionWithRestore(self, ctx, on_return, false);
            errdefer removeQuestion(self, question_id);
            try registerRetainedQuestion(self, question_id);
            return question_id;
        }

        pub fn checkRetainedQuestionAdmission(self: *Peer) !void {
            const retained_count = self.retained_questions.count();
            if (retained_count >= self.limits.max_retained_questions) {
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .retained_questions,
                    retained_count +| 1,
                    self.limits.max_retained_questions,
                    error.PeerLimitExceeded,
                );
                return error.PeerLimitExceeded;
            }
        }

        pub fn registerRetainedQuestion(self: *Peer, question_id: u32) !void {
            try checkRetainedQuestionAdmission(self);
            const retained_before = self.retained_questions.count();
            const question = self.questions.getPtr(question_id) orelse return error.QuestionClosed;
            question.suppress_auto_finish = true;
            question.restore_on_return_error = false;
            try self.retained_questions.register(question_id);
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .retained_questions,
                retained_before,
                self.retained_questions.count(),
                self.limits.max_retained_questions,
            );
        }

        /// Allocate an outbound question whose `on_return` callback OWNS and FREES
        /// the ctx (so the question must NOT be restored on a post-callback error —
        /// a restored copy would reference a ctx the callback already freed, a UAF at
        /// teardown). Used by `sendForwardedVineCall` (issue #56): its return callback
        /// frees the relay ctx, and in synchronous loopback the return is processed
        /// INSIDE the send, so `restore_on_return_error` must be false from creation —
        /// it cannot be cleared after the send the way async paths do.
        pub fn allocateQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            return allocateQuestionWithRestore(self, ctx, on_return, false);
        }

        /// Allocate the question id for a REFLECTED (loopback) call — one whose
        /// synthesized `Call` frame `sendCallToExport` feeds straight back into this
        /// peer's own `handleFrame` instead of writing it to the transport.
        ///
        /// On the wire, outbound question ids and inbound answer ids are INDEPENDENT
        /// namespaces: both peers legally start at 0. Reflection merges them — the
        /// id we picked as an outbound question also becomes an inbound answer id in
        /// `active_inbound_questions`/`resolved_answers`. Drawing loopback ids from
        /// `next_question_id` therefore collides with whatever the remote happens to
        /// have open, and `handleCall` rejects the reflected frame with
        /// `DuplicateQuestionId`. Cross-impl contact hit exactly that: the C++
        /// reference held its answer 0 open (awaiting `Finish`) while a cross-peer
        /// proxy reflected its first loopback call, also id 0.
        ///
        /// Loopback ids are drawn from the TOP of the space, descending, skipping
        /// anything live in either namespace. No frame carrying one of these ids is
        /// ever written to a socket, and every implementation (this one included)
        /// hands out wire question ids ascending from 0, so the two stay apart.
        pub fn allocateLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            return allocateLoopbackQuestionWithRestore(self, ctx, on_return, true);
        }

        pub fn allocateLoopbackQuestionNoRestore(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            return allocateLoopbackQuestionWithRestore(self, ctx, on_return, false);
        }

        pub fn allocateLoopbackQuestionWithRestore(
            self: *Peer,
            ctx: *anyopaque,
            on_return: QuestionCallback,
            restore_on_return_error: bool,
        ) !u32 {
            const scan_start = self.next_loopback_question_id;
            while (self.questions.contains(self.next_loopback_question_id) or
                self.retained_questions.containsLogicalOrWire(self.next_loopback_question_id) or
                (try self.inboundQuestionIdInUse(self.next_loopback_question_id)))
            {
                self.next_loopback_question_id -%= 1;
                if (self.next_loopback_question_id == scan_start) return error.QuestionIdExhausted;
            }
            const question_id = try allocateQuestionFrom(
                self,
                &self.next_loopback_question_id,
                ctx,
                on_return,
                restore_on_return_error,
            );
            // The shared allocator advances its cursor upward; walk it back down so
            // loopback ids keep descending from the top of the space.
            self.next_loopback_question_id = question_id -% 1;
            return question_id;
        }

        pub fn allocateRetainedLoopbackQuestion(self: *Peer, ctx: *anyopaque, on_return: QuestionCallback) !u32 {
            try checkRetainedQuestionAdmission(self);
            const question_id = try allocateLoopbackQuestion(self, ctx, on_return);
            errdefer removeQuestion(self, question_id);
            try registerRetainedQuestion(self, question_id);
            return question_id;
        }

        pub fn allocateQuestionWithRestore(
            self: *Peer,
            ctx: *anyopaque,
            on_return: QuestionCallback,
            restore_on_return_error: bool,
        ) !u32 {
            return allocateQuestionFrom(self, &self.next_question_id, ctx, on_return, restore_on_return_error);
        }

        pub fn allocateQuestionFrom(
            self: *Peer,
            cursor: *u32,
            ctx: *anyopaque,
            on_return: QuestionCallback,
            restore_on_return_error: bool,
        ) !u32 {
            const questions_before = self.questions.count();
            try ensureCountLimit(false, questions_before, self.limits.max_outbound_questions);
            var deadline_ns: ?i64 = null;
            var started_ns: ?i64 = null;
            if (self.clockNow()) |now| {
                started_ns = now;
                if (self.timeouts.default_call_timeout_ms) |ms| {
                    deadline_ns = now + msToNs(ms);
                }
            }
            const question_id = try peer_question_state.allocateQuestionExcluding(
                Question,
                retained_question_state.Registry,
                &self.questions,
                &self.retained_questions,
                retained_question_state.Registry.containsLogicalOrWire,
                cursor,
                .{
                    .ctx = ctx,
                    .on_return = on_return,
                    .is_loopback = false,
                    .deadline_ns = deadline_ns,
                    .started_ns = started_ns,
                    .restore_on_return_error = restore_on_return_error,
                },
            );
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .outbound_questions,
                questions_before,
                self.questions.count(),
                self.limits.max_outbound_questions,
            );
            return question_id;
        }

        pub fn removeQuestion(self: *Peer, question_id: u32) void {
            if (self.questions.remove(question_id)) {
                _ = self.retained_questions.retireLogicalOrWire(question_id);
                // The question is being discarded without a wire Return (send
                // rollback, loopback cancel, test drain): free any param-export
                // record without spending the refs.
                freeQuestionParamExports(self, question_id);
            }
            if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
                self.completeShutdown();
            }
        }

        pub fn removeQuestionAndDeinit(self: *Peer, question_id: u32) void {
            if (self.questions.fetchRemove(question_id)) |removed| {
                if (removed.value.deinit_ctx) |deinit_ctx| {
                    deinit_ctx(self.allocator, removed.value.ctx);
                }
                freeQuestionParamExports(self, question_id);
                _ = self.retained_questions.retireLogicalOrWire(question_id);
            }
            if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
                self.completeShutdown();
            }
        }

        /// Record, under `question_id`, the wire refs an outbound Call's params
        /// just took on our exports (the senderHosted/senderPromise entries of
        /// `effects.callback_applied`, one ref per occurrence), so the inbound
        /// Return can spend them when it carries `releaseParamCaps = true`.
        ///
        /// Called by the wire senders BEFORE the frame is sent: OOM propagates
        /// and the senders' errdefers (question removal — which frees the record
        /// — plus cap-effects rollback) undo everything, so no partial record
        /// can survive a failed send.
        pub fn recordQuestionParamExports(
            self: *Peer,
            question_id: u32,
            entries: []const cap_table.OutboundEntry,
        ) !void {
            var ids: std.ArrayList(u32) = .empty;
            errdefer ids.deinit(self.allocator);
            for (entries) |entry| {
                switch (entry.tag) {
                    .senderHosted, .senderPromise => try ids.append(self.allocator, entry.id),
                    else => {},
                }
            }
            if (ids.items.len == 0) {
                ids.deinit(self.allocator);
                return;
            }
            const slot = try self.question_param_export_refs.getOrPut(question_id);
            if (slot.found_existing) {
                // Question ids are never reused while outstanding, so a stale
                // record here is a bookkeeping bug; free it rather than leak.
                log.warn("replacing stale param-export record for question {}", .{question_id});
                slot.value_ptr.deinit(self.allocator);
            }
            slot.value_ptr.* = ids;
        }

        /// Free (without spending) the param-export record of a question that
        /// died without a wire Return — sender rollback, loopback cancel, drain,
        /// or deinit. The still-held refs reconcile at transport teardown,
        /// exactly as they did before records existed.
        pub fn freeQuestionParamExports(self: *Peer, question_id: u32) void {
            if (self.question_param_export_refs.fetchRemove(question_id)) |removed| {
                var ids = removed.value;
                ids.deinit(self.allocator);
            }
        }
    };
}
