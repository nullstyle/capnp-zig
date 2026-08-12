const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const provide_accept_join = @import("../provide_accept_join.zig");
const third_party = @import("../third_party.zig");

/// Experimental L3 origination senders, extracted from `peer/mod.zig` and
/// made generic over the peer type (the JoinCoordinator extraction
/// contract): Provide (incl. retained-answer form and its no-Return
/// callback), Accept, experimental Join with auto-Finish, ThirdPartyAnswer
/// id allocation + send, and pending third-party await registration.
/// `peer/mod.zig` keeps every caller-visible name as a thunk on `Peer`.
pub fn ProvideOrigination(comptime Peer: type) type {
    return struct {
        const ProvideTarget = state.ProvideTarget;
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const PendingThirdPartyAwait = state.PendingThirdPartyAwait(Question);
        const ProvideHandle = Peer.ProvideHandleRecord;
        const ProvideOriginationTarget = Peer.ProvideOriginationTargetRecord;
        const captureAnyPointerPayload = Peer.captureAnyPointerPayload;
        const third_party_answer_id_base: u32 = Peer.third_party_answer_id_base_value;
        const third_party_answer_id_limit: u32 = Peer.third_party_answer_id_limit_value;
        const vineRejectingCall = Peer.vineRejectingCall;
        const ensureCountLimit = Peer.ensureCountLimit;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        /// No-op Return callback for a held-open Provide question. A `Provide`
        /// receives NO `Return` (rpc.capnp:834-847); the question is registered only
        /// to reserve its id and to be Finished later (on vine release or teardown).
        /// The single case that invokes this is the shutdown drain delivering a
        /// synthetic local exception, which the held-open provision safely ignores.
        pub fn onProvideNoReturn(
            _: *anyopaque,
            _: *Peer,
            _: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {}

        /// Originate a three-party handoff: hand the capability named by
        /// `provided_target` (a `MessageTarget` local to the vat this peer is
        /// connected to — the HOST of the provided cap, VatC in the spec) to a third
        /// party (VatA), so VatA can pick it up directly with an `Accept`.
        ///
        /// Message direction follows rpc.capnp: the `Provide` goes to the HOST of
        /// the provided cap (this `self` peer = the VatB↔VatC connection). The vine
        /// export and the `thirdPartyHosted` descriptor, by contrast, belong on the
        /// host-of-recipient connection (`host_of_recipient` = the VatB↔VatA
        /// connection), since that is where VatA takes its wire reference and where
        /// the vine's later `Release` arrives.
        ///
        /// Steps:
        ///   1. Register a held-open Provide question on `self` (no Return; Finished
        ///      when the recipient releases the vine — see `handleRelease`).
        ///   2. Mint a vine export on `host_of_recipient` and mark it
        ///      third-party-hosted with `contact_payload` (the `ThirdPartyToContact`)
        ///      so the next descriptor emitted for the vine resolves to
        ///      `thirdPartyHosted{ id = contact, vineId }`.
        ///   3. Record the vine → Provide-question coupling on `host_of_recipient`.
        ///   4. Send the `Provide` to `self` (VatC) with `recipient` = the
        ///      `ThirdPartyToAwait`.
        ///
        /// Returns the vine id + Provide question id. The caller then drives the
        /// `thirdPartyHosted` emission by sending `host_of_recipient` a payload that
        /// carries the vine export (which is marked); the recipient picks it up and
        /// eventually `sendAccept`s on its own connection to VatC.
        pub fn sendProvide(
            self: *Peer,
            provided_target: protocol.MessageTarget,
            recipient: message.AnyPointerReader,
            host_of_recipient: *Peer,
            contact_payload: []const u8,
        ) !ProvideHandle {
            return sendProvideInternal(
                self,
                .{ .message_target = provided_target },
                recipient,
                host_of_recipient,
                contact_payload,
                null,
            );
        }

        /// Originate a Provide whose target is an explicitly retained outbound
        /// answer on this peer. The answer must have returned and remain
        /// caller-owned. A successful send transfers that answer into the
        /// vine/Provide coupling; manual Finish then rejects it, and coupling
        /// completion Finishes both the Provide and source questions.
        pub fn sendProvideFromRetainedAnswer(
            self: *Peer,
            retained_question_id: u32,
            ops: []const protocol.PromisedAnswerOp,
            recipient: message.AnyPointerReader,
            host_of_recipient: *Peer,
            contact_payload: []const u8,
        ) !ProvideHandle {
            self.assertThreadAffinity();
            host_of_recipient.assertThreadAffinity();
            const wire_answer_id = try self.claimRetainedQuestionForTransfer(retained_question_id);
            var transfer_claimed = true;
            errdefer if (transfer_claimed) self.rollbackRetainedQuestionTransfer(retained_question_id);

            const handle = try sendProvideInternal(
                self,
                .{ .retained_answer = .{
                    .question_id = wire_answer_id,
                    .ops = ops,
                } },
                recipient,
                host_of_recipient,
                contact_payload,
                retained_question_id,
            );
            // `sendBuilder` may synchronously drive recipient teardown. A terminal
            // close drains the coupling before this frame resumes; in that case the
            // transfer never commits and caller ownership is restored by errdefer.
            if (!host_of_recipient.outbound_provides.contains(handle.vine_id)) {
                return error.HandoffClosedDuringTransfer;
            }
            self.commitRetainedQuestionTransfer(retained_question_id) catch |err| {
                // The Provide and vine are already published, so a failed commit
                // must explicitly unwind them rather than returning an ownerless
                // handle. Finish publication is durable even if its wire send fails.
                if (host_of_recipient.outbound_provides.fetchRemove(handle.vine_id)) |removed| {
                    var op = removed.value;
                    op.deinit(host_of_recipient.allocator);
                }
                host_of_recipient.caps.clearThirdPartyHosted(handle.vine_id);
                host_of_recipient.releaseVineExport(handle.vine_id);
                self.deregisterCoupledVine(host_of_recipient, handle.vine_id);
                host_of_recipient.finishOriginatedProvide(
                    self,
                    handle.question_id,
                    retained_question_id,
                );
                return err;
            };
            transfer_claimed = false;
            return handle;
        }

        pub fn sendProvideInternal(
            self: *Peer,
            provided_target: ProvideOriginationTarget,
            recipient: message.AnyPointerReader,
            host_of_recipient: *Peer,
            contact_payload: []const u8,
            retained_source_question_id: ?u32,
        ) !ProvideHandle {
            self.assertThreadAffinity();
            host_of_recipient.assertThreadAffinity();
            if (self.is_shutting_down or host_of_recipient.is_shutting_down) return error.PeerShuttingDown;
            if (self.transport_close_notified or host_of_recipient.transport_close_notified) {
                return error.TransportClosed;
            }

            var forward_target = try provided_target.cloneForwardTarget(host_of_recipient.allocator);
            var forward_target_owned = true;
            defer if (forward_target_owned) forward_target.deinit(host_of_recipient.allocator);

            // (1) Held-open Provide question on the host-of-provided-cap connection.
            //     ctx is unused by onProvideNoReturn; pass a valid pointer (self)
            //     rather than undefined so no dispatch path ever reads garbage.
            const question_id = try self.allocateQuestion(self, onProvideNoReturn);
            errdefer self.removeQuestion(question_id);
            // A Provide is deliberately held open until the recipient drops its
            // vine; it never receives a Return. The ordinary outbound-call timeout
            // therefore must not cancel it while a valid handoff is in flight.
            if (self.questions.getPtr(question_id)) |question| {
                question.deadline_ns = null;
            }

            // (2) Vine export on the host-of-recipient connection. ctx is unused by
            //     vineRejectingCall; pass host_of_recipient rather than undefined.
            const vine_id = try host_of_recipient.addExport(.{
                .ctx = host_of_recipient,
                .on_call = vineRejectingCall,
            });
            errdefer host_of_recipient.releaseVineExport(vine_id);
            try host_of_recipient.caps.markThirdPartyHosted(vine_id, contact_payload, vine_id);
            errdefer host_of_recipient.caps.clearThirdPartyHosted(vine_id);

            // (3) Couple the vine to the held-open Provide question so a later
            //     vine Release (post-Accept) Finishes the provision on VatC.
            try ensureCountLimit(
                host_of_recipient.outbound_provides.contains(vine_id),
                host_of_recipient.outbound_provides.count(),
                host_of_recipient.limits.max_active_provides,
            );
            try host_of_recipient.outbound_provides.put(vine_id, .{
                .provide_peer = self,
                .provide_question_id = question_id,
                .retained_source_question_id = retained_source_question_id,
                .forward_target = forward_target,
            });
            forward_target_owned = false;
            errdefer if (host_of_recipient.outbound_provides.fetchRemove(vine_id)) |removed| {
                var op = removed.value;
                op.deinit(host_of_recipient.allocator);
            };

            // (3b) LIVENESS (BUG #55): register the reverse back-link on `self` (the
            //      host-of-provided-cap peer). If `self` deinits before the vine is
            //      released, this lets it null the borrowed pointer above so the
            //      later Release never dereferences freed memory. Must land or the
            //      whole coupling unwinds (the errdefers above roll it back).
            try self.registerCoupledVine(host_of_recipient, vine_id);
            errdefer self.deregisterCoupledVine(host_of_recipient, vine_id);

            // (4) Send the Provide to the host of the provided cap (VatC).
            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try provided_target.buildProvide(&builder, question_id, recipient);
            try self.sendBuilder(&builder);

            log.debug("sent provide question_id={} vine_id={}", .{ question_id, vine_id });
            return .{ .question_id = question_id, .vine_id = vine_id };
        }

        /// Pick up a capability a third party provided to us: send `Accept` to the
        /// HOST of the provided cap (this `self` peer = the VatA↔VatC connection).
        /// `provision` is the `ThirdPartyCompletion` obtained from the VatNetwork,
        /// matching the `ThirdPartyToAwait` VatB placed in its `Provide`. The
        /// `on_return` callback receives the `Return` carrying the accepted cap
        /// (standard import-from-return; see the host side `sendReturnProvidedTarget`).
        ///
        /// This slice sends `embargo = null` (no in-flight-promise ordering — that
        /// is Phase 4). Returns the Accept question id.
        pub fn sendAccept(
            self: *Peer,
            provision: message.AnyPointerReader,
            embargo: ?[]const u8,
            ctx: *anyopaque,
            on_return: QuestionCallback,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;

            const question_id = try self.allocateQuestion(ctx, on_return);
            errdefer self.removeQuestion(question_id);

            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.buildAccept(question_id, provision, embargo);
            try self.sendBuilder(&builder);

            log.debug("sent accept question_id={}", .{question_id});
            return question_id;
        }

        pub fn sendAcceptNoRestore(
            self: *Peer,
            provision: message.AnyPointerReader,
            embargo: ?[]const u8,
            ctx: *anyopaque,
            on_return: QuestionCallback,
            suppress_auto_finish: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;

            const question_id = try self.allocateQuestionNoRestore(ctx, on_return);
            errdefer self.removeQuestion(question_id);
            if (suppress_auto_finish) {
                const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
                question.suppress_auto_finish = true;
            }

            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.buildAccept(question_id, provision, embargo);
            try self.sendBuilder(&builder);

            log.debug("sent accept question_id={}", .{question_id});
            return question_id;
        }

        /// Experimental Level-4 Join origination. This is a low-level/manual
        /// helper: callers provide the raw `Join.keyPart` AnyPointer and receive the
        /// ordinary `Return` callback for the Join question. It intentionally does
        /// not implement the full E-join workflow, public `JoinResult` processing,
        /// or direct-connection formation.
        pub fn sendJoinExperimental(
            self: *Peer,
            target: protocol.MessageTarget,
            key_part: ?message.AnyPointerReader,
            ctx: *anyopaque,
            on_return: QuestionCallback,
        ) !u32 {
            return sendJoinExperimentalWithAutoFinish(self, target, key_part, ctx, on_return, false);
        }

        pub fn sendJoinExperimentalWithAutoFinish(
            self: *Peer,
            target: protocol.MessageTarget,
            key_part: ?message.AnyPointerReader,
            ctx: *anyopaque,
            on_return: QuestionCallback,
            suppress_auto_finish: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;

            const question_id = try self.allocateQuestion(ctx, on_return);
            errdefer self.removeQuestion(question_id);
            if (suppress_auto_finish) {
                const question = self.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
                question.suppress_auto_finish = true;
            }

            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.buildJoin(question_id, target, key_part);
            try self.sendBuilder(&builder);

            log.debug("sent experimental join question_id={}", .{question_id});
            return question_id;
        }

        /// Allocate the next callee-chosen answer id for an outbound
        /// `ThirdPartyAnswer`. Per rpc.capnp:936-941 the value must have bit 30 set
        /// and bit 31 clear ([2^30, 2^31)); this is enforced by masking the counter
        /// back into range on every wrap, and asserted via
        /// `third_party.isThirdPartyAnswerId`. The chosen id is NOT recorded in
        /// any local table on the callee: it names the answer on the *recipient's*
        /// (VatA's) connection, and the callee only re-uses it as the `answerId` of
        /// the follow-up `Return` (which is a plain outbound frame on the same
        /// wire). Callee and recipient are different vats, so there is no collision
        /// with the callee's own caller-allocated question ids.
        pub fn allocateThirdPartyAnswerId(self: *Peer) u32 {
            var id = self.next_third_party_answer_id;
            // Keep bit 30 set / bit 31 clear even across a full-range wrap.
            if (id < third_party_answer_id_base or id >= third_party_answer_id_limit) {
                id = third_party_answer_id_base;
            }
            var next = id + 1;
            if (next >= third_party_answer_id_limit) next = third_party_answer_id_base;
            self.next_third_party_answer_id = next;
            std.debug.assert(third_party.isThirdPartyAnswerId(id));
            return id;
        }

        pub fn allocateUnusedThirdPartyAnswerId(self: *Peer) !u32 {
            const first = self.next_third_party_answer_id;
            while (true) {
                const id = allocateThirdPartyAnswerId(self);
                if (!(try self.inboundQuestionIdInUse(id)) and
                    !self.incoming_automatic_third_party_routes.contains(id))
                {
                    return id;
                }
                if (self.next_third_party_answer_id == first) return error.AnswerIdExhausted;
            }
        }

        pub fn sendThirdPartyAnswerWithId(
            self: *Peer,
            answer_id: u32,
            completion: message.AnyPointerReader,
        ) !void {
            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.buildThirdPartyAnswer(answer_id, completion);
            try self.sendBuilder(&builder);
            log.debug("sent thirdPartyAnswer answer_id={}", .{answer_id});
        }

        /// Callee → third party: send a `ThirdPartyAnswer` on the connection to the
        /// vat that will receive this call's results (VatA). This is the
        /// redirected-return origination for a call that arrived with
        /// `sendResultsTo = thirdParty` (rpc.capnp:494-505, 906-942): the callee
        /// (VatC) connects to the third party and adopts the call into that
        /// connection by announcing a callee-allocated `answerId`. The recipient
        /// matches `completion` (the `ThirdPartyCompletion`) against the
        /// `awaitFromThirdParty` bookkeeping it was primed with, adopts its parked
        /// question under `answer_id`, and thereafter accepts the follow-up `Return`
        /// (which the callee sends under the SAME `answer_id`) as that question's
        /// results.
        ///
        /// `completion` must serialize to bytes byte-identical to the recipient's
        /// awaited `ThirdPartyToAwait`/`ThirdPartyCompletion` — the recipient keys
        /// its await table on those bytes (see `handleThirdPartyAnswer`).
        ///
        /// Returns the callee-allocated answer id, which the caller then re-uses as
        /// the `answerId` of the follow-up results `Return` on this same peer.
        pub fn sendThirdPartyAnswer(
            self: *Peer,
            completion: message.AnyPointerReader,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;

            const answer_id = try allocateUnusedThirdPartyAnswerId(self);
            try sendThirdPartyAnswerWithId(self, answer_id, completion);
            return answer_id;
        }

        /// Third-party (recipient) side: park an outbound question that awaits a
        /// `ThirdPartyAnswer` from the callee, keyed by the completion bytes it will
        /// present. This is the recipient-side half of the `awaitFromThirdParty`
        /// bookkeeping the receive path already reads: when the callee later sends a
        /// `ThirdPartyAnswer` whose completion matches `completion`, `handleThirdPartyAnswer`
        /// adopts this parked question under the callee-allocated answer id, and the
        /// follow-up `Return` completes it via `on_return`.
        ///
        /// In the spec's canonical proxy topology this parking happens implicitly
        /// when the recipient receives a `Return{awaitFromThirdParty}` for a call it
        /// itself made (`handleReturnAcceptFromThirdParty`). In a first-class
        /// origination the recipient (VatA) never made that call — the introducer
        /// (VatB) did — so the recipient is primed explicitly here. `completion`
        /// must serialize byte-identically to the completion the callee will send.
        ///
        /// The parked question is delivered exactly one `Return` (carrying the real
        /// results) and needs no auto-Finish of its own; adoption inserts it into
        /// the normal questions table under the adopted id, after which it follows
        /// the standard Return lifecycle.
        pub fn registerPendingThirdPartyAwait(
            self: *Peer,
            completion: message.AnyPointerReader,
            ctx: *anyopaque,
            on_return: QuestionCallback,
        ) !void {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;

            const completion_key = (try captureAnyPointerPayload(self.allocator, completion)) orelse
                return error.MissingThirdPartyPayload;
            var owns_key = true;
            errdefer if (owns_key) self.allocator.free(completion_key);

            if (self.pending_third_party_awaits.contains(completion_key)) {
                return error.DuplicateThirdPartyAwait;
            }

            // Reserve an answer-id slot's worth of question budget so the later
            // adoption into `questions` cannot overflow silently. The parked
            // question is a normal outbound question that has not yet been assigned
            // a wire id (the callee chooses it), so it is NOT inserted into
            // `questions` here — only into the await table.
            try self.ensurePendingThirdPartyAwaitBudget(completion_key);

            const question = Question{
                .ctx = ctx,
                .on_return = on_return,
                // The adopted question follows the standard Return lifecycle: when
                // the callee's follow-up results `Return` (under the callee-chosen
                // answer id) arrives, the recipient sends a `Finish` for that id
                // back to the callee, per rpc.capnp:924-926 ("the receiver ... must
                // eventually send a Finish message"). That Finish clears the
                // callee's answer-table entry for the redirected results, so
                // auto-finish must NOT be suppressed here.
            };

            try third_party.putPendingAwait(
                PendingThirdPartyAwait,
                &self.pending_third_party_awaits,
                completion_key,
                .{ .question_id = 0, .question = question },
            );
            owns_key = false;
        }
    };
}
