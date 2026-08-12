const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");
const events = @import("../../events.zig");
const pending_calls = @import("../../promises/pending_calls.zig");
const peer_outbound_control = @import("../peer_outbound_control.zig");
const finish = @import("../finish.zig");
const state = @import("../state.zig");
const third_party = @import("../third_party.zig");
const peer_call_orchestration = @import("./peer_call_orchestration.zig");
const peer_cap_lifecycle = @import("../peer_cap_lifecycle.zig");
const peer_call_targets = @import("./peer_call_targets.zig");
const message = @import("../../../serialization/message.zig");
const peer_forward_orchestration = @import("../forward/peer_forward_orchestration.zig");
const peer_return_dispatch = @import("../return/peer_return_dispatch.zig");

/// The inbound Call path and resolved-answer bookkeeping, extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract): handleCall dispatch, resolved-target dispatch,
/// resolved-answer reservation/commit/record, promised/promise-export call
/// queueing with pressure events, queued-call replay, and third-party answer
/// adoption. `peer/mod.zig` keeps every caller-visible name as a thunk on
/// `Peer`, so signatures, hook fn-types, and the api-snapshot rendering are
/// unchanged.
pub fn CallInbound(comptime Peer: type) type {
    return struct {
        const PendingCall = state.PendingCall;
        const ResolvedAnswerReservation = Peer.ResolvedAnswerReservation;
        const PendingQueuedCallStats = Peer.PendingQueuedCallStats;
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const ResolvedAnswer = state.ResolvedAnswer;
        const ensureCountLimit = Peer.ensureCountLimit;
        /// True when an inbound Call's params cap table contains at least one
        /// descriptor that grants this vat a wire reference — the descriptors
        /// `InboundCapTable.init` turns into `noteImport` calls, and therefore the
        /// ones this vat later settles with an explicit `Release` frame.
        ///
        /// Read straight off the caller's frame (tags only; no import is taken here)
        /// so the answering Return's `releaseParamCaps` can be decided before dispatch
        /// runs — see `Peer.returnReleasesParamCaps`.
        ///
        /// A descriptor this peer cannot even decode counts as ref-granting, but purely
        /// as a defensive default: no Return ever reads that value. `InboundCapTable`
        /// construction is about to fail on the same entry, and that failure unwinds
        /// `handleCall`, whose `errdefer` drops this answer's `active_inbound_questions`
        /// record — while the dispatch-error path sends no Return at all for a Call it
        /// could not dispatch. (Nor is a reference stranded: the table's own `errdefer`
        /// releases every import it noted before the failing entry.) The conservative
        /// direction is still the right one to be wrong in, because it can only ever
        /// withhold an implicit release, never invent a second one.
        fn callParamsGrantImportRefs(cap_table_list: ?message.StructListReader) bool {
            const list = cap_table_list orelse return false;
            var idx: u32 = 0;
            while (idx < list.len()) : (idx += 1) {
                const reader = list.get(idx) catch return true;
                const descriptor = protocol.CapDescriptor.fromReader(reader) catch return true;
                switch (descriptor.tag) {
                    // `resolveDescriptor` notes an import for each of these: the two
                    // sender-side forms, and the vine of a third-party hand-off.
                    .senderHosted, .senderPromise, .thirdPartyHosted => return true,
                    // `receiverHosted` names one of our own exports and `receiverAnswer`
                    // one of our own answers; neither grants us a reference to release.
                    .none, .receiverHosted, .receiverAnswer => {},
                }
            }
            return false;
        }
        const third_party_results_unsupported =
            "sendResultsTo.thirdParty unsupported: this vat cannot route results to a third party";

        pub fn handleCall(self: *Peer, frame: []const u8, call: protocol.Call) !void {
            // Reject duplicate question IDs from the remote peer (spec violation).
            // Covers the shared answer namespace plus in-flight Provide/Join
            // questions so a Call can never collide with any other inbound question.
            if (try self.inboundQuestionIdInUse(call.question_id)) {
                return error.DuplicateQuestionId;
            }

            // `sendResultsTo = thirdParty` asks this vat to connect to a third vat
            // and deliver the results there. Unless the host opted in, we cannot —
            // and accepting the call only to drop its results is the one outcome the
            // protocol never permits. Refuse with a single exception Return.
            //
            // Placement is load-bearing: after the duplicate-id check, so a reused
            // id still reports DuplicateQuestionId; before the answer bookkeeping,
            // so there is nothing to unwind; and before the inbound cap table is
            // built, so no import references are taken and the exception Return's
            // `releaseParamCaps` — still the schema default TRUE, because no
            // `active_inbound_questions` entry exists yet to say otherwise —
            // correctly tells the sender to drop its export refs. This also covers
            // in-process loopback calls, which are delivered through handleFrame.
            if (call.send_results_to.tag == .thirdParty and
                self.third_party_result_policy == .reject)
            {
                try self.sendReturnException(call.question_id, third_party_results_unsupported);
                return;
            }

            const inbound_before = self.active_inbound_questions.count();
            try ensureCountLimit(false, inbound_before, self.limits.max_active_inbound_questions);
            // Decide the answering Return's `releaseParamCaps` up front, from the
            // frame the caller actually sent: a `senderHosted`/`senderPromise` param
            // descriptor is a wire reference the caller recorded and this vat is
            // about to take (`InboundCapTable.init`), and every such reference is
            // settled here with an explicit `Release` frame — never implicitly.
            // rpc.capnp: "If true, all capabilities that were in the params should
            // be considered released. The sender must not send separate `Release`
            // messages for them."
            try self.active_inbound_questions.put(
                call.question_id,
                callParamsGrantImportRefs(call.params.cap_table),
            );
            errdefer _ = self.active_inbound_questions.remove(call.question_id);
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .active_inbound_questions,
                inbound_before,
                self.active_inbound_questions.count(),
                self.limits.max_active_inbound_questions,
            );

            peer_call_orchestration.handleCallForPeer(
                Peer,
                self,
                frame,
                call,
                peer_call_orchestration.handleCallImportedTargetForPeerFn(
                    Peer,
                    cap_table.InboundCapTable,
                    queuePromiseExportCall,
                    Peer.releaseInboundCaps,
                    peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
                    third_party.noteCallSendResultsForPeerFn(
                        Peer,
                        Peer.noteSendResultsToYourself,
                        Peer.noteSendResultsToThirdParty,
                    ),
                    Peer.sendReturnException,
                    handleResolvedCall,
                ),
                peer_call_orchestration.handleCallPromisedTargetForPeerFn(
                    Peer,
                    cap_table.InboundCapTable,
                    Peer.resolvePromisedAnswer,
                    peer_call_targets.hasUnresolvedPromiseExportForPeerFn(Peer),
                    Peer.lookupFailedAnswer,
                    queuePromisedCall,
                    queuePromiseExportCall,
                    Peer.sendReturnException,
                    Peer.sendReturnExceptionTyped,
                    handleResolvedCall,
                    Peer.releaseInboundCaps,
                    peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
                ),
            ) catch |err| {
                log.debug("call routing error for question {}: {}", .{ call.question_id, err });
                // The redirected result is already committed on the target
                // connection and the resultsSentElsewhere write may itself have
                // reached the source caller. Never risk a second terminal Return
                // on that answer id; let the transport-level error close the peer.
                if (err == error.AutomaticThirdPartySourceSettlementFailed) return err;
                try self.sendReturnException(call.question_id, @errorName(err));
            };
            if (self.automatic_third_party_routes.get(call.question_id)) |route| {
                if (route.source_marker_failed) {
                    self.finalizeAutomaticThirdPartyRoute(route, null);
                    return error.AutomaticThirdPartySourceSettlementFailed;
                }
            }
        }

        pub fn handleResolvedCall(
            self: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
            resolved: cap_table.ResolvedCap,
        ) !void {
            try peer_forward_orchestration.handleResolvedCall(
                Peer,
                cap_table.InboundCapTable,
                self,
                call,
                inbound_caps,
                resolved,
                peer_call_orchestration.handleResolvedExportedCallForPeerFn(
                    Peer,
                    cap_table.InboundCapTable,
                    third_party.noteCallSendResultsForPeerFn(
                        Peer,
                        Peer.noteSendResultsToYourself,
                        Peer.noteSendResultsToThirdParty,
                    ),
                    handleResolvedCall,
                    Peer.sendReturnException,
                    Peer.maybeForwardVineCall,
                ),
                peer_forward_orchestration.forwardResolvedCallForPeerFn(
                    Peer,
                    cap_table.InboundCapTable,
                    Peer.forwardResolvedCall,
                ),
                Peer.sendReturnException,
            );
        }

        /// Reserve everything needed to record a resolved answer BEFORE the Return
        /// frame is sent: the resolved-answers count budget, one unused map slot,
        /// the frame copy, and the answer-held references on every export in the
        /// frame's results cap table. This must precede the send so that recording
        /// — which happens only once the frame is already on the wire — is
        /// infallible.
        ///
        /// If the record step could fail after the send, the propagating error would
        /// drive the call-dispatch catch to emit a SECOND (exception) Return for the
        /// same answer_id: two Returns for one call, a remote-forceable protocol
        /// violation (a peer fills resolved_answers to max_resolved_answers with
        /// Finish-less calls, then any later successful call double-Returns). Audit
        /// 2026-07-03 item 7.
        ///
        /// The answer-held references keep the answer's pipeline targets alive
        /// until its Finish: without them the export dies as soon as the remote
        /// Releases the caps it imported from this Return — legal even before
        /// Finish — and a pipelined call on the answer can no longer dispatch.
        pub fn reserveResolvedAnswer(self: *Peer, question_id: u32, frame: []const u8) !ResolvedAnswerReservation {
            try ensureCountLimit(
                self.resolved_answers.contains(question_id),
                self.resolved_answers.count() + self.resolved_answer_reservations,
                self.limits.max_resolved_answers,
            );
            // Reserve a map slot so the post-send getOrPutAssumeCapacity cannot
            // OOM. Ensure one slot per OUTSTANDING reservation as well: a nested
            // reserve→commit for a different answer id (a synchronous transport
            // delivering a Call/Bootstrap while an outer send is on the stack)
            // would otherwise consume the single slot the outer reservation's
            // ensure counted on, and the outer infallible commit would underflow
            // the map's reserved-slot accounting.
            try self.resolved_answers.ensureUnusedCapacity(1 + self.resolved_answer_reservations);
            const frame_copy = try self.allocator.dupe(u8, frame);
            errdefer self.allocator.free(frame_copy);
            const held_export_ids = try peer_cap_lifecycle.noteAnswerHeldResultCaps(
                Peer,
                self,
                self.allocator,
                frame,
                Peer.noteAnswerExportRef,
                Peer.rollbackAnswerExportRef,
            );
            self.resolved_answer_reservations += 1;
            return .{ .frame_copy = frame_copy, .held_export_ids = held_export_ids };
        }

        /// Record a resolved answer using a reservation obtained (before the send)
        /// from `reserveResolvedAnswer`. Infallible: the map slot, frame copy, and
        /// answer-held export references are already reserved, so this only stores
        /// into the map. Takes ownership of the reservation: `frame_copy` moves
        /// into the map and the answer-held references now belong to the recorded
        /// answer (released at Finish by re-walking the stored frame, so the id
        /// list is no longer needed).
        pub fn commitReservedResolvedAnswer(
            self: *Peer,
            question_id: u32,
            reservation: ResolvedAnswerReservation,
        ) void {
            self.resolved_answer_reservations -= 1;
            self.allocator.free(reservation.held_export_ids);
            pending_calls.recordResolvedAnswerAssumeCapacity(
                Peer,
                ResolvedAnswer,
                PendingCall,
                cap_table.InboundCapTable,
                self.allocator,
                self,
                question_id,
                reservation.frame_copy,
                &self.resolved_answers,
                &self.pending_promises,
                Peer.resolvePromisedAnswer,
                Peer.sendReturnException,
                handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            );
        }

        fn recordResolvedAnswer(self: *Peer, question_id: u32, frame: []u8) !void {
            try ensureCountLimit(
                self.resolved_answers.contains(question_id),
                self.resolved_answers.count(),
                self.limits.max_resolved_answers,
            );
            try pending_calls.recordResolvedAnswer(
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
                handleResolvedCall,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            );
        }

        pub fn queuePromisedCall(self: *Peer, question_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
            try self.ensurePendingQueuedCallBudget(
                &self.pending_promises,
                question_id,
                frame.len,
                self.limits.max_pending_promises,
            );
            const stats_before = self.pendingQueuedCallStats();
            try pending_calls.queuePendingCall(
                PendingCall,
                cap_table.InboundCapTable,
                self.allocator,
                &self.pending_promises,
                question_id,
                frame,
                inbound_caps,
            );
            emitQueuedCallPressure(self, stats_before, frame.len);
        }

        fn queuePromiseExportCall(self: *Peer, export_id: u32, frame: []const u8, inbound_caps: cap_table.InboundCapTable) !void {
            try self.ensurePendingQueuedCallBudget(
                &self.pending_export_promises,
                export_id,
                frame.len,
                self.limits.max_pending_export_promises,
            );
            const stats_before = self.pendingQueuedCallStats();
            try pending_calls.queuePendingCall(
                PendingCall,
                cap_table.InboundCapTable,
                self.allocator,
                &self.pending_export_promises,
                export_id,
                frame,
                inbound_caps,
            );
            emitQueuedCallPressure(self, stats_before, frame.len);
        }

        /// Emit queued-call pressure crossings after a successful enqueue.
        fn emitQueuedCallPressure(self: *Peer, before: PendingQueuedCallStats, frame_len: usize) void {
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .queued_calls,
                before.calls,
                before.calls + 1,
                self.limits.max_pending_queued_calls,
            );
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .queued_call_bytes,
                before.bytes,
                before.bytes + frame_len,
                self.limits.max_pending_queued_call_bytes,
            );
        }

        pub fn replayResolvedPromiseExport(self: *Peer, export_id: u32, resolved: cap_table.ResolvedCap) !void {
            try pending_calls.replayResolvedPromiseExport(
                Peer,
                PendingCall,
                cap_table.InboundCapTable,
                self.allocator,
                self,
                export_id,
                resolved,
                &self.pending_export_promises,
                handleResolvedCall,
                Peer.sendReturnException,
                Peer.releaseInboundCaps,
                peer_return_dispatch.reportNonfatalErrorForPeerFn(Peer),
            );
        }

        pub fn adoptThirdPartyAnswer(
            self: *Peer,
            question_id: u32,
            adopted_answer_id: u32,
            question: Question,
        ) anyerror!void {
            // A retained call is addressed publicly by its original caller-chosen
            // question id, but after awaitFromThirdParty the open remote answer is
            // the callee-chosen adopted id. Publish that wire identity before the
            // adoption helper can replay an already-buffered terminal Return: its
            // callback may synchronously Finish or transfer the retained answer.
            var previous_retained_answer_id: ?u32 = null;
            if (self.retained_questions.contains(question_id)) {
                previous_retained_answer_id = try self.retained_questions.adoptWireAnswer(
                    question_id,
                    adopted_answer_id,
                );
            }
            errdefer if (previous_retained_answer_id) |previous| {
                self.retained_questions.rollbackWireAnswer(
                    question_id,
                    adopted_answer_id,
                    previous,
                );
            };
            try third_party.adoption.adoptThirdPartyAnswer(
                Peer,
                Question,
                self,
                question_id,
                adopted_answer_id,
                question,
                &self.questions,
                &self.adopted_third_party_answers,
                &self.pending_third_party_returns,
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                Peer.ensureThirdPartyAdoptionBudget,
                finish.freeOwnedFrameForPeerFn(Peer),
                third_party.returns.handlePendingReturnFrameForPeerFn(
                    Peer,
                    Peer.handleReturn,
                ),
            );
        }
    };
}
