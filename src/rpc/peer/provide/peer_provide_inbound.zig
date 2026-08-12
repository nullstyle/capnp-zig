const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const vat_provisions = @import("../../vat/provisions.zig");
const events = @import("../../events.zig");
const provide_accept_join = @import("../provide_accept_join.zig");
const third_party = @import("../third_party.zig");
const finish = @import("../finish.zig");
const peer_outbound_control = @import("../peer_outbound_control.zig");

/// Inbound Provide/Accept/Join/ThirdPartyAnswer dispatch arms, extracted
/// from `peer/mod.zig` and made generic over the peer type (the
/// JoinCoordinator extraction contract): embargoed-Accept queueing, the
/// Provide registration arm (vat-index and legacy), Accept index-routing
/// entry, hosted L4 JoinResult acceptance, the L4 runtime join completion,
/// the Join dispatch arm, and ThirdPartyAnswer adoption. `peer/mod.zig`
/// keeps every caller-visible name as a thunk on `Peer`.
pub fn ProvideInbound(comptime Peer: type) type {
    return struct {
        const ProvideTarget = state.ProvideTarget;
        const ProvisionIndex = vat_provisions.ProvisionIndex(Peer);
        const ProvideEntry = state.ProvideEntry;
        const JoinState = state.JoinState;
        const HostedJoin = Peer.HostedJoinRecord;
        const ensureCountLimit = Peer.ensureCountLimit;
        const ensureByteLimit = Peer.ensureByteLimit;
        const saturatingAdd = Peer.saturatingAdd;
        const joinWireReason = Peer.joinWireReason;
        const provideTargetsEqual = Peer.provideTargetsEqual;
        const captureAnyPointerPayload = Peer.captureAnyPointerPayload;
        const PendingEmbargoedAccept = state.PendingEmbargoedAccept;
        const JoinKeyPart = state.JoinKeyPart;
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const PendingThirdPartyAwait = state.PendingThirdPartyAwait(Question);
        const PendingJoinQuestion = state.PendingJoinQuestion;
        const makeProvideTarget = Peer.makeProvideTarget;
        const adoptThirdPartyAnswer = Peer.adoptThirdPartyAnswer;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        pub fn queueEmbargoedAccept(
            self: *Peer,
            answer_id: u32,
            provided_question_id: u32,
            embargo: []const u8,
        ) !void {
            try ensureCountLimit(
                self.pending_accept_embargo_by_question.contains(answer_id),
                self.pending_accept_embargo_by_question.count(),
                self.limits.max_pending_accepts,
            );
            if (!self.pending_accepts_by_embargo.contains(embargo)) {
                try ensureCountLimit(
                    false,
                    self.pending_accepts_by_embargo.count(),
                    self.limits.max_pending_accept_embargo_buckets,
                );
                try ensureByteLimit(
                    self.pendingAcceptEmbargoKeyBytes(),
                    embargo.len,
                    self.limits.max_pending_accept_embargo_bytes,
                );
            }

            try provide_accept_join.embargo_accepts.queueEmbargoedAcceptForPeer(
                Peer,
                PendingEmbargoedAccept,
                self,
                answer_id,
                provided_question_id,
                embargo,
            );
        }

        pub fn handleProvide(self: *Peer, provide: protocol.Provide) !void {
            // Expire due Accept-before-Provide reservations before a Provide can
            // adopt them. The cached due check makes the common path O(1).
            if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();

            // A Provide must not reuse a question id already live as a Call /
            // Bootstrap answer or a pending Join (spec violation). Same-type
            // (provide) collisions fall through to the orchestration's specific
            // "duplicate provide question" abort below.
            if (try self.inboundAnswerQuestionIdInUse(provide.question_id) or
                self.pending_join_questions.contains(provide.question_id))
            {
                return error.DuplicateQuestionId;
            }
            try provide_accept_join.orchestration.handleProvide(
                Peer,
                ProvideEntry,
                ProvideTarget,
                self,
                self.allocator,
                provide,
                &self.provides_by_question,
                &self.provides_by_key,
                provide_accept_join.orchestration.captureProvideRecipientForPeerFn(
                    Peer,
                    third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                ),
                finish.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                Peer.ensureProvideBudget,
                provide_accept_join.resolveProvideTargetForPeerFn(
                    Peer,
                    provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                    provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
                ),
                makeProvideTarget,
                ProvideTarget.deinit,
            );

            // Vat-wide registration (index mode only). On failure, roll the
            // just-stored per-peer provide back, send ONE abort with the matched
            // reason, and propagate the ORIGINAL error.
            if (self.provision_index) |idx| {
                var adopted: ?*ProvisionIndex.Provision = null;
                self.registerProvisionForProvide(idx, provide.question_id, &adopted) catch |err| {
                    provide_accept_join.provides_state.clearProvideForPeer(
                        Peer,
                        ProvideEntry,
                        ProvideTarget,
                        self,
                        provide.question_id,
                        ProvideTarget.deinit,
                    );
                    const reason: []const u8 = switch (err) {
                        error.DuplicateProvideRecipient => "duplicate provide recipient",
                        else => @errorName(err),
                    };
                    peer_outbound_control.sendAbortViaSendFrame(Peer, self, reason, Peer.sendFrameControl) catch |send_err| {
                        log.debug("provide registration abort send failed: {}", .{send_err});
                    };
                    return err;
                };
                // PHASE B (adoption drain) runs OUTSIDE the rollback catch: the
                // provision is active and may serve accepts — a Provide rollback
                // would be wrong here. Only a terminal OOM re-raises, after every
                // parked entry was terminally handled.
                if (adopted) |prov| try self.drainAdoptedParkedAccepts(idx, prov);
            }
        }

        pub fn handleAccept(self: *Peer, accept: protocol.Accept) !void {
            if (try tryHandleJoinAccept(self, accept)) return;
            if (self.provision_index) |idx| return self.handleAcceptWithProvisionIndex(idx, accept);
            try provide_accept_join.orchestration.handleAccept(
                Peer,
                ProvideEntry,
                ProvideTarget,
                self,
                accept,
                &self.provides_by_question,
                &self.provides_by_key,
                provide_accept_join.orchestration.captureAcceptProvisionForPeerFn(
                    Peer,
                    third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                ),
                finish.freeOwnedFrameForPeerFn(Peer),
                queueEmbargoedAccept,
                Peer.sendReturnProvidedTarget,
                Peer.sendReturnException,
            );
        }

        pub fn tryHandleJoinAccept(self: *Peer, accept: protocol.Accept) !bool {
            const key = try provide_accept_join.orchestration.captureAcceptProvisionForPeer(
                Peer,
                self,
                accept,
                third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
            );
            defer if (key) |bytes| self.allocator.free(bytes);
            const provision = key orelse return false;

            // First establish that this token names an L4 lease without consuming
            // it. The Accept answer ID must be free across the complete inbound
            // namespace before canonical pickup becomes irreversible; a duplicate
            // frame leaves the provision wholly live for a fresh ID retry.
            if (!self.pending_join_accepts.contains(provision)) return false;
            if (try self.inboundQuestionIdInUse(accept.question_id)) {
                return error.DuplicateQuestionId;
            }

            var target = self.takePendingJoinAccept(provision) orelse return error.MissingJoinProvision;
            defer target.deinit(self.allocator);

            if (accept.embargo != null) {
                try self.sendReturnException(accept.question_id, "l4 join accept embargo unsupported");
                return true;
            }

            self.sendReturnProvidedTarget(accept.question_id, &target) catch |err| {
                try self.sendReturnException(accept.question_id, joinWireReason(err));
            };
            return true;
        }

        pub fn completeJoinWithL4Runtime(self: *Peer, join_id: u32) !void {
            self.enterJoinOperation();
            defer self.leaveJoinOperation();
            if (self.join_network == null) return error.NoJoinNetwork;
            const pending = self.pending_joins.get(join_id) orelse return;
            if (pending.parts.count() == 0) return;
            const completing_records = try std.math.add(usize, self.completing_join_records, 1);
            const network = try self.beginJoinNetworkBorrow();
            defer self.endJoinNetworkBorrow();
            const removed = self.pending_joins.fetchRemove(join_id) orelse return;
            var join_state = removed.value;
            defer JoinState.deinit(&join_state, self.allocator);
            self.refreshNextJoinDeadline();

            var first_target: ?*const ProvideTarget = null;
            var all_equal = true;
            var check_it = join_state.parts.iterator();
            while (check_it.next()) |entry| {
                if (first_target) |target| {
                    if (!provideTargetsEqual(target, &entry.value_ptr.target)) {
                        all_equal = false;
                        break;
                    }
                } else {
                    first_target = &entry.value_ptr.target;
                }
            }

            // Reserve every completing answer id before the first application
            // callback or Return. These tombstones preserve the old bucket+parts
            // aggregate footprint (one transition record plus one per answer), so
            // callback reentry cannot steal capacity, Finish cannot vanish, and no
            // later answer in this fanout can be reused under us.
            self.completing_join_records = completing_records;
            var transition_live = true;
            defer if (transition_live) {
                std.debug.assert(self.completing_join_records > 0);
                self.completing_join_records -= 1;
            };
            var reserve_it = join_state.parts.valueIterator();
            while (reserve_it.next()) |part| {
                self.putCompletingJoinAnswerAssumeCapacity(part.question_id, true);
            }
            // This defer also owns any canonical result refs attached later. It is
            // intentionally installed before fanout guards so their operation pin
            // unwinds first; forced cleanup can then invoke callbacks without
            // destroying a canonical still borrowed by the fanout stack.
            defer {
                var cleanup_answers = join_state.parts.valueIterator();
                while (cleanup_answers.next()) |part| {
                    if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                        self.retireCompletingJoinAnswerAccounting(completing);
                        self.dropCompletingJoinResultRef(completing);
                    }
                    _ = self.removeCompletingJoinAnswer(part.question_id);
                    _ = self.finished_early_answers.remove(part.question_id);
                }
            }

            // Detach every part record before host callbacks. The local join_state
            // owns targets and the counted completion tombstones own IDs from here.
            var cleanup_it = join_state.parts.iterator();
            while (cleanup_it.next()) |entry| {
                _ = self.pending_join_questions.remove(entry.value_ptr.question_id);
            }

            if (!all_equal) {
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                var mismatch_it = join_state.parts.iterator();
                while (mismatch_it.next()) |entry| {
                    try self.sendReturnException(entry.value_ptr.question_id, "join target mismatch");
                }
                return;
            }

            const target = first_target orelse return;
            const hosted_result = network.hostJoinResult(self.allocator, self, join_id) catch |err| {
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                var err_it = join_state.parts.iterator();
                while (err_it.next()) |entry| {
                    try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
                }
                return;
            };
            const accept_peer = hosted_result.accept_peer;
            if (accept_peer != self) accept_peer.enterJoinOperation();
            defer if (accept_peer != self) accept_peer.leaveJoinOperation();
            defer self.allocator.free(hosted_result.result);

            // `hostJoinResult` is the only callback that can reveal the Accept
            // peer. The detached transition already reserves the complete owner
            // footprint; only the direct Accept is a positive record delta, and it
            // belongs either here or to the distinct Accept host. Reentrant Join
            // admission is refused while the captured network borrow is live, so
            // this capacity cannot be stolen between preflight and publication.
            const capacity_peer = accept_peer;
            capacity_peer.ensureJoinRecordCapacity(1) catch {
                const attempted_records = saturatingAdd(capacity_peer.joinRecordCount(), 1);
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                network.cancelHostJoinResult(hosted_result.provision);
                self.allocator.free(hosted_result.provision);
                events.emitResourceRejection(
                    capacity_peer.observer,
                    .peer,
                    .unknown,
                    .join_records,
                    attempted_records,
                    capacity_peer.limits.max_pending_join_records,
                    error.PeerLimitExceeded,
                );
                var err_it = join_state.parts.iterator();
                while (err_it.next()) |entry| {
                    try self.sendReturnException(entry.value_ptr.question_id, "join unavailable");
                }
                return;
            };

            var target_copy = accept_peer.cloneProvideTarget(target) catch |err| {
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                network.cancelHostJoinResult(hosted_result.provision);
                self.allocator.free(hosted_result.provision);
                var err_it = join_state.parts.iterator();
                while (err_it.next()) |entry| {
                    try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
                }
                return;
            };
            var target_owned = true;
            defer if (target_owned) target_copy.deinit(accept_peer.allocator);

            const canonical = self.allocator.create(HostedJoin) catch |err| {
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                network.cancelHostJoinResult(hosted_result.provision);
                self.allocator.free(hosted_result.provision);
                var err_it = join_state.parts.iterator();
                while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, joinWireReason(err));
                return;
            };
            canonical.* = .{
                .owner_peer = self,
                .accept_peer = null,
                .network = network,
                .provision = hosted_result.provision,
            };
            var canonical_unpublished = true;
            defer if (canonical_unpublished) {
                network.cancelHostJoinResult(canonical.provision);
                self.allocator.free(canonical.provision);
                self.allocator.destroy(canonical);
            };

            self.ownHostedJoinFromCompletion(canonical) catch |err| {
                const attempted_bytes = saturatingAdd(self.join_accept_bytes, canonical.provision.len);
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                canonical_unpublished = false;
                network.cancelHostJoinResult(canonical.provision);
                self.allocator.free(canonical.provision);
                self.allocator.destroy(canonical);
                if (err == error.PeerLimitExceeded) {
                    events.emitResourceRejection(
                        self.observer,
                        .peer,
                        .unknown,
                        .join_accept_bytes,
                        attempted_bytes,
                        self.limits.max_pending_join_accept_bytes,
                        err,
                    );
                }
                var err_it = join_state.parts.iterator();
                const reason = joinWireReason(err);
                while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, reason);
                return;
            };
            canonical_unpublished = false;
            transition_live = false;

            accept_peer.putPendingJoinAcceptOwned(canonical, target_copy) catch |err| {
                const attempted_records = saturatingAdd(accept_peer.joinRecordCount(), 1);
                const emit_record_rejection = err == error.PeerLimitExceeded or
                    err == error.JoinRecordLimitExceeded;
                self.retireCompletingJoinTransitionAccounting(&join_state, &transition_live);
                self.cancelHostedJoin(canonical);
                if (emit_record_rejection) {
                    events.emitResourceRejection(
                        accept_peer.observer,
                        .peer,
                        .unknown,
                        .join_records,
                        attempted_records,
                        accept_peer.limits.max_pending_join_records,
                        error.PeerLimitExceeded,
                    );
                }
                var err_it = join_state.parts.iterator();
                const reason = joinWireReason(err);
                while (err_it.next()) |entry| try self.sendReturnException(entry.value_ptr.question_id, reason);
                return;
            };
            target_owned = false;

            // Attach every unfinished tombstone to the canonical before the first
            // Return. The result map's capacity was reserved at part admission,
            // but publication waits until all callback-bearing sends finish so the
            // IDs remain coherently transitional as a group.
            var result_refs: usize = 0;
            var reserve_results_it = join_state.parts.valueIterator();
            while (reserve_results_it.next()) |part| {
                const completing = self.completing_join_answers.getPtr(part.question_id) orelse continue;
                if (completing.finished) continue;
                completing.hosted = canonical;
                result_refs += 1;
            }
            canonical.result_refs = result_refs;

            canonical.operation_depth += 1;
            defer {
                canonical.operation_depth -= 1;
                self.maybeDestroyHostedJoin(canonical);
            }
            if (canonical.result_refs == 0) {
                self.cancelHostedJoin(canonical);
                return;
            }

            var send_it = join_state.parts.iterator();
            while (send_it.next()) |entry| {
                const answer_id = entry.value_ptr.question_id;
                const before_send = self.completing_join_answers.get(answer_id) orelse continue;
                if (before_send.finished) continue;
                if (canonical.cancelled or before_send.hosted != canonical) {
                    if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                        self.retireCompletingJoinAnswerAccounting(completing);
                    }
                    try self.sendReturnException(answer_id, "join unavailable");
                    continue;
                }

                self.sendReturnJoinResultPayload(answer_id, hosted_result.result) catch |err| {
                    if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                        if (!completing.finished) {
                            self.retireCompletingJoinAnswerAccounting(completing);
                            self.dropCompletingJoinResultRef(completing);
                            try self.sendReturnException(answer_id, joinWireReason(err));
                        }
                    }
                    continue;
                };
                canonical.published_results += 1;
                if (canonical.timeout_answer_id == null) {
                    canonical.timeout_answer_id = answer_id;
                }
                if (self.completing_join_answers.getPtr(answer_id)) |completing| {
                    if (!completing.finished and completing.hosted == canonical) {
                        completing.result_sent = true;
                    }
                }
            }

            // Atomically exchange each surviving tombstone for its steady-state
            // result record. No callback occurs between remove and assume-capacity
            // insert, so the answer namespace never observes a reusable ID.
            var publish_it = join_state.parts.valueIterator();
            while (publish_it.next()) |part| {
                const answer_id = part.question_id;
                const completing = self.completing_join_answers.getPtr(answer_id) orelse continue;
                if (completing.result_sent and !completing.finished and completing.hosted == canonical) {
                    _ = self.removeCompletingJoinAnswer(answer_id);
                    self.pending_join_result_answers.putAssumeCapacity(answer_id, .{
                        .hosted = canonical,
                        .published = true,
                    });
                    continue;
                }
                self.retireCompletingJoinAnswerAccounting(completing);
                self.dropCompletingJoinResultRef(completing);
                _ = self.removeCompletingJoinAnswer(answer_id);
                _ = self.finished_early_answers.remove(answer_id);
            }

            if (canonical.result_refs == 0 and canonical.accept_live) {
                self.cancelHostedJoin(canonical);
            }
        }

        pub fn handleJoin(self: *Peer, join: protocol.Join) !void {
            // A captured JoinNetwork callback may synchronously inject another
            // Join on this peer. The callback cannot reveal the final Accept host
            // until it returns, so admitting nested work would let it consume the
            // positive-delta slot preflighted for the outer canonical lease.
            // Refuse generically; never reinterpret callback reentrancy as a
            // TTL/quota opt-out or expose which completion is in flight.
            if (self.join_network_borrows != 0) {
                try self.sendReturnException(join.question_id, "join unavailable");
                return;
            }
            // A Join must not reuse a question id already live as a Call /
            // Bootstrap answer or a Provide (spec violation). Same-type (join)
            // collisions fall through to the orchestration's specific "duplicate
            // join question" abort below.
            if (try self.inboundAnswerQuestionIdInUse(join.question_id) or
                self.provides_by_question.contains(join.question_id))
            {
                return error.DuplicateQuestionId;
            }
            if (try self.tryHandleCrossPeerProxyJoin(join)) return;
            const deadline_key = provide_accept_join.join_state.parseJoinKeyPart(JoinKeyPart, join.key_part) catch null;
            const sampled_deadline = if (deadline_key) |key|
                if (!self.pending_joins.contains(key.join_id))
                    self.newJoinDeadline() catch {
                        try self.sendReturnException(join.question_id, "join unavailable");
                        return;
                    }
                else
                    null
            else
                null;
            const records_before = self.joinRecordCount();
            provide_accept_join.orchestration.handleJoin(
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
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                provide_accept_join.resolveProvideTargetForPeerFn(
                    Peer,
                    provide_accept_join.resolveProvideImportedCapForPeerFn(Peer),
                    provide_accept_join.resolveProvidePromisedAnswerForPeerFn(Peer, Peer.resolvePromisedAnswer),
                ),
                makeProvideTarget,
                ProvideTarget.deinit,
                JoinState.init,
                JoinState.deinit,
                Peer.ensureJoinBudget,
                if (self.join_network != null)
                    completeJoinWithL4Runtime
                else
                    Peer.completeJoinLegacy,
                Peer.sendReturnException,
            ) catch |err| {
                if (err == error.PeerLimitExceeded or
                    err == error.JoinRecordLimitExceeded or
                    err == error.JoinClockReentrant)
                {
                    try self.sendReturnException(join.question_id, "join unavailable");
                    return;
                }
                return err;
            };
            if (deadline_key) |key| {
                if (self.pending_joins.getPtr(key.join_id)) |join_state| {
                    if (!join_state.deadline_initialized) {
                        join_state.deadline_ns = sampled_deadline;
                        join_state.deadline_initialized = true;
                        self.noteJoinDeadline(join_state.deadline_ns);
                    }
                }
            }
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                records_before,
                self.joinRecordCount(),
                self.limits.max_pending_join_records,
            );
        }

        pub fn handleThirdPartyAnswer(self: *Peer, third_party_answer: protocol.ThirdPartyAnswer) !void {
            try third_party.adoption.handleThirdPartyAnswer(
                Peer,
                PendingThirdPartyAwait,
                self.allocator,
                self,
                third_party_answer,
                &self.pending_third_party_awaits,
                &self.pending_third_party_answers,
                third_party.adoption.captureThirdPartyCompletionForPeerFn(
                    Peer,
                    third_party.captureAnyPointerPayloadForPeerFn(Peer, captureAnyPointerPayload),
                ),
                finish.freeOwnedFrameForPeerFn(Peer),
                peer_outbound_control.sendAbortViaSendFrameForPeerFn(Peer, Peer.sendFrameControl),
                Peer.ensurePendingThirdPartyAnswerBudget,
                third_party.adoption.adoptPendingAwaitEntryForPeerFn(
                    Peer,
                    Question,
                    PendingThirdPartyAwait,
                    adoptThirdPartyAnswer,
                ),
            );
        }
    };
}
