const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const events = @import("../events.zig");
const peer_cleanup = @import("./peer_cleanup.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");
const peer_return_frames = @import("./return/peer_return_frames.zig");

/// Peer lifecycle: teardown, graceful shutdown, question cancellation, and
/// the deadline sweep — extracted from `peer/mod.zig` (P10) and made generic
/// over the peer type (the JoinCoordinator extraction contract).
///
/// The `deinit` body moved here AS ONE UNIT: its teardown order is
/// load-bearing (see the BUG #55 liveness note and the neutralize-before-
/// forceCancel choreography inside). Do not reorder or split it. The frozen
/// `Peer.deinit` signature stays byte-identical as a thunk in `peer/mod.zig`;
/// run()-auto-adopt and hoisting the deinit affinity assert were both tried
/// and REVERTED earlier in this sprint — the callback-deferred deinit branch
/// must not assert, and run() frees `self` during teardown.
pub fn Lifecycle(comptime Peer: type) type {
    return struct {
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const Question = state.Question(QuestionCallback);
        const CrossPeerJoinRelay = Peer.CrossPeerJoinRelayRecord;
        const JoinOperationGuards = Peer.JoinOperationGuards;
        const msToNs = Peer.msToNsHelper;
        const disconnected_reason = Peer.disconnected_reason_text;
        const deadline_reason = Peer.deadline_reason_text;
        const shutdown_reason = Peer.shutdown_reason_text;
        const ensureCountLimit = Peer.ensureCountLimit;

        /// Release all owned state: pending calls, resolved answers, export
        /// entries, and the capability table.
        ///
        /// Outstanding questions receive a terminal synthetic "disconnected"
        /// exception Return through their callbacks BEFORE any state is torn
        /// down — deinit of a live peer must not strand callers awaiting a
        /// Return (their heap contexts previously leaked through the
        /// callback-less deinit_ctx sweep). After a transport close this pass
        /// is a no-op: `onConnectionClose` already delivered the terminals.
        pub fn deinit(self: *Peer) void {
            self.assertThreadAffinity();
            if (self.in_deinit) return;
            if (self.automatic_third_party_operation_depth != 0 or
                self.automatic_third_party_dispatch_depth != 0 or
                self.join_operation_depth != 0)
            {
                self.automatic_third_party_deinit_deferred = true;
                self.is_shutting_down = true;
                return;
            }
            self.automatic_third_party_deinit_deferred = false;
            self.in_deinit = true;
            self.is_shutting_down = true;
            // Automatic redirected results borrow peers in both directions. Sever
            // target backlinks first, then retire routes this peer owns, before any
            // callback-bearing question/export teardown can destroy a sibling.
            self.neutralizeAutomaticThirdPartyRoutesOnTargetPeer();
            self.neutralizeAutomaticThirdPartyRoutesOnSourcePeer();
            // LIVENESS (BUG #55): this peer may host held-open Provide questions that
            // recipient peers' `outbound_provides` entries borrow a `provide_peer`
            // pointer back to. `forceCancelAllQuestions` below removes and Finishes
            // those Provide questions, but the borrowed pointer to THIS peer would
            // still dangle. Neutralize it here — null every recipient's back-pointer
            // BEFORE this peer's memory is freed — so a later vine Release is a safe
            // no-op instead of a freed-peer deref. Must run before we return; running
            // it first keeps it independent of any error path below.
            // Vat-wide provision teardown, split per the canonical procedure:
            // infallible neutralize (sever the index back-link, move the owner map
            // out, mark provisions closed, clear holder records) BEFORE
            // forceCancelAllQuestions; the send-bearing drain AFTER it.
            self.detachCrossPeerAcceptsOnHolderPeer();
            self.drainOutboundProvidesOnRecipientPeer();
            var provision_teardown = self.neutralizeProvisionsOnOwnerPeer();
            self.neutralizeCoupledVinesOnProvidePeer();
            self.neutralizeForwardVineRelaysOnRecipientPeer();
            self.neutralizeHandoffPickupsOnPromisePeer();
            self.neutralizeCrossPeerProxiesOnSourcePeer();
            self.neutralizeCrossPeerJoinRelaysOnSourcePeer();
            self.cancelJoinAcceptHostLinks();
            self.cancelAllHostedJoins();
            _ = forceCancelAllQuestions(self, disconnected_reason, .disconnected);
            self.neutralizeJoinCoordinatorResultLinks();
            self.neutralizeJoinCoordinatorAcceptLinks();
            self.drainClosedProvisionsOnOwnerPeer(&provision_teardown);
            peer_cleanup.deinitPendingCallMapOwned(
                @TypeOf(self.pending_promises),
                self.allocator,
                &self.pending_promises,
            );
            peer_cleanup.deinitPendingCallMapOwned(
                @TypeOf(self.pending_export_promises),
                self.allocator,
                &self.pending_export_promises,
            );
            peer_cleanup.deinitResolvedAnswerMap(
                @TypeOf(self.resolved_answers),
                self.allocator,
                &self.resolved_answers,
            );
            // NOTE: deinit_ctx callbacks must NOT access peer maps (loopback_questions,
            // forwarded_questions, etc.) as they may be deinited after this loop.
            {
                var q_it = self.questions.valueIterator();
                while (q_it.next()) |q| {
                    if (q.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, q.ctx);
                }
            }
            // Questions parked in pending_third_party_awaits were moved out of the
            // questions map, so the loop above never sees them. Invoke their
            // deinit_ctx too (heap Save/Restore/ForwardCallContext), or a teardown
            // with in-flight three-party handoffs leaks them. The owned map keys are
            // freed by deinitOwnedStringKeyMap below.
            {
                var await_it = self.pending_third_party_awaits.valueIterator();
                while (await_it.next()) |pending_await| {
                    if (pending_await.question.deinit_ctx) |deinit_ctx| {
                        deinit_ctx(self.allocator, pending_await.question.ctx);
                    }
                }
            }
            self.questions.deinit();
            self.retained_questions.deinit();
            // forceCancelAllQuestions above already freed the param-export record
            // of every question still in the map; sweep any stragglers (there
            // should be none — records never outlive their question) so a
            // bookkeeping bug degrades to a counter leak, not a memory leak.
            {
                var rec_it = self.question_param_export_refs.valueIterator();
                while (rec_it.next()) |ids| ids.deinit(self.allocator);
            }
            self.question_param_export_refs.deinit();
            self.active_inbound_questions.deinit();
            self.resolving_answers.deinit();
            self.finished_early_answers.deinit();
            {
                var f_it = self.failed_answers.valueIterator();
                while (f_it.next()) |failed| self.allocator.free(failed.reason);
            }
            self.failed_answers.deinit();
            {
                var p_it = self.persistent_exports.valueIterator();
                while (p_it.next()) |st| self.allocator.destroy(st.*);
            }
            self.persistent_exports.deinit();
            {
                var e_it = self.exports.valueIterator();
                while (e_it.next()) |entry| {
                    if (entry.deinit_ctx) |deinit_ctx| {
                        if (entry.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
                    }
                }
            }
            self.exports.deinit();
            self.forwarded_questions.deinit();
            self.forwarded_tail_questions.deinit();
            peer_cleanup.deinitProvideEntryMap(
                @TypeOf(self.provides_by_question),
                self.allocator,
                &self.provides_by_question,
            );
            self.provides_by_key.deinit();
            // The FRESH (post-neutralize) vat-provision maps: contents were moved
            // out and drained above; only the backing stores remain.
            self.provisions_by_question.deinit();
            self.cross_peer_pending_accepts.deinit();
            // Recipient-side couplings were moved out and converted to durable
            // provider Finish requests before any callback-bearing teardown.
            std.debug.assert(self.outbound_provides.count() == 0);
            self.outbound_provides.deinit();
            // Symmetric back-link list (this peer as a provide_peer). Any residual
            // entries were neutralized at the top of deinit; free the backing store.
            self.coupled_vines.deinit(self.allocator);
            self.forward_vine_relay_links.deinit(self.allocator);
            self.handoff_pickup_links.deinit(self.allocator);
            self.cross_peer_proxy_links.deinit(self.allocator);
            self.cross_peer_join_relay_links.deinit(self.allocator);

            peer_cleanup.deinitJoinStateMap(
                @TypeOf(self.pending_joins),
                self.allocator,
                &self.pending_joins,
            );
            self.pending_join_questions.deinit();
            std.debug.assert(self.completing_join_answers.count() == 0);
            std.debug.assert(self.completing_join_records == 0);
            std.debug.assert(self.completing_join_answer_records == 0);
            std.debug.assert(self.join_network_borrows == 0);
            self.completing_join_answers.deinit();
            {
                // Move the entire owner map out, then sever every reciprocal link
                // before the first Finish send can re-enter either peer. The fresh
                // empty map is what callbacks and public stats observe.
                var owned_relays = self.pending_join_relays;
                self.pending_join_relays = std.AutoHashMap(u32, CrossPeerJoinRelay).init(self.allocator);
                var relay_it = owned_relays.iterator();
                while (relay_it.next()) |entry| {
                    if (entry.value_ptr.source_peer) |source_peer| {
                        source_peer.deregisterCrossPeerJoinRelay(self, entry.key_ptr.*);
                    }
                }
                relay_it = owned_relays.iterator();
                while (relay_it.next()) |entry| {
                    if (entry.value_ptr.source_peer) |source_peer| {
                        var guards = JoinOperationGuards{};
                        guards.add(self);
                        guards.add(source_peer);
                        guards.enter();
                        source_peer.sendJoinRelayFinishAndNeutralize(entry.value_ptr.source_question_id, false) catch |err| {
                            log.debug("cross-peer join relay: failed to finish downstream question {} during deinit: {}", .{
                                entry.value_ptr.source_question_id,
                                err,
                            });
                            source_peer.neutralizeJoinRelayQuestion(entry.value_ptr.source_question_id);
                        };
                        guards.leave();
                    }
                }
                owned_relays.deinit();
                self.pending_join_relays.deinit();
            }

            peer_cleanup.deinitOwnedStringKeyListMap(
                @TypeOf(self.pending_accepts_by_embargo),
                self.allocator,
                &self.pending_accepts_by_embargo,
            );
            // Values in pending_accept_embargo_by_question are borrowed from
            // pending_accepts_by_embargo (already freed above), so just deinit.
            self.pending_accept_embargo_by_question.deinit();
            std.debug.assert(self.pending_join_accepts.count() == 0);
            self.pending_join_accepts.deinit();
            std.debug.assert(self.pending_join_result_answers.count() == 0);
            self.pending_join_result_answers.deinit();
            std.debug.assert(self.hosted_joins.count() == 0);
            self.hosted_joins.deinit();
            self.join_accept_host_links.deinit(self.allocator);
            self.join_coordinator_accept_links.deinit(self.allocator);
            self.join_coordinator_result_links.deinit(self.allocator);
            peer_cleanup.deinitOwnedStringKeyMap(
                @TypeOf(self.pending_third_party_awaits),
                self.allocator,
                &self.pending_third_party_awaits,
            );
            peer_cleanup.deinitOwnedStringKeyMap(
                @TypeOf(self.pending_third_party_answers),
                self.allocator,
                &self.pending_third_party_answers,
            );
            peer_cleanup.deinitOwnedBytesMap(
                @TypeOf(self.pending_third_party_returns),
                self.allocator,
                &self.pending_third_party_returns,
            );
            self.adopted_third_party_answers.deinit();

            self.resolved_imports.deinit();
            self.pending_embargoes.deinit();
            self.loopback_questions.deinit();
            self.send_results_to_yourself.deinit();
            {
                var stash_it = self.loopback_result_stash.valueIterator();
                while (stash_it.next()) |frame| self.allocator.free(frame.*);
            }
            self.loopback_result_stash.deinit();
            peer_cleanup.deinitOptionalOwnedBytesMap(
                @TypeOf(self.send_results_to_third_party),
                self.allocator,
                &self.send_results_to_third_party,
            );
            std.debug.assert(self.automatic_third_party_routes.count() == 0);
            std.debug.assert(self.incoming_automatic_third_party_routes.count() == 0);
            self.automatic_third_party_routes.deinit();
            self.incoming_automatic_third_party_routes.deinit();
            peer_cleanup.clearOptionalOwnedBytes(self.allocator, &self.last_remote_abort_reason);
            releaseAllImports(self);
            self.caps.deinit();
        }

        /// Best-effort: send Release messages for all remaining imports so the
        /// remote peer can decrement its export ref counts.
        ///
        /// Errors are logged but not propagated because this runs during `deinit`
        /// when the transport may already be closed or in an error state.
        ///
        /// If the transport send function is null (i.e. the connection was
        /// already destroyed or never attached), we skip sending entirely --
        /// there is no peer to receive the Release messages.
        pub fn releaseAllImports(self: *Peer) void {
            // If neither a send-frame override nor the transport send function is
            // available, the connection is already gone -- skip sending.
            if (self.send_frame_override == null and self.transport.send == null) {
                log.debug("releaseAllImports: transport not attached, skipping release messages", .{});
                return;
            }
            var it = self.caps.imports.iterator();
            while (it.next()) |entry| {
                // The sweep stays EAGER by design (never routed through the
                // handoff withhold seam), and it settles the whole debt: live
                // wire refs PLUS any `deferred_release` still withheld under an
                // abandoned handoff pin — that tally was never emitted and the
                // table dies right after this walk, so this is its only exit.
                peer_outbound_control.sendReleaseViaSendFrame(
                    Peer,
                    self,
                    entry.key_ptr.*,
                    entry.value_ptr.ref_count +| entry.value_ptr.deferred_release,
                    Peer.sendFrameControl,
                ) catch |err| {
                    log.debug("releaseAllImports: failed to send release for import {}: {}", .{ entry.key_ptr.*, err });
                };
            }
        }

        /// Begin a graceful shutdown: reject new outbound calls, wait for
        /// outstanding questions to receive their Return, then fire `on_complete`.
        ///
        /// If there are no outstanding questions the callback fires immediately.
        /// Calling `shutdown` a second time is a no-op.
        pub fn shutdown(self: *Peer, on_complete: ?*const fn (peer: *Peer) void) void {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return;
            self.is_shutting_down = true;
            self.shutdown_callback = on_complete;

            if (self.questions.count() == 0) {
                completeShutdown(self);
                return;
            }
            // Stamp the drain bound; `checkDeadlines` force-cancels stragglers
            // once it passes.
            if (self.clockNow()) |now| {
                if (self.timeouts.shutdown_drain_timeout_ms) |ms| {
                    self.shutdown_deadline_ns = now + msToNs(ms);
                }
            }
        }

        /// Finish the graceful-shutdown sequence: close the transport (if any)
        /// and fire the shutdown callback. Public so cross-module question
        /// bookkeeping (return orchestration) can complete a drain; not
        /// intended for direct application use.
        pub fn completeShutdown(self: *Peer) void {
            self.assertThreadAffinity();
            if (self.transport.ctx) |transport_ctx| {
                // Close transport if attached and not already closing.
                if (self.transport.close) |close_fn| {
                    if (self.transport.is_closing) |is_closing_fn| {
                        if (!is_closing_fn(transport_ctx)) {
                            close_fn(transport_ctx);
                        }
                    } else {
                        close_fn(transport_ctx);
                    }
                }
            }
            if (self.shutdown_callback) |cb| {
                self.shutdown_callback = null;
                cb(self);
            }
        }

        /// Finish a completed caller-owned retained answer. The entry is removed
        /// only after the Finish frame is accepted by the transport; a send failure
        /// restores it to the returned state so the caller can retry. A retained
        /// loopback result has no remote answer table and retires locally.
        pub fn finishRetainedQuestion(
            self: *Peer,
            question_id: u32,
            release_result_caps: bool,
        ) !void {
            self.assertThreadAffinity();
            const target = try self.retained_questions.beginCallerFinish(question_id);
            errdefer self.retained_questions.rollbackCallerFinish(question_id);
            if (!target.is_loopback) {
                try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                    Peer,
                    self,
                    target.wire_answer_id,
                    release_result_caps,
                    false,
                    Peer.sendFrameControl,
                );
            }
            _ = self.retained_questions.completeFinish(question_id);
        }

        /// Internal ownership seam for Level-3 handoff integration. Claim is
        /// allocation-free and is valid only after the terminal Return became
        /// callback-visible. Commit keeps the entry tracked as transferred until
        /// that lifecycle successfully Finishes the source answer.
        pub fn claimRetainedQuestionForTransfer(self: *Peer, question_id: u32) !u32 {
            self.assertThreadAffinity();
            return try self.retained_questions.beginTransfer(question_id);
        }

        pub fn rollbackRetainedQuestionTransfer(self: *Peer, question_id: u32) void {
            self.assertThreadAffinity();
            self.retained_questions.rollbackTransfer(question_id);
        }

        pub fn commitRetainedQuestionTransfer(self: *Peer, question_id: u32) !void {
            self.assertThreadAffinity();
            if (!self.retained_questions.commitTransfer(question_id)) {
                return error.RetainedQuestionTransferNotInProgress;
            }
        }

        /// Internal transfer-owner Finish. On failure the transferred answer stays
        /// live and retryable; on success it leaves all retained gauges exactly once.
        pub fn finishTransferredRetainedQuestion(
            self: *Peer,
            question_id: u32,
            release_result_caps: bool,
        ) !void {
            self.assertThreadAffinity();
            const target = try self.retained_questions.beginTransferredFinish(question_id);
            errdefer self.retained_questions.rollbackTransferredFinish(question_id);
            if (!target.is_loopback) {
                try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                    Peer,
                    self,
                    target.wire_answer_id,
                    release_result_caps,
                    false,
                    Peer.sendFrameControl,
                );
            }
            _ = self.retained_questions.completeFinish(question_id);
        }

        /// Cancel an outstanding outbound question.
        ///
        /// The local callback is delivered an exception Return carrying
        /// `reason` immediately, and a Finish with `releaseResultCaps` is sent
        /// to the remote. Per the Cap'n Proto RPC spec the remote still sends
        /// exactly one Return for the question (possibly `canceled`); the
        /// question entry stays in the table, marked cancelled, so that Return
        /// is absorbed silently when it arrives. Loopback questions complete
        /// locally and are removed outright.
        pub fn cancelQuestion(self: *Peer, question_id: u32, reason: []const u8) !void {
            return cancelQuestionTyped(self, question_id, reason, .failed);
        }

        /// `cancelQuestion` carrying an explicit `Exception.Type` for the locally
        /// synthesized exception the caller observes.
        pub fn cancelQuestionTyped(
            self: *Peer,
            question_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            self.assertThreadAffinity();
            const logical_question_id = if (self.retained_questions.contains(question_id))
                question_id
            else
                self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
            const wire_answer_id = if (self.retained_questions.get(logical_question_id)) |retained|
                retained.wire_answer_id
            else
                question_id;
            const entry = self.questions.getPtr(wire_answer_id) orelse return error.UnknownQuestion;
            if (entry.cancelled) return;
            const question = entry.*;

            // Cancellation owns the answer lifetime from this point onward: it
            // emits Finish itself and absorbs the mandatory late Return, so no
            // caller-owned retained record remains.
            _ = self.retained_questions.retire(logical_question_id);

            if (question.is_loopback) {
                _ = self.loopback_questions.remove(wire_answer_id);
                self.removeQuestion(wire_answer_id);
                try deliverLocalException(self, question, logical_question_id, reason, ex_type);
                return;
            }

            // Delivery transfers ctx ownership to the callback; drop the
            // undelivered-cleanup hook so peer deinit cannot double-free.
            entry.cancelled = true;
            entry.deadline_ns = null;
            entry.deinit_ctx = null;

            // Tell the remote we no longer want the answer. A send failure is
            // tolerated: the local caller still observes the exception, and
            // transport teardown reconciles remote state.
            peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                wire_answer_id,
                true,
                false,
                Peer.sendFrameControl,
            ) catch |err| {
                log.debug("cancel finish send failed for question {}: {}", .{ wire_answer_id, err });
            };

            try deliverLocalException(self, question, logical_question_id, reason, ex_type);
        }

        /// Cancel every question whose deadline has passed, and enforce the
        /// shutdown drain bound. Returns the number of questions cancelled.
        ///
        /// Call this periodically — typically from a transport tick (see
        /// `Connection.Options.tick_interval_ms`) or a test harness. A peer
        /// without a clock returns 0 immediately.
        pub fn checkDeadlines(self: *Peer) usize {
            self.assertThreadAffinity();
            self.enterJoinOperation();
            defer self.leaveJoinOperation();

            // The vat-wide parked-Accept clock may be configured independently of
            // this peer's outbound-question clock. Sweep it before the early
            // return below, and do not include detached parks in this method's
            // documented outbound-question cancellation count.
            if (self.provision_index) |idx| _ = idx.sweepExpiredParkedAccepts();
            _ = self.sweepExpiredJoins();

            // A failed Finish send must not orphan a completed Provide or its
            // transferred source answer. Retry those locally-ended lifetimes on
            // the normal maintenance path, without changing this method's return
            // value (which remains the count of cancelled questions).
            self.retryDeferredFinishes();

            const now = self.clockNow() orelse return 0;

            var expired: std.ArrayList(u32) = .empty;
            defer expired.deinit(self.allocator);
            var it = self.questions.iterator();
            while (it.next()) |kv| {
                if (kv.value_ptr.cancelled or kv.value_ptr.finish_on_maintenance) continue;
                const deadline = kv.value_ptr.deadline_ns orelse continue;
                if (now >= deadline) expired.append(self.allocator, kv.key_ptr.*) catch break;
            }

            var cancelled: usize = 0;
            for (expired.items) |question_id| {
                events.emitTimeout(self.observer, .peer, .unknown, .call_deadline, question_id);
                cancelQuestionTyped(self, question_id, deadline_reason, .overloaded) catch |err| {
                    log.debug("deadline cancel failed for question {}: {}", .{ question_id, err });
                    continue;
                };
                cancelled += 1;
            }

            // Parked third-party-await questions are not in the questions map, so
            // the loop above misses them; sweep expired ones so they cannot escape
            // deadline enforcement.
            cancelled += self.sweepThirdPartyAwaits(true);

            if (self.is_shutting_down) {
                if (self.shutdown_deadline_ns) |drain_deadline| {
                    if (now >= drain_deadline and self.questions.count() != 0) {
                        self.shutdown_deadline_ns = null;
                        events.emitTimeout(self.observer, .peer, .unknown, .shutdown_drain, null);
                        cancelled += forceCancelAllQuestions(self, shutdown_reason, .disconnected);
                    }
                }
            }
            return cancelled;
        }

        /// Synthesize an exception Return for `question_id` and deliver it to
        /// the question's callback, exactly as if the remote had answered.
        ///
        /// Ctx ownership: once the callback runs it owns `question.ctx`
        /// (generated callbacks destroy it unconditionally). If synthesis fails
        /// before the callback could run, the ctx is freed here via
        /// `question.deinit_ctx` — callers have already removed the entry from
        /// the questions map (or dropped its cleanup hook), so nothing else can.
        pub fn deliverLocalException(
            self: *Peer,
            question: Question,
            question_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            var callback_ran = false;
            errdefer if (!callback_ran) {
                if (question.deinit_ctx) |deinit_ctx| deinit_ctx(self.allocator, question.ctx);
            };
            // Synthetic and LOCAL: delivered straight to our own question callback,
            // never sent, and never routed through `handleReturn`. Nothing consumes
            // its `releaseParamCaps`, so it keeps the rpc.capnp default.
            const frame = try peer_return_frames.buildReturnExceptionFrame(
                self.allocator,
                question_id,
                reason,
                ex_type,
                true,
            );
            defer self.allocator.free(frame);
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            const ret = try decoded.asReturn();
            var inbound_caps = try cap_table.InboundCapTable.init(self.allocator, null, &self.caps);
            defer inbound_caps.deinit();
            callback_ran = true;
            question.on_return(question.ctx, self, ret, &inbound_caps) catch |err| {
                if (err == error.OutOfMemory) return error.OutOfMemory;
                log.debug("cancel exception callback error for question {}: {}", .{ question_id, err });
            };
        }

        /// Remove and cancel every outstanding question (drain-bound
        /// enforcement). Unlike `cancelQuestion` this does not keep entries
        /// for late Returns — the transport is about to close.
        pub fn forceCancelAllQuestions(self: *Peer, reason: []const u8, ex_type: protocol.ExceptionType) usize {
            var ids: std.ArrayList(u32) = .empty;
            defer ids.deinit(self.allocator);
            var it = self.questions.keyIterator();
            while (it.next()) |key| ids.append(self.allocator, key.*) catch break;

            var cancelled: usize = 0;
            for (ids.items) |question_id| {
                const removed = self.questions.fetchRemove(question_id) orelse continue;
                const question = removed.value;
                const logical_question_id = self.retained_questions.logicalQuestionIdForWire(question_id) orelse question_id;
                _ = self.retained_questions.retire(logical_question_id);
                _ = self.loopback_questions.remove(question_id);
                // No wire Return will consume this question's param-export
                // record; free it without spending the refs (transport teardown
                // reconciles export state, as before the record existed).
                self.freeQuestionParamExports(question_id);
                if (question.cancelled) continue; // exception already delivered
                if (!question.is_loopback) {
                    peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                        Peer,
                        self,
                        question_id,
                        true,
                        false,
                        Peer.sendFrameControl,
                    ) catch |err| {
                        log.debug("drain finish send failed for question {}: {}", .{ question_id, err });
                    };
                }
                deliverLocalException(self, question, logical_question_id, reason, ex_type) catch |err| {
                    log.debug("drain exception delivery failed for question {}: {}", .{ logical_question_id, err });
                };
                cancelled += 1;
            }
            cancelled += self.sweepThirdPartyAwaits(false);
            if (self.is_shutting_down and !self.in_deinit and self.questions.count() == 0) {
                completeShutdown(self);
            }
            return cancelled;
        }
    };
}
