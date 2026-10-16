const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");
const return_routing = @import("../../promises/return_routing.zig");
const return_send = @import("../../promises/return_send.zig");
const finish = @import("../finish.zig");
const peer_call_targets = @import("../call/peer_call_targets.zig");
const peer_return_dispatch = @import("./peer_return_dispatch.zig");
const peer_return_frames = @import("./peer_return_frames.zig");
const state = @import("../state.zig");
const peer_fds = @import("../peer_fds.zig");
const peer_export_release = @import("../peer_export_release.zig");

/// The outbound Return send family, extracted from `peer/mod.zig` and made
/// generic over the peer type (the JoinCoordinator extraction contract):
/// results/exception/tag/takeFromOtherQuestion/acceptFromThirdParty Returns,
/// the reflected self-loopback completion and its stash delivery, prebuilt
/// Return frames, failed-answer records, and the queued-pipelined-call drain.
/// `peer/mod.zig` keeps every caller-visible name as a thunk on `Peer`, so
/// signatures, hook fn-types, and the api-snapshot rendering are unchanged.
pub fn ReturnSend(comptime Peer: type) type {
    return struct {
        const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
        const ResolvedAnswerReservation = Peer.ResolvedAnswerReservation;
        const ProvideTarget = state.ProvideTarget;

        const results_sent_elsewhere_no_pipelining =
            "results sent elsewhere; pipelining is not supported on a redirected answer";
        const answer_canceled_no_pipelining = "call canceled";

        pub fn sendReturnResults(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
            self.assertThreadAffinity();
            if (self.automatic_third_party_routes.get(answer_id)) |route| {
                return self.sendAutomaticThirdPartyResults(route, ctx, build);
            }
            // sendResultsTo routing is resolved in precedence order:
            // third-party handoff > local results-sent-elsewhere marker > normal results payload.
            if (self.send_results_to_third_party.contains(answer_id)) {
                // This answer's results were redirected to a third vat, so we are
                // holding results we cannot deliver. Refuse loudly rather than drop
                // them: the marker is deliberately LEFT IN PLACE, because every
                // dispatch site converts an error from here into exactly one
                // exception Return, and that path clears the routing state and frees
                // the captured payload. An application that performed the redirect
                // itself settles the answer with sendReturnResultsSentElsewhere.
                //
                // Under the default `.reject` policy this is unreachable: handleCall
                // refuses such calls before the marker is ever recorded.
                return error.ThirdPartyResultsNotRedirected;
            }

            // The marker stays until the results are built: when `build` (a
            // DIRECT handler's body) fails, the caller answers with
            // sendReturnException, which must still see this call as a
            // `yourself` call (see completeSelfLoopbackReturn).
            if (self.send_results_to_yourself.contains(answer_id)) {
                try completeSelfLoopbackReturn(self, answer_id, ctx, build);
                return;
            }

            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            var effects = cap_table.OutboundCapEffects.init(self.allocator, self, Peer.rollbackOutboundCap);
            defer effects.deinit();
            var effects_committed = false;
            errdefer if (!effects_committed) effects.rollback();

            var ret = try builder.beginReturn(answer_id, .results);
            // Stamp BEFORE `build`: relay builders that computed their own value
            // (buildCrossPeerReturnResults) still override it, and an application
            // build fn keeps the last word for answers it settles itself.
            ret.setReleaseParamCaps(self.returnReleasesParamCaps(answer_id));
            try build(ctx, &ret);

            // Capture before delivery: sendReturnFrameWithLoopback consumes the
            // loopback marker. Do not record a resolved answer for loopback
            // answers: they are delivered locally, are never referenced by a
            // remote PromisedAnswer, and get no Finish — and their outbound-counter
            // ids would collide with the remote question-id space, so a recorded
            // entry would leak and poison DuplicateQuestionId checks.
            //
            // Finished-early answers (Finish already arrived — before this late
            // async Return, or reentrantly while this send is on the stack) still
            // reserve: the post-send commit step re-checks the tombstone map and,
            // when a tombstone is present, commits the reserved answer FIRST so
            // calls pipelined on it replay with their own Returns, then
            // immediately applies the Finish cleanup. Skipping the reservation
            // would strand those parked calls with no Return at all.
            const is_loopback = self.loopback_questions.contains(answer_id);

            // A loopback Return comes straight back to this peer, which reads
            // its results from our own side (`deliverLoopbackReturn`): it
            // takes no wire reference on our exports, so none is noted.
            if (is_loopback) {
                try cap_table.outbound.encodeLoopbackReturnPayloadCaps(&self.caps, &ret);
            } else {
                _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &ret, Peer.onOutboundCap, &effects);
            }

            // Fd passing: a Return that goes on the wire carries the fds of
            // its senderHosted results (`peer_fds.zig`); a loopback one never
            // leaves the process and carries none.
            var frame_fds: peer_fds.OutboundFds = .{};
            if (!is_loopback) try peer_fds.PeerFds(Peer).attachPayload(self, ret.payload, &frame_fds);

            const bytes = try builder.finish();
            defer self.allocator.free(bytes);
            const should_record = !is_loopback;

            // Reserve the record resources (count budget, map slot, frame copy)
            // BEFORE sending so recording is infallible afterward. If it could fail
            // after the frame is on the wire, the error would drive the dispatch
            // catch to send a second exception Return for this answer (audit item 7).
            var reservation: ?ResolvedAnswerReservation = null;
            errdefer if (reservation) |r| r.deinit(self);
            var resolving_answer = false;
            errdefer if (resolving_answer) {
                _ = self.resolving_answers.remove(answer_id);
            };
            if (should_record) {
                reservation = try self.reserveResolvedAnswer(answer_id, bytes);
                try self.resolving_answers.put(answer_id, {});
                resolving_answer = true;
            }

            const completing_finished_before_send = if (self.completing_join_answers.get(answer_id)) |completing|
                completing.finished
            else
                false;
            const finished_before_send = self.finished_early_answers.contains(answer_id) or
                completing_finished_before_send;
            sendReturnFrameWithFds(self, answer_id, bytes, frame_fds.slice()) catch |err| {
                // A synchronous transport can deliver the complete Return, receive
                // the peer's Finish reentrantly, and only then report a trailing
                // local send error. A newly-created Finish tombstone while this
                // answer is in `resolving_answers` is definitive consumption proof:
                // never roll back the visible terminal or send a second one.
                const completing_finished_after_send = if (self.completing_join_answers.get(answer_id)) |completing|
                    completing.finished
                else
                    false;
                const finish_proves_delivery = resolving_answer and
                    !finished_before_send and
                    self.resolving_answers.contains(answer_id) and
                    (self.finished_early_answers.contains(answer_id) or completing_finished_after_send);
                if (!finish_proves_delivery) return err;

                log.debug("Return {} consumed before trailing send error: {}", .{ answer_id, err });
                cap_table.commitOutboundCapEffects(&self.caps, &effects);
                effects_committed = true;
                _ = self.resolving_answers.remove(answer_id);
                resolving_answer = false;
                commitOrRollbackResolvedAnswerAfterSend(self, answer_id, bytes, &reservation);
                return;
            };
            cap_table.commitOutboundCapEffects(&self.caps, &effects);
            effects_committed = true;
            if (resolving_answer) {
                _ = self.resolving_answers.remove(answer_id);
                resolving_answer = false;
            }

            commitOrRollbackResolvedAnswerAfterSend(self, answer_id, bytes, &reservation);
        }

        /// Complete a return for an inbound call that carried `sendResultsTo =
        /// yourself` (the reflected-loopback / Level-1 tail-call optimization).
        ///
        /// The forwarder (the peer that parked this call on a promise and later
        /// resolved that promise to a capability we host) does NOT want the results
        /// on the wire — it will name the local question that consumes them via a
        /// `takeFromOtherQuestion` Return. Two things must still happen here:
        ///
        ///   1. The user handler MUST run. For generated DIRECT handlers the handler
        ///      body lives inside `build`, so we run `build` unconditionally to
        ///      produce the results — otherwise the handler would silently never
        ///      execute on this path (a real Level-1 self-loopback correctness gap).
        ///   2. The forwarder is told `resultsSentElsewhere`.
        ///
        /// The computed results go to the caller's own question that the
        /// matching inbound `takeFromOtherQuestion` names — completing the value
        /// round-trip locally without a wire round-trip through the forwarder:
        /// at once when that redirect is already held (`held_tail_calls`), else
        /// through the stash when it arrives (see `settleSelfLoopbackAnswer`).
        /// Results carrying capabilities cannot be re-delivered locally (their
        /// descriptors are wire-encoded for the forwarder), so the caller falls
        /// back to receiving the `takeFromOtherQuestion` relay tag.
        fn completeSelfLoopbackReturn(self: *Peer, answer_id: u32, ctx: *anyopaque, build: ReturnBuildFn) !void {
            // Build the results (runs the user handler) into a standalone frame.
            // Encode caps with rollback: these results never traverse the wire to
            // the forwarder, so any outbound wire refs the encode takes must be
            // undone. The encode still classifies descriptors so we can detect
            // whether the results carry capabilities.
            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            var effects = cap_table.OutboundCapEffects.init(self.allocator, self, Peer.rollbackOutboundCap);
            defer effects.deinit();

            var ret = try builder.beginReturn(answer_id, .results);
            ret.setReleaseParamCaps(self.returnReleasesParamCaps(answer_id));
            // `build` can receive the remote's Finish of this call (a
            // handler body that waits on the connection, or a synchronous
            // transport). That Finish settles a held tail call with
            // `tail_call_finished_reason`, and its redirect is then used up.
            const held_before_build = self.held_tail_calls.by_source.contains(answer_id);
            try build(ctx, &ret);
            const hold_settled_during_build = held_before_build and
                !self.held_tail_calls.by_source.contains(answer_id);
            _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &ret, Peer.onOutboundCap, &effects);
            effects.rollback();

            const frame = try builder.finish();
            const has_result_caps = selfLoopbackResultsHaveCaps(self.allocator, frame) catch |err| {
                self.allocator.free(frame);
                return err;
            };
            // The results are built: the call is answered from here on. Until
            // now a failure left the marker, so the caller's follow-up
            // sendReturnException (generated dispatch turns a handler error
            // into one; a native host calls capnp_return_exception) still
            // answers `resultsSentElsewhere` and settles the tail call with
            // the error. A Finish during `build` already removed the marker.
            _ = self.send_results_to_yourself.remove(answer_id);
            // No redirect is left to take results with capabilities, or
            // results whose held redirect a Finish during `build` already
            // ended: keeping them would only leave a stale stash entry.
            if (has_result_caps or hold_settled_during_build) {
                self.allocator.free(frame);
                return settleSelfLoopbackAnswer(self, answer_id, null);
            }
            return settleSelfLoopbackAnswer(self, answer_id, @constCast(frame));
        }

        /// Answer a `sendResultsTo = yourself` call with an exception. rpc.capnp
        /// answers such a call with `resultsSentElsewhere`, also when it fails,
        /// and the C++ reference rejects an exception Return for its tail call
        /// as a protocol error. So the remote gets `resultsSentElsewhere`, and
        /// the exception goes where results would: to the question whose
        /// `takeFromOtherQuestion` names this call.
        ///
        /// Like a successful `yourself` answer, the exception is not recorded
        /// in `failed_answers`: a call pipelined on this answer after its
        /// Return does not find it. Calls already queued on it are still
        /// failed (`sendReturnExceptionTyped` drains them).
        fn completeSelfLoopbackException(
            self: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            const frame = try peer_return_frames.buildReturnExceptionFrame(
                self.allocator,
                answer_id,
                reason,
                ex_type,
                self.returnReleasesParamCaps(answer_id),
            );
            _ = self.send_results_to_yourself.remove(answer_id);
            return settleSelfLoopbackAnswer(self, answer_id, @constCast(frame));
        }

        /// Settle an answered `sendResultsTo = yourself` call. `frame` is the
        /// call's Return (cap-free results or an exception), owned and freed
        /// here; null for results that carry capabilities, which cannot be
        /// delivered locally. The remote is told `resultsSentElsewhere`. The
        /// Return goes to the question whose `takeFromOtherQuestion` names this
        /// call: now when that redirect is held (`held_tail_calls`), else through
        /// the stash when it arrives.
        fn settleSelfLoopbackAnswer(self: *Peer, answer_id: u32, frame: ?[]u8) !void {
            var owned = frame;
            defer if (owned) |bytes| self.allocator.free(bytes);

            // Take the hold before the send: a synchronous transport can
            // deliver the remote's Finish for this call inside it, and that
            // Finish must not settle the question first.
            const held_question = self.held_tail_calls.takeBySource(answer_id);
            if (held_question == null) {
                // Stash BEFORE telling the forwarder: in a synchronous in-process
                // transport, the `resultsSentElsewhere` we send can re-enter us
                // with the matching `takeFromOtherQuestion` before this call
                // returns, and that handler consumes the stash.
                if (owned) |bytes| {
                    if (self.loopback_result_stash.count() < self.limits.max_loopback_result_stash) {
                        // fetchPut, not put: a duplicate/hostile answer_id can
                        // complete a self-loopback return twice before a
                        // takeFromOtherQuestion consumes the first, and a plain
                        // put would overwrite — and leak — the stashed frame.
                        // Out of memory degrades as a full stash does: the call
                        // still gets its Return, the redirect its bare tag.
                        if (self.loopback_result_stash.fetchPut(answer_id, bytes)) |previous| {
                            if (previous) |old| self.allocator.free(old.value);
                            owned = null;
                        } else |err| {
                            log.debug("self-loopback Return {} not stashed: {}", .{ answer_id, err });
                        }
                    }
                }
            }
            // The held question gets this call's terminal even if the send
            // below fails: the value is decided here, not by the wire. Runs
            // before `owned` is freed.
            defer if (held_question) |question_id| deliverHeldTailCall(self, question_id, answer_id, owned);

            sendReturnTag(self, answer_id, .resultsSentElsewhere) catch |err| {
                // The remote got no Return, and the caller answers again
                // with an exception (generated dispatch does; a native host
                // answers a failed capnp_return_results with
                // capnp_return_exception). Mark the call again, so that
                // answer still sends `resultsSentElsewhere`, never an
                // exception Return for a tail call. Not when the remote
                // finished the call during the send, or the Return went
                // out after all: then nothing is owed.
                if (self.active_inbound_questions.contains(answer_id) and
                    !self.finished_early_answers.contains(answer_id))
                {
                    self.send_results_to_yourself.put(answer_id, {}) catch |mark_err| {
                        log.debug("yourself mark for {} not restored: {}", .{ answer_id, mark_err });
                    };
                }
                return err;
            };
        }

        /// Hold a `takeFromOtherQuestion` Return for `question_id` when the call
        /// it names, `source_answer_id`, is an unanswered `sendResultsTo =
        /// yourself` call (the C++ reference sends the redirect right after that
        /// call). False, with nothing held, otherwise or when the hold table is
        /// full or out of memory: the question then gets the bare redirect, as
        /// before holds existed.
        pub fn holdTailCall(self: *Peer, source_answer_id: u32, question_id: u32) bool {
            if (!self.send_results_to_yourself.contains(source_answer_id)) return false;
            if (self.held_tail_calls.count() >= self.limits.max_send_results_to_yourself) return false;
            return self.held_tail_calls.put(self.allocator, source_answer_id, question_id) catch |err| {
                log.debug("tail call for question {} not held: {}", .{ question_id, err });
                return false;
            };
        }

        /// The remote finished `source_answer_id` before it was answered: a tail
        /// call held on it can no longer get its results. A question cancelled
        /// meanwhile is retired, as its late Return would retire it; any other
        /// ends with `tail_call_finished_reason`.
        pub fn settleTailCallOnFinish(self: *Peer, source_answer_id: u32) void {
            const question_id = self.held_tail_calls.takeBySource(source_answer_id) orelse return;
            if (!self.questions.contains(question_id)) return;
            const frame = peer_return_frames.buildReturnExceptionFrame(
                self.allocator,
                question_id,
                Peer.tail_call_finished_reason_text,
                .failed,
                false,
            ) catch |err| return reportNonfatalError(self, err);
            defer self.allocator.free(frame);
            deliverStashedLoopbackResults(self, question_id, frame) catch |err| reportNonfatalError(self, err);
        }

        /// Deliver the terminal of a held tail call to its question: `frame`,
        /// the named call's Return re-keyed, or with no frame the bare
        /// `takeFromOtherQuestion`, as when nothing was held. The named call's
        /// terminal is already decided, so a delivery error is reported, not
        /// returned.
        ///
        /// The held redirect's own flags are not kept: the delivered frame
        /// says `noFinishNeeded = false`, so the question still sends its
        /// Finish. The C++ reference never sets that flag on a tail-call
        /// Return.
        fn deliverHeldTailCall(self: *Peer, question_id: u32, source_answer_id: u32, frame: ?[]const u8) void {
            // Settled meanwhile (transport close, the shutdown drain bound):
            // nothing is owed.
            if (!self.questions.contains(question_id)) return;
            if (frame) |bytes| {
                deliverStashedLoopbackResults(self, question_id, bytes) catch |err| reportNonfatalError(self, err);
                return;
            }
            const bare = peer_return_frames.buildReturnTakeFromOtherQuestionFrame(
                self.allocator,
                question_id,
                source_answer_id,
                false,
            ) catch |err| return reportNonfatalError(self, err);
            defer self.allocator.free(bare);
            deliverStashedLoopbackResults(self, question_id, bare) catch |err| reportNonfatalError(self, err);
        }

        /// A Return for `answer_id` went out that does not carry the call's
        /// results to this peer: `sendPrebuiltReturnFrame` (its frame goes on
        /// the wire as built), the `resultsSentElsewhere` of a call this peer
        /// forwarded back to the remote, `sendReturnTakeFromOtherQuestion`,
        /// `sendReturnAcceptFromThirdParty`. A tail call held on the call can
        /// no longer get them, and the remote's Finish of the call comes only
        /// after this question's Finish (rpc.capnp `Call.sendResultsTo`), so
        /// waiting for it would never end: give the question the bare
        /// redirect now, as before holds existed. Runs after the send, so no
        /// question callback runs inside it.
        fn releaseHeldTailCall(self: *Peer, answer_id: u32) void {
            const question_id = self.held_tail_calls.takeBySource(answer_id) orelse return;
            deliverHeldTailCall(self, question_id, answer_id, null);
        }

        /// True when a built Return-results frame carries any capability
        /// descriptors in its payload cap table.
        fn selfLoopbackResultsHaveCaps(allocator: std.mem.Allocator, frame: []const u8) !bool {
            var decoded = try protocol.DecodedMessage.init(allocator, frame);
            defer decoded.deinit();
            const built = try decoded.asReturn();
            const payload = built.results orelse return false;
            const table = payload.cap_table orelse return false;
            return table.len() > 0;
        }

        /// Deliver a self-loopback Return frame (results, or an exception) to the
        /// local question named by a `takeFromOtherQuestion` redirect. Re-enters
        /// `handleReturn` with the frame re-keyed to the target question so the
        /// normal dispatch, cap-release, and auto-finish paths run unchanged.
        pub fn deliverStashedLoopbackResults(self: *Peer, target_question_id: u32, frame: []const u8) !void {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .@"return") return error.UnexpectedMessage;
            var results_ret = try decoded.asReturn();
            results_ret.answer_id = target_question_id;
            try self.handleReturn(frame, results_ret);
        }

        /// Send a pre-built return frame, tracking outbound cap refs and recording
        /// the resolved answer for later PromisedAnswer resolution.
        pub fn sendPrebuiltReturnFrame(self: *Peer, ret: protocol.Return, frame: []const u8) !void {
            self.assertThreadAffinity();
            // Capture before delivery consumes the loopback marker. Finished-early
            // answers still reserve (see sendReturnResults): the post-send commit
            // step re-checks the tombstone map and commits-then-cleans so calls
            // pipelined on the answer replay instead of being stranded.
            const is_loopback = self.loopback_questions.contains(ret.answer_id);
            // A loopback Return's results are read back from our own side
            // (`deliverLoopbackReturn`), which takes no wire reference on our
            // exports: note none.
            var rollback_outbound_refs = !is_loopback;
            errdefer if (rollback_outbound_refs) {
                rollbackOutboundReturnCapRefs(self, ret) catch |err| {
                    log.debug("failed to roll back outbound prebuilt return refs: {}", .{err});
                };
            };
            if (!is_loopback) try noteOutboundReturnCapRefs(self, ret);
            clearSendResultsRouting(self, ret.answer_id);

            // See sendReturnResults: loopback answers may not be recorded — no
            // Finish will clear them and the id would be poisoned for reuse.
            const should_record = ret.tag == .results and !is_loopback;

            // Reserve record resources before the send so recording is infallible
            // afterward and cannot force a second (exception) Return for this
            // answer (audit item 7). A reserve failure here rolls the outbound cap
            // refs back (via rollback_outbound_refs) since nothing was sent yet.
            var reservation: ?ResolvedAnswerReservation = null;
            errdefer if (reservation) |r| r.deinit(self);
            var resolving_answer = false;
            errdefer if (resolving_answer) {
                _ = self.resolving_answers.remove(ret.answer_id);
            };
            if (should_record) {
                reservation = try self.reserveResolvedAnswer(ret.answer_id, frame);
                try self.resolving_answers.put(ret.answer_id, {});
                resolving_answer = true;
            }

            try sendReturnFrameWithLoopback(self, ret.answer_id, frame);
            rollback_outbound_refs = false;
            if (resolving_answer) {
                _ = self.resolving_answers.remove(ret.answer_id);
                resolving_answer = false;
            }

            commitOrRollbackResolvedAnswerAfterSend(self, ret.answer_id, frame, &reservation);
            releaseHeldTailCall(self, ret.answer_id);
        }

        /// Send a return with an exception for a previously received call.
        ///
        /// A failed answer carries no results, so any pipelined calls queued
        /// against it can never be satisfied. After sending the exception this
        /// drains those queued calls, failing each with its own Return so the
        /// exactly-one-Return-per-call invariant holds and the caller's question
        /// table can drain (a compliant peer otherwise hangs forever).
        pub fn sendReturnException(self: *Peer, answer_id: u32, reason: []const u8) !void {
            return sendReturnExceptionTyped(self, answer_id, reason, .failed);
        }

        /// `sendReturnException` carrying an explicit `Exception.Type`, the
        /// retryability signal a remote peer acts on.
        ///
        /// Pipelined children inherit the parent answer's type: they failed for the
        /// same reason.
        pub fn sendReturnExceptionTyped(
            self: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            self.assertThreadAffinity();
            if (self.automatic_third_party_routes.get(answer_id)) |route| {
                if (route.source_marker_failed) {
                    self.finalizeAutomaticThirdPartyRoute(route, null);
                    return error.AutomaticThirdPartySourceSettlementFailed;
                }
                return self.sendAutomaticThirdPartyException(route, reason, ex_type);
            }
            try sendReturnExceptionNoDrain(self, answer_id, reason, ex_type);
            failQueuedPromisedCalls(self, answer_id, reason, ex_type);
        }

        /// Send an exception Return without draining queued pipelined children.
        /// Used internally where the queued-call drain must not re-enter (e.g.
        /// while iterating the live `pending_promises` map).
        pub fn sendReturnExceptionNoDrain(
            self: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            // A `sendResultsTo = yourself` call is answered with
            // resultsSentElsewhere, also when it fails.
            if (self.send_results_to_yourself.contains(answer_id)) {
                return completeSelfLoopbackException(self, answer_id, reason, ex_type);
            }
            // Capture before delivery consumes the loopback marker (see
            // sendReturnResults).
            const is_loopback = self.loopback_questions.contains(answer_id);
            // Mark the answer resolving around the send. A synchronous compliant
            // receiver may Finish from inside the transport callback; without this
            // marker that Finish vanished and the post-send failed-answer record
            // poisoned the ID forever. Preserve a pre-existing marker owned by an
            // outer Return path and remove only the one installed here.
            const already_resolving = self.resolving_answers.contains(answer_id);
            if (!already_resolving) try self.resolving_answers.put(answer_id, {});
            var resolving_owned = !already_resolving;
            defer {
                if (resolving_owned) _ = self.resolving_answers.remove(answer_id);
            }

            peer_return_dispatch.sendReturnExceptionForPeer(
                Peer,
                self,
                answer_id,
                reason,
                ex_type,
                self.returnReleasesParamCaps(answer_id),
                clearSendResultsRouting,
                sendReturnFrameWithLoopback,
            ) catch |err| {
                if (resolving_owned) {
                    _ = self.resolving_answers.remove(answer_id);
                    resolving_owned = false;
                }
                // A synchronous Finish consumed the logical answer even if the
                // transport callback returned a trailing error.
                const completing_finished = if (self.completing_join_answers.get(answer_id)) |completing|
                    completing.finished
                else
                    false;
                if (self.finished_early_answers.remove(answer_id) or completing_finished) return;
                return err;
            };
            if (resolving_owned) {
                _ = self.resolving_answers.remove(answer_id);
                resolving_owned = false;
            }
            const completing_finished = if (self.completing_join_answers.get(answer_id)) |completing|
                completing.finished
            else
                false;
            const finished_early = self.finished_early_answers.remove(answer_id) or completing_finished;
            // Record AFTER a successful send, mirroring resolved_answers' gates:
            // never for loopback answers (no Finish will clear the record and the
            // id would be poisoned for legal reuse) and never for finished-early
            // answers (the Finish already arrived — same poisoned-reuse hazard,
            // and no compliant pipelined call can follow it). This is what every
            // exception Return funnels through, so calls failed by the queued
            // drain below get their own record too — a late call pipelined on a
            // FAILED pipelined call still finds the failure, transitively.
            if (!is_loopback and !finished_early) {
                recordFailedAnswer(self, answer_id, reason, ex_type);
            }
        }

        /// Best-effort record of an exception Return for `answer_id` (see the
        /// `failed_answers` field doc). Keeps the FIRST exception if somehow
        /// recorded twice; bounded like `finished_early_answers`; a skip under
        /// budget or OOM degrades to the pre-record behavior for that answer
        /// (late pipelined calls queue until their own Finish), never a crash.
        fn recordFailedAnswer(
            self: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) void {
            if (self.failed_answers.contains(answer_id)) return;
            if (self.failed_answers.count() >= self.limits.max_active_inbound_questions) return;
            const owned = self.allocator.dupe(u8, reason) catch |err| {
                return reportNonfatalError(self, err);
            };
            self.failed_answers.put(answer_id, .{ .reason = owned, .ex_type = ex_type }) catch |err| {
                self.allocator.free(owned);
                reportNonfatalError(self, err);
            };
        }

        /// Planner hook (`planPromisedTarget`): the recorded exception for an
        /// already-failed inbound answer, if any. Borrowed view — valid until the
        /// record is removed at Finish.
        pub fn lookupFailedAnswer(self: *Peer, answer_id: u32) ?peer_call_targets.FailedAnswerView {
            const failed = self.failed_answers.get(answer_id) orelse return null;
            return .{ .reason = failed.reason, .ex_type = failed.ex_type };
        }

        /// Fail and drain every pipelined call queued against `answer_id` (and,
        /// transitively, calls pipelined on those). Each queued call is sent a
        /// Return(exception) and has its inbound caps released.
        ///
        /// Drains iteratively via an explicit worklist rather than recursing, so
        /// a hostile peer cannot exhaust the stack with a deep pipelined chain.
        /// The caller must NOT be iterating the live `pending_promises` map when
        /// invoking this (it fetchRemoves buckets); callers that resolve a
        /// detached bucket, or fail a parent answer outside iteration, are safe.
        pub fn failQueuedPromisedCalls(
            self: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) void {
            var worklist = std.ArrayList(u32).empty;
            defer worklist.deinit(self.allocator);
            // Best-effort under OOM: if we cannot even seed the worklist the
            // queued children leak rather than crash; report and bail.
            worklist.append(self.allocator, answer_id) catch |err| {
                reportNonfatalError(self, err);
                return;
            };
            while (worklist.pop()) |aid| {
                var pending = self.pending_promises.fetchRemove(aid) orelse continue;
                defer pending.value.deinit(self.allocator);
                for (pending.value.items) |*pending_call| {
                    defer pending_call.caps.deinit();
                    defer self.allocator.free(pending_call.frame);

                    // question_id was decoded once at enqueue; null means the frame
                    // was not a decodable Call, so there is nothing to fail. Zero is
                    // a valid wire question id and must receive its terminal Return.
                    const child_qid = pending_call.question_id orelse continue;

                    // Non-draining send: descendants are handled by the worklist,
                    // not by re-entering this drain.
                    sendReturnExceptionNoDrain(self, child_qid, reason, ex_type) catch |err| {
                        reportNonfatalError(self, err);
                    };
                    self.releaseInboundCaps(&pending_call.caps) catch |err| {
                        reportNonfatalError(self, err);
                    };
                    worklist.append(self.allocator, child_qid) catch |err| {
                        // Cannot enqueue this child's descendants for draining;
                        // they leak under memory pressure. Report and continue.
                        reportNonfatalError(self, err);
                    };
                }
            }
        }

        pub fn reportNonfatalError(self: *Peer, err: anyerror) void {
            peer_return_dispatch.reportNonfatalErrorForPeer(Peer, self, err);
        }

        pub fn commitOrRollbackResolvedAnswerAfterSend(
            self: *Peer,
            answer_id: u32,
            frame: []const u8,
            reservation: *?ResolvedAnswerReservation,
        ) void {
            if (self.finished_early_answers.fetchRemove(answer_id)) |finished| {
                if (reservation.*) |r| {
                    self.commitReservedResolvedAnswer(answer_id, r);
                    reservation.* = null;
                    cleanupResolvedAnswerAfterEarlyFinish(self, answer_id, finished.value);
                    return;
                }
                if (finished.value) {
                    self.releaseResultCaps(frame) catch |err| reportNonfatalError(self, err);
                }
                return;
            }

            // Join completion/expiry owns an allocation-free tombstone outside
            // the ordinary active-answer budget. If the bounded
            // `finished_early_answers` map was full, Finish is still authoritative:
            // commit the reserved result only long enough to apply its cap cleanup,
            // then retire it immediately instead of poisoning the answer ID.
            if (self.completing_join_answers.get(answer_id)) |completing| {
                if (completing.finished) {
                    if (reservation.*) |r| {
                        self.commitReservedResolvedAnswer(answer_id, r);
                        reservation.* = null;
                        cleanupResolvedAnswerAfterEarlyFinish(self, answer_id, completing.release_result_caps);
                        return;
                    }
                    if (completing.release_result_caps) {
                        self.releaseResultCaps(frame) catch |err| reportNonfatalError(self, err);
                    }
                    return;
                }
            }

            if (reservation.*) |r| {
                self.commitReservedResolvedAnswer(answer_id, r);
                reservation.* = null;
            }
        }

        fn cleanupResolvedAnswerAfterEarlyFinish(self: *Peer, answer_id: u32, release_result_caps: bool) void {
            finish.handleResolvedAnswerCleanup(
                Peer,
                self,
                answer_id,
                release_result_caps,
                finish.takeResolvedAnswerFrameForPeerFn(Peer),
                Peer.releaseAnswerHeldResultCaps,
                Peer.releaseResultCaps,
                finish.freeOwnedFrameForPeerFn(Peer),
            ) catch |err| reportNonfatalError(self, err);
        }

        /// Send a return with an empty struct result (0 data words, 0 pointers).
        /// Used by streaming methods to auto-ack after the handler completes.
        pub fn sendReturnEmptyStruct(self: *Peer, answer_id: u32) !void {
            self.assertThreadAffinity();
            const BuildCtx = struct {
                fn build(_: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                    var payload = try ret.payloadTyped();
                    var any = try payload.initContent();
                    _ = try any.initStruct(0, 0);
                    _ = try ret.initCapTableTyped(0);
                }
            };
            var ctx: u8 = 0;
            try sendReturnResults(self, answer_id, &ctx, BuildCtx.build);
        }

        /// Settle an inbound call that carried `sendResultsTo = thirdParty`, after
        /// the application has delivered the results to the third vat itself.
        ///
        /// Emits `Return{resultsSentElsewhere}` — the tag the protocol mandates for
        /// a Return answering a Call whose `sendResultsTo` was not `caller`.
        /// `awaitFromThirdParty` is *not* this tag: that is what an introducer sends
        /// to the original caller on a different connection, and it is gated on that
        /// caller having set `allowThirdPartyTailCall`.
        ///
        /// Requires `setThirdPartyResultPolicy(.application)`. Errors with
        /// `error.ResultsNotRedirected` when this answer's caller did not redirect
        /// its results, so a plain caller-routed question can never be settled
        /// without them.
        ///
        /// Pipelining is not supported on a redirected answer: this vat never sees
        /// the results, so it cannot resolve a promised-answer target against them.
        /// Any calls already pipelined on this answer are failed with their own
        /// exception `Return` rather than left waiting forever.
        pub fn sendReturnResultsSentElsewhere(self: *Peer, answer_id: u32) !void {
            self.assertThreadAffinity();
            if (!self.send_results_to_third_party.contains(answer_id)) {
                return error.ResultsNotRedirected;
            }
            try sendReturnTag(self, answer_id, .resultsSentElsewhere);
            failQueuedPromisedCalls(self, answer_id, results_sent_elsewhere_no_pipelining, .failed);
        }

        /// Experimental: answer a call whose caller sent Finish first.
        ///
        /// rpc.capnp reserves `canceled` for exactly that case; the C++
        /// reference rejects it for a question still awaiting its Return, and
        /// treats a Return for a question it already retired as a protocol
        /// error. `Peer.sendReturnCanceled` therefore refuses, before anything
        /// is sent, an answer the caller has not finished and one the host
        /// does not owe a Return for. Calls pipelined on the answer can never
        /// resolve now, so each gets its own exception Return, as with
        /// `sendReturnResultsSentElsewhere`.
        pub fn sendReturnCanceled(self: *Peer, answer_id: u32) !void {
            self.assertThreadAffinity();
            if (self.active_inbound_questions.contains(answer_id)) return error.AnswerNotFinished;
            try sendReturnTag(self, answer_id, .canceled);
            failQueuedPromisedCalls(self, answer_id, answer_canceled_no_pipelining, .failed);
        }

        pub fn sendReturnTag(self: *Peer, answer_id: u32, tag: protocol.ReturnTag) !void {
            try peer_return_dispatch.sendReturnTagForPeer(
                Peer,
                self,
                answer_id,
                tag,
                self.returnReleasesParamCaps(answer_id),
                clearSendResultsRouting,
                sendReturnFrameWithLoopback,
            );
            _ = self.finished_early_answers.remove(answer_id);
            // `canceled` answers a call the remote already finished: that
            // Finish settles a hold (`settleTailCallOnFinish`). A
            // `yourself` answer took its hold before this send.
            if (tag != .canceled) releaseHeldTailCall(self, answer_id);
        }

        pub fn sendReturnTakeFromOtherQuestion(self: *Peer, answer_id: u32, other_question_id: u32) !void {
            try peer_return_dispatch.sendReturnTakeFromOtherQuestionForPeer(
                Peer,
                self,
                answer_id,
                other_question_id,
                self.returnReleasesParamCaps(answer_id),
                clearSendResultsRouting,
                sendReturnFrameWithLoopback,
            );
            _ = self.finished_early_answers.remove(answer_id);
            releaseHeldTailCall(self, answer_id);
        }

        pub fn sendReturnAcceptFromThirdParty(self: *Peer, answer_id: u32, await_payload: ?[]const u8) !void {
            try peer_return_dispatch.sendReturnAcceptFromThirdPartyForPeer(
                Peer,
                self,
                answer_id,
                await_payload,
                self.returnReleasesParamCaps(answer_id),
                clearSendResultsRouting,
                sendReturnFrameWithLoopback,
            );
            _ = self.finished_early_answers.remove(answer_id);
            releaseHeldTailCall(self, answer_id);
        }

        fn clearSendResultsRouting(self: *Peer, answer_id: u32) void {
            return_routing.clearSendResultsRoutingForPeer(
                Peer,
                self,
                answer_id,
                Peer.clearSendResultsToThirdParty,
            );
        }

        /// `sendReturnFrameWithLoopback` for a Return built with fds attached
        /// (fd passing). Only a Return that goes on the wire has any, so the
        /// loopback branch sees none.
        fn sendReturnFrameWithFds(self: *Peer, answer_id: u32, bytes: []const u8, fds: []const peer_fds.FdHandle) !void {
            if (fds.len == 0) return sendReturnFrameWithLoopback(self, answer_id, bytes);
            std.debug.assert(!self.loopback_questions.contains(answer_id));
            try peer_export_release.ExportRelease(Peer).sendFrameControlWithFds(self, bytes, fds);
            _ = self.active_inbound_questions.remove(answer_id);
            _ = self.finished_early_param_grants.remove(answer_id);
            _ = self.cross_peer_proxy_answers.remove(answer_id);
        }

        /// Send a Return, or deliver it here for a loopback answer. A
        /// loopback Return never reaches the wire, even when its delivery
        /// fails (`return_send.sendReturnFrameWithLoopbackForPeer`).
        pub fn sendReturnFrameWithLoopback(self: *Peer, answer_id: u32, bytes: []const u8) !void {
            try return_send.sendReturnFrameWithLoopbackForPeer(
                Peer,
                self,
                answer_id,
                bytes,
                Peer.deliverLoopbackReturn,
                Peer.sendFrameControl,
                loopbackQuestionOpen,
                reportNonfatalError,
            );
            _ = self.active_inbound_questions.remove(answer_id);
            _ = self.finished_early_param_grants.remove(answer_id);
            _ = self.cross_peer_proxy_answers.remove(answer_id);
        }

        /// A loopback answer's question is its own: still open means its
        /// Return has not been delivered.
        fn loopbackQuestionOpen(self: *Peer, answer_id: u32) bool {
            return self.questions.contains(answer_id);
        }

        pub fn sendReturnProvidedTarget(self: *Peer, answer_id: u32, target: *const ProvideTarget) !void {
            const BuildCtx = struct {
                peer: *Peer,
                target: *const ProvideTarget,

                fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                    const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
                    var payload = try ret.payloadTyped();
                    var any = try payload.initContent();

                    const descriptors = cap_table.descriptors;
                    // Write the provided cap with its known origin so the accepting
                    // (third) peer receives the correct descriptor variant — never a
                    // space re-derived from a colliding bare id.
                    const tagged: struct { origin_code: u4, cap_id: u32 } = switch (ctx.target.*) {
                        .local => |t| .{ .origin_code = t.origin_code, .cap_id = t.cap_id },
                        .promised => |promised| .{
                            .origin_code = descriptors.originCodeForTag(.receiverAnswer),
                            .cap_id = try ctx.peer.caps.noteReceiverAnswerOps(
                                promised.question_id,
                                promised.ops,
                            ),
                        },
                    };
                    try any.setCapabilityOriginTagged(tagged.origin_code, tagged.cap_id);
                }
            };

            var ctx = BuildCtx{
                .peer = self,
                .target = target,
            };
            try sendReturnResults(self, answer_id, &ctx, BuildCtx.build);
        }
        fn noteOutboundReturnCapRefs(self: *Peer, ret: protocol.Return) !void {
            try return_send.noteOutboundReturnCapRefsForPeer(
                Peer,
                self,
                ret,
                Peer.noteExportRef,
            );
        }

        fn rollbackOutboundReturnCapRefs(self: *Peer, ret: protocol.Return) !void {
            try return_send.rollbackOutboundReturnCapRefsForPeer(
                Peer,
                self,
                ret,
                Peer.rollbackExportRef,
            );
        }
    };
}
