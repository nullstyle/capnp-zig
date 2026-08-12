const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const events = @import("../../events.zig");

/// Experimental L4 cross-peer Join relay, extracted from `peer/mod.zig` and
/// made generic over the peer type (the JoinCoordinator extraction contract):
/// proxy-target Join detection, relay record lifecycle
/// (put/clear/detach/retire), the forwarded cross-peer Join send, and its
/// Return callback. `peer/mod.zig` keeps every caller-visible name as a thunk
/// on `Peer`.
pub fn JoinRelay(comptime Peer: type) type {
    return struct {
        const ProvideTarget = state.ProvideTarget;
        const JoinState = state.JoinState;
        const JoinOperationGuards = Peer.JoinOperationGuards;
        const ensureCountLimit = Peer.ensureCountLimit;
        const ensureByteLimit = Peer.ensureByteLimit;
        const saturatingAdd = Peer.saturatingAdd;
        const joinWireReason = Peer.joinWireReason;
        const provideTargetsEqual = Peer.provideTargetsEqual;
        const relayReturnAcrossPeers = Peer.relayReturnAcrossPeers;

        const HostedJoin = Peer.HostedJoinRecord;
        const CompletingJoinAnswer = Peer.CompletingJoinAnswerRecord;
        const CrossPeerJoinRelay = Peer.CrossPeerJoinRelayRecord;
        const CrossPeerProxyContext = Peer.CrossPeerProxyCtx;
        const CrossPeerJoinRelayContext = Peer.CrossPeerJoinRelayCtx;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        pub fn crossPeerProxyContextForExport(self: *Peer, export_id: u32) ?*CrossPeerProxyContext {
            const entry = self.exports.getPtr(export_id) orelse return null;
            const handler = entry.handler orelse return null;
            if (handler.on_call != CrossPeerProxyContext.onCall) return null;
            return localCastCtx(*CrossPeerProxyContext, handler.ctx);
        }

        pub fn crossPeerJoinTargetForResolved(target: cap_table.ResolvedCap) !protocol.MessageTarget {
            return switch (target) {
                .imported => |cap| .{
                    .tag = .importedCap,
                    .imported_cap = cap.id,
                    .promised_answer = null,
                },
                .exported, .promised, .none => error.UnsupportedCrossPeerJoinTarget,
            };
        }

        pub fn putPendingJoinRelay(
            self: *Peer,
            owner_answer_id: u32,
            source_peer: *Peer,
            source_question_id: u32,
        ) !void {
            // Do not call an application clock while a map entry or reciprocal
            // backlink is half-published.
            const deadline_ns = try self.newJoinDeadline();
            self.ensureJoinRecordCapacity(1) catch {
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .join_records,
                    saturatingAdd(self.joinRecordCount(), 1),
                    self.limits.max_pending_join_records,
                    error.PeerLimitExceeded,
                );
                return error.PeerLimitExceeded;
            };
            try ensureCountLimit(
                self.pending_join_relays.contains(owner_answer_id),
                self.pending_join_relays.count(),
                self.limits.max_pending_join_questions,
            );
            const settlement_answers = std.math.add(
                usize,
                self.completing_join_answers.count(),
                self.pending_join_questions.count(),
            ) catch return error.PeerLimitExceeded;
            const with_relays = std.math.add(
                usize,
                settlement_answers,
                self.pending_join_relays.count(),
            ) catch return error.PeerLimitExceeded;
            const settlement_capacity_usize = std.math.add(usize, with_relays, 1) catch
                return error.PeerLimitExceeded;
            const settlement_capacity = std.math.cast(u32, settlement_capacity_usize) orelse
                return error.PeerLimitExceeded;
            try self.completing_join_answers.ensureTotalCapacity(settlement_capacity);
            const entry = try self.pending_join_relays.getOrPut(owner_answer_id);
            if (entry.found_existing) return error.DuplicateJoinQuestionId;
            entry.value_ptr.* = .{
                .source_peer = source_peer,
                .source_question_id = source_question_id,
                .deadline_ns = deadline_ns,
            };
            errdefer _ = self.pending_join_relays.remove(owner_answer_id);
            try source_peer.registerCrossPeerJoinRelay(self, owner_answer_id);
            self.noteJoinDeadline(entry.value_ptr.deadline_ns);
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .join_records,
                self.joinRecordCount() - 1,
                self.joinRecordCount(),
                self.limits.max_pending_join_records,
            );
        }

        pub fn clearPendingJoinRelay(
            self: *Peer,
            owner_answer_id: u32,
            send_downstream_finish: bool,
            release_result_caps: bool,
        ) !void {
            const pending = self.pending_join_relays.get(owner_answer_id) orelse return;
            const source_peer = pending.source_peer;
            var guards = JoinOperationGuards{};
            guards.add(self);
            if (source_peer) |peer| guards.add(peer);
            guards.enter();
            defer guards.leave();

            const removed = detachPendingJoinRelay(self, owner_answer_id) orelse return;
            if (source_peer) |peer| {
                if (send_downstream_finish) {
                    peer.sendJoinRelayFinishAndNeutralize(
                        removed.source_question_id,
                        release_result_caps,
                    ) catch |err| {
                        // Restore both halves only after the failed callback has
                        // returned. During the send, observers see the relay and
                        // reciprocal backlink wholly detached.
                        if (!self.pending_join_relays.contains(owner_answer_id)) {
                            self.pending_join_relays.putAssumeCapacity(owner_answer_id, removed);
                            peer.registerCrossPeerJoinRelay(self, owner_answer_id) catch |restore_err| {
                                _ = self.pending_join_relays.remove(owner_answer_id);
                                peer.neutralizeJoinRelayQuestion(removed.source_question_id);
                                self.refreshNextJoinDeadline();
                                return restore_err;
                            };
                            self.noteJoinDeadline(removed.deadline_ns);
                        }
                        return err;
                    };
                }
            }
        }

        /// Remove both halves of a relay without invoking sends or callbacks.
        /// Callers guard `self` and the optional source peer before entering.
        pub fn detachPendingJoinRelay(self: *Peer, owner_answer_id: u32) ?CrossPeerJoinRelay {
            const removed = self.pending_join_relays.fetchRemove(owner_answer_id) orelse return null;
            if (removed.value.source_peer) |source_peer| {
                source_peer.deregisterCrossPeerJoinRelay(self, owner_answer_id);
            }
            self.refreshNextJoinDeadline();
            return removed.value;
        }

        /// Terminal relay retirement for timeout/transport close. Unlike explicit
        /// Finish retry, a failed best-effort downstream Finish cannot resurrect an
        /// expired or disconnected record.
        pub fn retirePendingJoinRelayTerminal(
            self: *Peer,
            owner_answer_id: u32,
            emit_timeout: bool,
            send_upstream_exception: bool,
        ) bool {
            const pending = self.pending_join_relays.get(owner_answer_id) orelse return false;
            const source_peer = pending.source_peer;
            var guards = JoinOperationGuards{};
            guards.add(self);
            if (source_peer) |peer| guards.add(peer);
            guards.enter();
            defer guards.leave();

            const detached = detachPendingJoinRelay(self, owner_answer_id) orelse return false;
            self.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
            defer {
                _ = self.removeCompletingJoinAnswer(owner_answer_id);
                _ = self.finished_early_answers.remove(owner_answer_id);
            }
            // From this point onward every local record and reciprocal backlink is
            // gone. Observer, wire-send, and deinit callbacks may safely re-enter.
            if (emit_timeout) events.emitJoinTimeout(self.observer, owner_answer_id);
            if (source_peer) |peer| {
                peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |err| {
                    log.debug("terminal Join relay Finish failed for question {}: {}", .{ detached.source_question_id, err });
                    peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                };
            }
            if (send_upstream_exception and !detached.upstream_terminal_started) {
                if (self.completing_join_answers.getPtr(owner_answer_id)) |completing| {
                    // Relay teardown state is already fully detached. Preserve only
                    // the uncounted answer-ID tombstone across the terminal send.
                    self.retireCompletingJoinAnswerAccounting(completing);
                }
                self.sendReturnException(owner_answer_id, "join unavailable") catch |err| {
                    log.debug("expired Join relay exception send failed for answer {}: {}", .{ owner_answer_id, err });
                };
            }
            return true;
        }

        pub fn tryHandleCrossPeerProxyJoin(self: *Peer, join: protocol.Join) !bool {
            if (join.target.tag != .importedCap) return false;
            const export_id = join.target.imported_cap orelse return false;
            const proxy_ctx = crossPeerProxyContextForExport(self, export_id) orelse return false;
            const source_peer = proxy_ctx.source_peer orelse {
                try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
                return true;
            };
            if (source_peer.is_shutting_down) {
                try self.sendReturnException(join.question_id, "cross-peer proxy source disconnected");
                return true;
            }
            try forwardCrossPeerProxyJoin(self, join, source_peer, proxy_ctx.target);
            return true;
        }

        pub fn forwardCrossPeerProxyJoin(
            self: *Peer,
            join: protocol.Join,
            source_peer: *Peer,
            source_target: cap_table.ResolvedCap,
        ) !void {
            var guards = JoinOperationGuards{};
            guards.add(self);
            guards.add(source_peer);
            guards.enter();
            defer guards.leave();

            const downstream_target = crossPeerJoinTargetForResolved(source_target) catch |err| {
                try self.sendReturnException(join.question_id, @errorName(err));
                return;
            };

            const relay = try source_peer.allocator.create(CrossPeerJoinRelayContext);
            relay.* = .{
                .owner_peer = self,
                .owner_answer_id = join.question_id,
            };
            var relay_owned = true;
            errdefer if (relay_owned) source_peer.allocator.destroy(relay);

            const source_question_id = source_peer.allocateQuestionNoRestore(relay, onCrossPeerJoinReturn) catch |err| {
                try self.sendReturnException(join.question_id, joinWireReason(err));
                return;
            };
            var question_owned = true;
            errdefer if (question_owned) source_peer.removeQuestionAndDeinit(source_question_id);

            const source_question = source_peer.questions.getPtr(source_question_id) orelse return error.MissingAllocatedQuestion;
            source_question.suppress_auto_finish = true;
            // The owner relay's Join-domain deadline is authoritative. A generic
            // outbound-call deadline here would race it in the source peer's clock
            // domain and leak "deadline exceeded" instead of the redacted Join
            // terminal.
            source_question.deadline_ns = null;
            source_question.deinit_ctx = CrossPeerJoinRelayContext.deinit;
            relay_owned = false;

            putPendingJoinRelay(self, join.question_id, source_peer, source_question_id) catch |err| {
                question_owned = false;
                source_peer.removeQuestionAndDeinit(source_question_id);
                const reason = joinWireReason(err);
                try self.sendReturnException(join.question_id, reason);
                return;
            };
            var relay_registered = true;
            errdefer if (relay_registered) {
                clearPendingJoinRelay(self, join.question_id, false, false) catch |err| {
                    log.debug("cross-peer join relay: failed to roll back relay {}: {}", .{ join.question_id, err });
                };
            };

            var relay_settled = false;
            relay.settled_flag = &relay_settled;

            var builder = protocol.MessageBuilder.init(source_peer.allocator);
            defer builder.deinit();
            try builder.buildJoin(source_question_id, downstream_target, join.key_part);
            source_peer.sendBuilder(&builder) catch |err| {
                if (relay_settled) {
                    question_owned = false;
                    relay_registered = false;
                    return;
                }
                relay_registered = false;
                clearPendingJoinRelay(self, join.question_id, false, false) catch |clear_err| {
                    log.debug("cross-peer join relay: failed to clear relay {} after send failure: {}", .{
                        join.question_id,
                        clear_err,
                    });
                };
                question_owned = false;
                source_peer.removeQuestionAndDeinit(source_question_id);
                try self.sendReturnException(join.question_id, joinWireReason(err));
                return;
            };

            if (relay_settled) {
                question_owned = false;
                relay_registered = false;
                return;
            }
            relay.settled_flag = null;
            question_owned = false;
            relay_registered = false;
        }

        pub fn onCrossPeerJoinReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const ctx: *CrossPeerJoinRelayContext = localCastCtx(*CrossPeerJoinRelayContext, ctx_ptr);
            const owner_peer = ctx.owner_peer;
            const owner_answer_id = ctx.owner_answer_id;
            if (ctx.settled_flag) |flag| flag.* = true;
            var guards = JoinOperationGuards{};
            guards.add(owner_peer);
            guards.add(peer);
            guards.enter();
            defer guards.leave();
            // Registered after guards.leave so LIFO destroys the source-owned ctx
            // while both peers are still protected from callback-triggered deinit.
            defer CrossPeerJoinRelayContext.deinit(peer.allocator, ctx);

            if (!owner_peer.pending_join_relays.contains(owner_answer_id)) return;

            switch (ret.tag) {
                .results => {
                    const relay = owner_peer.pending_join_relays.getPtr(owner_answer_id) orelse return;
                    relay.upstream_terminal_started = true;
                    relayReturnAcrossPeers(owner_peer, owner_answer_id, peer, ret, inbound_caps, true) catch |err| {
                        const detached = detachPendingJoinRelay(owner_peer, owner_answer_id) orelse return;
                        owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                        defer {
                            _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                            _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                        }
                        peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                            log.debug("cross-peer join relay: failed to finish downstream question after relay error: {}", .{clear_err});
                            peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                        };
                        owner_peer.sendReturnException(owner_answer_id, joinWireReason(err)) catch |send_err| {
                            log.debug("cross-peer join relay: failed to fail upstream question {}: {}", .{
                                owner_answer_id,
                                send_err,
                            });
                        };
                    };
                },
                .exception => {
                    const detached = detachPendingJoinRelay(owner_peer, owner_answer_id) orelse return;
                    owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                    defer {
                        _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                        _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                    }
                    const reason = if (ret.exception) |exception| exception.reason else "cross-peer join failed";
                    owner_peer.sendReturnException(owner_answer_id, reason) catch |send_err| {
                        log.debug("cross-peer join relay: failed to relay exception for question {}: {}", .{
                            owner_answer_id,
                            send_err,
                        });
                    };
                    peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                        log.debug("cross-peer join relay: failed to finish exception result {}: {}", .{ owner_answer_id, clear_err });
                        peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                    };
                },
                else => {
                    const detached = detachPendingJoinRelay(owner_peer, owner_answer_id) orelse return;
                    owner_peer.putCompletingJoinAnswerAssumeCapacity(owner_answer_id, false);
                    defer {
                        _ = owner_peer.removeCompletingJoinAnswer(owner_answer_id);
                        _ = owner_peer.finished_early_answers.remove(owner_answer_id);
                    }
                    owner_peer.sendReturnException(owner_answer_id, "cross-peer join relay: unexpected return") catch |send_err| {
                        log.debug("cross-peer join relay: failed to fail unexpected return for question {}: {}", .{
                            owner_answer_id,
                            send_err,
                        });
                    };
                    peer.sendJoinRelayFinishAndNeutralize(detached.source_question_id, false) catch |clear_err| {
                        log.debug("cross-peer join relay: failed to finish unexpected result {}: {}", .{ owner_answer_id, clear_err });
                        peer.neutralizeJoinRelayQuestion(detached.source_question_id);
                    };
                },
            }
        }
    };
}
