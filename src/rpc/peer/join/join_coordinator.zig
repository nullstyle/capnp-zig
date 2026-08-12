const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");
const cap_table = @import("../../caps/table.zig");
const join_network = @import("../../vat/join.zig");

/// Experimental L4 Join coordinator, extracted from `peer/mod.zig` and made
/// generic over the peer type so it can live beside the other Join machinery
/// without a `mod.zig` import cycle. `peer/mod.zig` re-exports the instantiated
/// `JoinCoordinator(Peer)` under its canonical name, so every existing
/// `rpc.peer.JoinCoordinator` reference (tests, back-link records, Peer
/// register/deregister methods) resolves to the same type unchanged.
pub fn JoinCoordinator(comptime Peer: type) type {
    return struct {
        const Self = @This();
        const JoinNetwork = join_network.JoinNetwork(Peer);
        const Joined = join_network.Joined(Peer);

        pub const Accepted = struct {
            peer: *Peer,
            cap: cap_table.ResolvedCap,
        };

        allocator: std.mem.Allocator,
        origin_peer: *Peer,
        join_network: JoinNetwork,
        join_id: u32,
        expected_parts: u16,
        question_ids: std.ArrayList(u32) = .empty,
        question_peers: std.ArrayList(?*Peer) = .empty,
        question_finished: std.ArrayList(bool) = .empty,
        sent_parts: std.AutoHashMap(u16, u32),
        joined: std.ArrayList(Joined) = .empty,
        accept_question_id: ?u32 = null,
        accept_answer_peer: ?*Peer = null,
        accept_answer_id: ?u32 = null,
        accept_answer_finished: bool = true,
        accept_link_peer: ?*Peer = null,
        accept_send_in_progress: bool = false,
        accept_peer: ?*Peer = null,
        accepted_peer: ?*Peer = null,
        accepted_cap: ?cap_table.ResolvedCap = null,
        join_results_finished: bool = false,
        canceled: bool = false,
        mismatch_exceptions: u32 = 0,
        cancel_exceptions: u32 = 0,
        unexpected_exceptions: u32 = 0,
        accept_exceptions: u32 = 0,
        finish_failures: u32 = 0,

        pub fn init(
            allocator: std.mem.Allocator,
            origin_peer: *Peer,
            join_network_value: JoinNetwork,
            join_id: u32,
            expected_parts: u16,
        ) Self {
            return .{
                .allocator = allocator,
                .origin_peer = origin_peer,
                .join_network = join_network_value,
                .join_id = join_id,
                .expected_parts = expected_parts,
                .sent_parts = std.AutoHashMap(u16, u32).init(allocator),
            };
        }

        pub fn deinit(self: *@This()) void {
            self.cancelPending("join coordinator deinit") catch |err| {
                log.debug("failed to fully cancel L4 JoinCoordinator during deinit: {}", .{err});
            };
            self.clearAcceptPeerLink();
            for (self.joined.items) |*joined| joined.deinit(self.allocator);
            self.joined.deinit(self.allocator);
            self.sent_parts.deinit();
            self.clearResultPeerLinks();
            self.question_finished.deinit(self.allocator);
            self.question_peers.deinit(self.allocator);
            self.question_ids.deinit(self.allocator);
        }

        /// Send one Join part to `peer`. The `target` must name the proxied
        /// capability being joined on that peer.
        pub fn sendPart(
            self: *@This(),
            peer: *Peer,
            target: protocol.MessageTarget,
            part_count: u16,
            part_num: u16,
        ) !u32 {
            // Clock and transport seams can synchronously request result-peer
            // close/deinit. Keep its maps and the newly published backlink alive
            // until this send has either settled or rolled back atomically.
            peer.enterJoinOperation();
            defer peer.leaveJoinOperation();
            if (self.canceled) return error.JoinCanceled;
            if (self.accept_question_id != null or self.accepted_cap != null) return error.JoinAlreadyAccepting;
            if (part_count == 0 or part_num >= part_count) return error.InvalidJoinKeyPart;
            if (self.expected_parts != 0 and part_count != self.expected_parts) return error.JoinPartCountMismatch;
            if (self.sent_parts.contains(part_num)) return error.DuplicateJoinPart;
            try self.question_ids.ensureUnusedCapacity(self.allocator, 1);
            try self.question_peers.ensureUnusedCapacity(self.allocator, 1);
            try self.question_finished.ensureUnusedCapacity(self.allocator, 1);
            try self.sent_parts.ensureUnusedCapacity(1);
            try peer.join_coordinator_result_links.ensureUnusedCapacity(peer.allocator, 1);

            const key_bytes = try join_network.encodeJoinKeyPart(self.allocator, self.join_id, part_count, part_num);
            defer self.allocator.free(key_bytes);
            var key_msg = try message.Message.initUnvalidated(self.allocator, key_bytes);
            defer key_msg.deinit();

            // Publish every coordinator slot and the peer backlink before the send
            // can synchronously Return, close, or deinit the result peer. All
            // backing capacity is reserved first, making publication infallible.
            const question_id = try peer.allocateQuestion(self, Self.onJoinReturn);
            const question = peer.questions.getPtr(question_id) orelse return error.MissingAllocatedQuestion;
            question.suppress_auto_finish = true;
            self.question_ids.appendAssumeCapacity(question_id);
            self.question_peers.appendAssumeCapacity(peer);
            self.question_finished.appendAssumeCapacity(false);
            self.sent_parts.putAssumeCapacityNoClobber(part_num, question_id);
            peer.join_coordinator_result_links.appendAssumeCapacity(.{
                .coordinator = self,
                .question_id = question_id,
            });

            var builder = protocol.MessageBuilder.init(peer.allocator);
            defer builder.deinit();
            builder.buildJoin(question_id, target, try key_msg.getRootAnyPointer()) catch |err| {
                self.rollbackUnsentPart(peer, question_id, part_num);
                return err;
            };
            peer.sendBuilder(&builder) catch |err| {
                // A synchronous Return/close may consume the question before the
                // transport reports a trailing local error. In that case all
                // coordinator state already reflects the terminal; never roll it
                // back into a dangling callback.
                if (!peer.questions.contains(question_id)) {
                    log.debug("L4 Join part {} settled before trailing send error: {}", .{ question_id, err });
                    return question_id;
                }
                self.rollbackUnsentPart(peer, question_id, part_num);
                return err;
            };
            log.debug("sent experimental join question_id={}", .{question_id});
            return question_id;
        }

        fn rollbackUnsentPart(self: *@This(), peer: *Peer, question_id: u32, part_num: u16) void {
            peer.removeQuestion(question_id);
            peer.deregisterJoinCoordinatorResult(self, question_id);
            _ = self.sent_parts.remove(part_num);
            if (self.question_ids.pop()) |popped_id| {
                std.debug.assert(popped_id == question_id);
            } else {
                std.debug.assert(false);
            }
            _ = self.question_peers.pop();
            _ = self.question_finished.pop();
        }

        pub fn sendImportedCapPart(
            self: *@This(),
            peer: *Peer,
            target_import_id: u32,
            part_count: u16,
            part_num: u16,
        ) !u32 {
            return self.sendPart(
                peer,
                .{ .tag = .importedCap, .imported_cap = target_import_id, .promised_answer = null },
                part_count,
                part_num,
            );
        }

        /// Send the direct `Accept` once all expected JoinResults have arrived.
        /// The accepted capability is retained and can be read with `acceptedCap()`
        /// or transferred out with `takeAccepted()`.
        pub fn acceptFirst(self: *@This()) !u32 {
            if (self.accept_question_id) |question_id| return question_id;
            if (self.expected_parts == 0) return error.InvalidJoinKeyPart;
            if (self.mismatch_exceptions != 0 or
                self.cancel_exceptions != 0 or
                self.unexpected_exceptions != 0 or
                self.accept_exceptions != 0)
            {
                return error.JoinDidNotSucceed;
            }
            if (self.joined.items.len != self.expected_parts) return error.MissingJoinResults;

            const first = &self.joined.items[0];
            for (self.joined.items[1..]) |*joined| {
                if (joined.peer != first.peer or !std.mem.eql(u8, joined.provision, first.provision)) {
                    self.mismatch_exceptions += 1;
                    self.canceled = true;
                    self.finishJoinResults() catch |err| {
                        self.finish_failures += 1;
                        log.debug("failed to finish L4 JoinResult questions after mismatch: {}", .{err});
                    };
                    self.clearJoinInputs();
                    return error.JoinResultMismatch;
                }
            }

            var provision_msg = try message.Message.initUnvalidated(self.allocator, first.provision);
            defer provision_msg.deinit();
            const provision = try provision_msg.getRootAnyPointer();
            const direct_peer = first.peer;
            try self.registerAcceptPeerLink(direct_peer);
            self.accept_send_in_progress = true;
            defer self.accept_send_in_progress = false;
            const question_id = direct_peer.sendAcceptNoRestore(provision, null, self, Self.onAcceptReturn, true) catch |err| {
                if (self.accepted_cap != null or self.accept_exceptions != 0) {
                    log.debug("L4 JoinCoordinator Accept settled before send returned trailing error: {}", .{err});
                    const answer_id = self.accept_answer_id orelse return err;
                    _ = self.finishAcceptAnswer(direct_peer, answer_id, "trailing Accept send error");
                    return answer_id;
                }
                self.clearAcceptPeerLink();
                return err;
            };
            self.noteAcceptAnswerNeedsFinish(direct_peer, question_id);
            if (self.accepted_cap != null or self.accept_exceptions != 0) {
                _ = self.finishAcceptAnswer(direct_peer, question_id, "synchronous Accept");
            }
            if (self.accepted_cap == null and self.accept_exceptions == 0) {
                self.accept_question_id = question_id;
                self.accept_peer = direct_peer;
            }
            return question_id;
        }

        pub fn acceptedCap(self: *const @This()) ?cap_table.ResolvedCap {
            return self.accepted_cap;
        }

        pub fn acceptedPeer(self: *const @This()) ?*Peer {
            return self.accepted_peer;
        }

        /// Transfer ownership of the retained accepted cap to the caller. The
        /// caller must later release the returned cap on the returned peer.
        pub fn takeAccepted(self: *@This()) ?Accepted {
            self.retryAcceptAnswerFinish("takeAccepted") catch |err| {
                log.debug("failed to retry L4 JoinCoordinator Accept Finish before takeAccepted: {}", .{err});
            };
            const cap = self.accepted_cap orelse return null;
            const peer = self.accepted_peer orelse return null;
            self.accepted_cap = null;
            self.accepted_peer = null;
            self.clearAcceptPeerLinkIfDrained();
            return .{ .peer = peer, .cap = cap };
        }

        pub fn releaseAccepted(self: *@This()) !void {
            var first_err: ?anyerror = null;
            self.retryAcceptAnswerFinish("releaseAccepted") catch |err| {
                if (first_err == null) first_err = err;
            };

            const cap = self.accepted_cap orelse {
                if (first_err) |err| return err;
                return;
            };
            const peer = self.accepted_peer orelse {
                if (first_err) |err| return err;
                return error.MissingAcceptedPeer;
            };
            self.accepted_cap = null;
            self.accepted_peer = null;
            peer.releaseResolvedCap(cap) catch |err| {
                if (first_err == null) first_err = err;
            };
            self.clearAcceptPeerLinkIfDrained();
            if (first_err) |err| return err;
        }

        fn clearJoinedResults(self: *@This()) void {
            for (self.joined.items) |*joined| joined.deinit(self.allocator);
            self.joined.clearRetainingCapacity();
        }

        fn clearJoinInputs(self: *@This()) void {
            self.clearJoinedResults();
            self.sent_parts.clearRetainingCapacity();
        }

        /// Finish every JoinResult question without releasing result caps. This
        /// releases the host-side JoinResult lifetime after direct Accept succeeds.
        pub fn finishJoinResults(self: *@This()) !void {
            if (self.join_results_finished) return;
            var first_err: ?anyerror = null;
            for (self.question_ids.items, self.question_peers.items, self.question_finished.items) |question_id, *peer_slot, *finished| {
                if (finished.*) continue;
                const peer = peer_slot.* orelse {
                    finished.* = true;
                    continue;
                };
                peer.enterJoinOperation();
                defer peer.leaveJoinOperation();
                peer.sendFinishForHost(question_id, false, false) catch |err| {
                    if (first_err == null) first_err = err;
                    continue;
                };
                finished.* = true;
                peer.deregisterJoinCoordinatorResult(self, question_id);
                peer_slot.* = null;
            }
            if (first_err) |err| return err;
            self.join_results_finished = true;
        }

        fn noteAllJoinResultsFinished(self: *@This()) void {
            for (self.question_finished.items) |finished| {
                if (!finished) return;
            }
            self.join_results_finished = true;
        }

        fn cancelQuestionIndex(self: *@This(), index: usize, reason: []const u8) !void {
            if (self.question_finished.items[index]) return;
            const question_id = self.question_ids.items[index];
            const peer = self.question_peers.items[index] orelse {
                self.question_finished.items[index] = true;
                return;
            };
            peer.enterJoinOperation();
            defer peer.leaveJoinOperation();
            if (peer.questions.contains(question_id)) {
                peer.cancelQuestion(question_id, reason) catch |err| {
                    self.question_finished.items[index] = true;
                    return err;
                };
                self.question_finished.items[index] = true;
                peer.deregisterJoinCoordinatorResult(self, question_id);
                self.question_peers.items[index] = null;
            } else if (!self.join_results_finished) {
                try peer.sendFinishForHost(question_id, false, false);
                self.question_finished.items[index] = true;
                peer.deregisterJoinCoordinatorResult(self, question_id);
                self.question_peers.items[index] = null;
            }
        }

        pub fn cancelPending(self: *@This(), reason: []const u8) !void {
            self.canceled = true;
            var first_err: ?anyerror = null;
            for (0..self.question_ids.items.len) |index| {
                self.cancelQuestionIndex(index, reason) catch |err| {
                    if (first_err == null) first_err = err;
                };
            }
            self.noteAllJoinResultsFinished();

            if (self.accept_question_id) |accept_question_id| {
                if (self.accept_peer) |peer| {
                    if (peer.questions.contains(accept_question_id)) {
                        peer.cancelQuestion(accept_question_id, reason) catch |err| {
                            if (first_err == null) first_err = err;
                        };
                    }
                }
            }

            self.releaseAccepted() catch |err| {
                if (first_err == null) first_err = err;
            };
            self.clearJoinInputs();
            self.clearAcceptPeerLinkIfDrained();

            if (first_err) |err| return err;
        }

        fn finishOneBestEffort(self: *@This(), peer: *Peer, question_id: u32) void {
            peer.enterJoinOperation();
            defer peer.leaveJoinOperation();
            peer.sendFinishForHost(question_id, false, false) catch |err| {
                self.finish_failures += 1;
                log.debug("failed to finish L4 JoinResult question {}: {}", .{ question_id, err });
                return;
            };
            self.markQuestionFinished(peer, question_id);
            self.noteAllJoinResultsFinished();
        }

        fn noteSyntheticCancelReturn(self: *@This(), peer: *Peer, question_id: u32) bool {
            if (!self.canceled) return false;
            const question = peer.questions.get(question_id) orelse return false;
            if (!question.cancelled) return false;
            self.markQuestionFinished(peer, question_id);
            self.noteAllJoinResultsFinished();
            return true;
        }

        fn failTerminalJoinResult(self: *@This(), peer: *Peer, question_id: u32, reason: []const u8) void {
            if (self.noteSyntheticCancelReturn(peer, question_id)) return;
            self.finishOneBestEffort(peer, question_id);
            self.cancelPending(reason) catch |err| {
                self.finish_failures += 1;
                log.debug("failed to cancel L4 JoinCoordinator after terminal JoinResult {}: {}", .{ question_id, err });
            };
        }

        fn finishJoinResultsAfterAccept(self: *@This(), context: []const u8) void {
            self.finishJoinResults() catch |err| {
                self.finish_failures += 1;
                log.debug("failed to finish L4 JoinResult questions after {s}: {}", .{ context, err });
            };
            self.clearJoinInputs();
        }

        fn failTerminalAcceptReturn(self: *@This(), context: []const u8) void {
            self.accept_exceptions += 1;
            self.canceled = true;
            self.accept_question_id = null;
            self.accept_peer = null;
            self.finishJoinResultsAfterAccept(context);
            self.clearAcceptPeerLinkIfDrained();
        }

        fn registerAcceptPeerLink(self: *@This(), peer: *Peer) !void {
            if (self.accept_link_peer) |linked_peer| {
                if (linked_peer == peer) return;
                return error.ConflictingAcceptPeer;
            }
            try peer.registerJoinCoordinatorAccept(self);
            self.accept_link_peer = peer;
        }

        fn clearAcceptPeerLink(self: *@This()) void {
            if (self.accept_link_peer) |peer| {
                peer.deregisterJoinCoordinatorAccept(self);
                self.accept_link_peer = null;
            }
        }

        fn clearAcceptPeerLinkIfDrained(self: *@This()) void {
            if (self.accept_question_id != null) return;
            if (self.accept_peer != null) return;
            if (self.accepted_cap != null or self.accepted_peer != null) return;
            if (!self.accept_answer_finished) return;
            self.clearAcceptPeerLink();
        }

        pub fn neutralizeAcceptedPeer(self: *@This(), peer: *Peer) void {
            if (self.accept_answer_peer == peer and !self.accept_answer_finished) {
                if (self.accept_answer_id) |answer_id| {
                    peer.sendFinishForHost(answer_id, false, false) catch |err| {
                        self.finish_failures += 1;
                        log.debug("failed to finish L4 JoinCoordinator Accept answer {} during peer deinit: {}", .{
                            answer_id,
                            err,
                        });
                    };
                }
            }
            if (self.accept_link_peer == peer) self.accept_link_peer = null;
            if (self.accept_peer == peer) {
                self.accept_peer = null;
                self.accept_question_id = null;
            }
            if (self.accept_answer_peer == peer) {
                self.accept_answer_peer = null;
                self.accept_answer_finished = true;
            }
            if (self.accepted_peer == peer) {
                self.accepted_peer = null;
                self.accepted_cap = null;
            }
        }

        fn noteAcceptAnswerNeedsFinish(self: *@This(), peer: *Peer, answer_id: u32) void {
            if (self.accept_answer_id == answer_id and self.accept_answer_finished) return;
            self.accept_answer_peer = peer;
            self.accept_answer_id = answer_id;
            self.accept_answer_finished = false;
        }

        fn noteAcceptAnswerFinished(self: *@This(), peer: *Peer, answer_id: u32) void {
            self.accept_answer_peer = peer;
            self.accept_answer_id = answer_id;
            self.accept_answer_finished = true;
        }

        fn finishAcceptAnswer(self: *@This(), peer: *Peer, answer_id: u32, context: []const u8) bool {
            if (self.accept_answer_id == answer_id and self.accept_answer_finished) return true;
            var attempts: u8 = 0;
            while (attempts < 2) : (attempts += 1) {
                peer.sendFinishForHost(answer_id, false, false) catch |err| {
                    self.finish_failures += 1;
                    log.debug("failed to finish L4 JoinCoordinator Accept answer {} after {s}: {}", .{
                        answer_id,
                        context,
                        err,
                    });
                    continue;
                };
                self.noteAcceptAnswerFinished(peer, answer_id);
                self.clearAcceptPeerLinkIfDrained();
                return true;
            }
            return false;
        }

        fn retryAcceptAnswerFinish(self: *@This(), context: []const u8) !void {
            const answer_id = self.accept_answer_id orelse return;
            if (self.accept_answer_finished) return;
            const peer = self.accept_answer_peer orelse return error.MissingAcceptPeer;
            if (!self.finishAcceptAnswer(peer, answer_id, context)) return error.AcceptFinishFailed;
        }

        fn markQuestionFinished(self: *@This(), peer: *Peer, question_id: u32) void {
            for (self.question_ids.items, self.question_peers.items, self.question_finished.items) |qid, *qpeer, *finished| {
                if (qid == question_id and qpeer.* == peer) {
                    finished.* = true;
                    peer.deregisterJoinCoordinatorResult(self, question_id);
                    qpeer.* = null;
                    return;
                }
            }
        }

        pub fn neutralizeResultPeer(self: *@This(), peer: *Peer, question_id: u32) void {
            for (self.question_ids.items, self.question_peers.items, self.question_finished.items) |qid, *qpeer, *finished| {
                if (qid != question_id or qpeer.* != peer) continue;
                qpeer.* = null;
                finished.* = true;
                self.noteAllJoinResultsFinished();
                return;
            }
        }

        fn clearResultPeerLinks(self: *@This()) void {
            for (self.question_ids.items, self.question_peers.items) |question_id, peer_opt| {
                if (peer_opt) |peer| peer.deregisterJoinCoordinatorResult(self, question_id);
            }
        }

        fn onJoinReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            _: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            switch (ret.tag) {
                .results => {
                    const payload = ret.results orelse {
                        self.unexpected_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    };
                    if (self.expected_parts != 0 and self.joined.items.len >= self.expected_parts) {
                        self.unexpected_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    }
                    const decoded = join_network.decodeJoinResult(payload.content) catch {
                        self.unexpected_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    };
                    if (!decoded.succeeded or decoded.join_id != self.join_id) {
                        self.mismatch_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    }
                    var joined = self.join_network.connectJoined(self.allocator, payload.content) catch {
                        self.unexpected_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    };
                    self.joined.append(self.allocator, joined) catch {
                        joined.deinit(self.allocator);
                        self.unexpected_exceptions += 1;
                        self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                        return;
                    };
                },
                .exception => {
                    const reason = if (ret.exception) |exception| exception.reason else "";
                    if (std.mem.eql(u8, reason, "join target mismatch")) {
                        self.mismatch_exceptions += 1;
                    } else if (std.mem.eql(u8, reason, "join canceled")) {
                        self.cancel_exceptions += 1;
                    } else {
                        self.unexpected_exceptions += 1;
                    }
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                },
                else => {
                    self.unexpected_exceptions += 1;
                    self.failTerminalJoinResult(peer, ret.answer_id, "join canceled");
                },
            }
        }

        fn onAcceptReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx_ptr));
            if (ret.no_finish_needed) {
                self.noteAcceptAnswerFinished(peer, ret.answer_id);
            } else {
                self.noteAcceptAnswerNeedsFinish(peer, ret.answer_id);
            }
            defer if (!self.accept_send_in_progress and !ret.no_finish_needed) {
                _ = self.finishAcceptAnswer(peer, ret.answer_id, "Accept Return");
            };
            switch (ret.tag) {
                .results => {
                    if (self.accepted_cap != null) {
                        self.failTerminalAcceptReturn("duplicate Accept result");
                        return;
                    }
                    const payload = ret.results orelse {
                        self.failTerminalAcceptReturn("malformed Accept result");
                        return;
                    };
                    const cap = payload.content.getCapability() catch {
                        self.failTerminalAcceptReturn("malformed Accept result");
                        return;
                    };
                    const resolved = caps.resolveCapability(cap) catch {
                        self.failTerminalAcceptReturn("unresolved Accept result");
                        return;
                    };
                    var mutable_caps: *cap_table.InboundCapTable = @constCast(caps);
                    mutable_caps.retainCapability(cap) catch {
                        self.failTerminalAcceptReturn("unretained Accept result");
                        return;
                    };
                    self.accepted_peer = peer;
                    self.accepted_cap = resolved;
                    self.accept_question_id = null;
                    self.accept_peer = null;
                    self.finishJoinResultsAfterAccept("Accept");
                    self.clearAcceptPeerLinkIfDrained();
                },
                .exception => {
                    self.failTerminalAcceptReturn("Accept exception");
                },
                else => {
                    self.failTerminalAcceptReturn("unexpected Accept return");
                },
            }
        }
    };
}
