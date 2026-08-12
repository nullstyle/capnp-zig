const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const events = @import("../../events.zig");
const message = @import("../../../serialization/message.zig");

/// Experimental L4 hosted-Join accept/completion lifecycle, extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract): pending JoinResult accept ownership, completing-join
/// answer bookkeeping (tombstones outside the ordinary answer budget), legacy
/// join completion, and the canonical HostedJoin ownership/cancel/destroy
/// choreography. `peer/mod.zig` keeps every caller-visible name as a thunk on
/// `Peer`.
pub fn JoinAccept(comptime Peer: type) type {
    return struct {
        const ProvideTarget = state.ProvideTarget;
        const JoinState = state.JoinState;
        const JoinOperationGuards = Peer.JoinOperationGuards;
        const ensureCountLimit = Peer.ensureCountLimit;
        const ensureByteLimit = Peer.ensureByteLimit;
        const saturatingAdd = Peer.saturatingAdd;
        const joinWireReason = Peer.joinWireReason;
        const provideTargetsEqual = Peer.provideTargetsEqual;

        const HostedJoin = Peer.HostedJoinRecord;
        const CompletingJoinAnswer = Peer.CompletingJoinAnswerRecord;
        const CrossPeerJoinRelay = Peer.CrossPeerJoinRelayRecord;
        const CrossPeerProxyContext = Peer.CrossPeerProxyCtx;
        const CrossPeerJoinRelayContext = Peer.CrossPeerJoinRelayCtx;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        pub fn putPendingJoinAcceptOwned(self: *Peer, hosted: *HostedJoin, target: ProvideTarget) !void {
            // Sample application time before publishing either side of the
            // cross-peer lease. The caller holds Join operation guards for both
            // owner and Accept peers, so close/deinit requested by Clock.now is
            // deferred until this admission finishes.
            const deadline_ns = try self.newJoinDeadline();
            try ensureCountLimit(
                self.pending_join_accepts.contains(hosted.provision),
                self.pending_join_accepts.count(),
                self.limits.max_pending_join_accepts,
            );
            try self.ensureJoinRecordCapacity(1);
            try self.registerJoinAcceptHost(hosted);
            errdefer self.deregisterJoinAcceptHost(hosted);

            const entry = try self.pending_join_accepts.getOrPut(hosted.provision);
            if (entry.found_existing) return error.DuplicateJoinProvision;
            entry.value_ptr.* = .{ .hosted = hosted, .target = target };
            hosted.accept_peer = self;
            hosted.accept_live = true;
            hosted.deadline_ns = deadline_ns;
            self.noteJoinDeadline(hosted.deadline_ns);
        }

        pub fn takePendingJoinAccept(self: *Peer, provision: []const u8) ?ProvideTarget {
            if (self.pending_join_accepts.fetchRemove(provision)) |removed| {
                const hosted = removed.value.hosted;
                const target = removed.value.target;
                self.deregisterJoinAcceptHost(hosted);
                hosted.accept_live = false;
                hosted.accept_peer = null;
                hosted.deadline_ns = null;
                self.refreshNextJoinDeadline();
                retireHostedJoinNetwork(hosted.owner_peer, hosted);
                return target;
            }
            return null;
        }

        pub fn rememberPendingJoinResultAnswer(self: *Peer, answer_id: u32, hosted: *HostedJoin) !void {
            try ensureCountLimit(
                self.pending_join_result_answers.contains(answer_id),
                self.pending_join_result_answers.count(),
                self.limits.max_pending_join_questions,
            );
            try self.ensureJoinRecordCapacity(1);
            const entry = try self.pending_join_result_answers.getOrPut(answer_id);
            if (entry.found_existing) return error.DuplicateJoinQuestionId;
            entry.value_ptr.* = .{ .hosted = hosted };
            hosted.result_refs = std.math.add(usize, hosted.result_refs, 1) catch {
                _ = self.pending_join_result_answers.remove(answer_id);
                return error.PeerLimitExceeded;
            };
        }

        pub fn clearPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
            const removed = self.pending_join_result_answers.fetchRemove(answer_id) orelse return;
            const hosted = removed.value.hosted;
            std.debug.assert(hosted.result_refs > 0);
            hosted.result_refs -= 1;
            // Explicitly Finishing the final JoinResult answer cancels its direct
            // pickup lease. Result-path transport close is intentionally different:
            // it detaches these answer records without interpreting them as Finish,
            // so a distinct live Accept host can still serve the committed
            // provision until TTL or owner teardown.
            if (hosted.result_refs == 0 and hosted.accept_live) {
                // Canonical cancellation guards both the result owner and a
                // distinct Accept host across the captured JoinNetwork callback.
                // Calling through the Accept peer directly would let a reentrant
                // callback deinit that peer while its cleanup frame still borrowed
                // the peer allocator.
                cancelHostedJoin(self, hosted);
                return;
            }
            maybeDestroyHostedJoin(self, hosted);
        }

        /// Drop the canonical result reference held by a completing-answer
        /// tombstone.  The tombstone itself remains published until the whole
        /// fanout has finished, keeping the inbound answer ID unavailable to
        /// callback-triggered reuse.
        pub fn dropCompletingJoinResultRef(self: *Peer, completing: *CompletingJoinAnswer) void {
            const hosted = completing.hosted orelse return;
            completing.hosted = null;
            std.debug.assert(hosted.owner_peer == self);
            std.debug.assert(hosted.result_refs > 0);
            hosted.result_refs -= 1;
            if (hosted.result_refs == 0 and hosted.accept_live) {
                cancelHostedJoin(self, hosted);
                return;
            }
            maybeDestroyHostedJoin(self, hosted);
        }

        pub fn putCompletingJoinAnswerAssumeCapacity(
            self: *Peer,
            answer_id: u32,
            counts_as_join_record: bool,
        ) void {
            std.debug.assert(!self.completing_join_answers.contains(answer_id));
            self.completing_join_answers.putAssumeCapacity(answer_id, .{
                .counts_as_join_record = counts_as_join_record,
            });
            if (counts_as_join_record) self.completing_join_answer_records += 1;
        }

        pub fn retireCompletingJoinAnswerAccounting(self: *Peer, completing: *CompletingJoinAnswer) void {
            if (!completing.counts_as_join_record) return;
            completing.counts_as_join_record = false;
            std.debug.assert(self.completing_join_answer_records > 0);
            self.completing_join_answer_records -= 1;
        }

        pub fn removeCompletingJoinAnswer(self: *Peer, answer_id: u32) bool {
            const removed = self.completing_join_answers.fetchRemove(answer_id) orelse return false;
            if (removed.value.counts_as_join_record) {
                std.debug.assert(self.completing_join_answer_records > 0);
                self.completing_join_answer_records -= 1;
            }
            return true;
        }

        /// Finish may arrive synchronously from an observer or Return transport
        /// callback while a complete Join is still fanning out.  Marking rather
        /// than removing preserves the answer reservation through later sends.
        pub fn finishCompletingJoinAnswer(self: *Peer, answer_id: u32, release_result_caps: bool) bool {
            const completing = self.completing_join_answers.getPtr(answer_id) orelse return false;
            if (completing.finished) return true;
            completing.finished = true;
            completing.release_result_caps = release_result_caps;
            // Finish makes this answer operationally retired immediately, even
            // though its uncounted tombstone still reserves the wire ID until the
            // surrounding fanout unwinds.
            retireCompletingJoinAnswerAccounting(self, completing);
            dropCompletingJoinResultRef(self, completing);
            return true;
        }

        pub fn forgetPendingJoinResultAnswer(self: *Peer, answer_id: u32) void {
            clearPendingJoinResultAnswer(self, answer_id);
        }

        pub fn retireCompletingJoinTransitionAccounting(
            self: *Peer,
            join_state: *JoinState,
            transition_live: *bool,
        ) void {
            if (transition_live.*) {
                std.debug.assert(self.completing_join_records > 0);
                self.completing_join_records -= 1;
                transition_live.* = false;
            }
            var it = join_state.parts.valueIterator();
            while (it.next()) |part| {
                if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                    retireCompletingJoinAnswerAccounting(self, completing);
                }
            }
        }

        pub fn completeJoinLegacy(self: *Peer, join_id: u32) !void {
            const pending = self.pending_joins.get(join_id) orelse return;
            if (pending.parts.count() == 0) return;
            // `ensureJoinBudget` reserved the completing map before the final part
            // was published. Updating the scalar record first can therefore fail
            // only on an impossible configured-state overflow, while the bucket is
            // still wholly live and retryable.
            const completing_records = try std.math.add(usize, self.completing_join_records, 1);
            const removed = self.pending_joins.fetchRemove(join_id) orelse return;
            var join_state = removed.value;
            defer JoinState.deinit(&join_state, self.allocator);
            self.refreshNextJoinDeadline();

            self.completing_join_records = completing_records;
            var transition_live = true;
            defer if (transition_live) {
                std.debug.assert(self.completing_join_records > 0);
                self.completing_join_records -= 1;
            };
            var reserve_it = join_state.parts.valueIterator();
            while (reserve_it.next()) |part| {
                putCompletingJoinAnswerAssumeCapacity(self, part.question_id, true);
            }
            defer {
                var cleanup_answers = join_state.parts.valueIterator();
                while (cleanup_answers.next()) |part| {
                    _ = removeCompletingJoinAnswer(self, part.question_id);
                    _ = self.finished_early_answers.remove(part.question_id);
                }
            }
            var detach_it = join_state.parts.valueIterator();
            while (detach_it.next()) |part| _ = self.pending_join_questions.remove(part.question_id);

            var first_target: ?*const ProvideTarget = null;
            var all_equal = true;
            var target_it = join_state.parts.valueIterator();
            while (target_it.next()) |part| {
                if (first_target) |target| {
                    if (!provideTargetsEqual(target, &part.target)) {
                        all_equal = false;
                        break;
                    }
                } else first_target = &part.target;
            }

            // Legacy completion has no steady Join lease. Retire every gauge before
            // the first Return while retaining uncounted answer tombstones across
            // synchronous Finish/reuse callbacks.
            std.debug.assert(self.completing_join_records > 0);
            self.completing_join_records -= 1;
            transition_live = false;
            var retire_it = join_state.parts.valueIterator();
            while (retire_it.next()) |part| {
                if (self.completing_join_answers.getPtr(part.question_id)) |completing| {
                    retireCompletingJoinAnswerAccounting(self, completing);
                }
            }

            var send_it = join_state.parts.valueIterator();
            while (send_it.next()) |part| {
                const completing = self.completing_join_answers.get(part.question_id) orelse continue;
                if (completing.finished) continue;
                if (all_equal) {
                    self.sendReturnProvidedTarget(part.question_id, first_target orelse &part.target) catch |err| {
                        try self.sendReturnException(part.question_id, joinWireReason(err));
                    };
                } else {
                    try self.sendReturnException(part.question_id, "join target mismatch");
                }
            }
        }

        const DetachedJoinAccept = struct {
            allocator: std.mem.Allocator,
            target: ProvideTarget,
        };

        /// Detach the Accept-host half without invoking the application-supplied
        /// JoinNetwork callback. Callers can therefore remove every cross-peer
        /// borrow before cancellation re-enters arbitrary host code.
        pub fn detachHostedJoinAcceptNoCallback(self: *Peer, hosted: *HostedJoin) ?DetachedJoinAccept {
            if (!hosted.accept_live or hosted.accept_peer != self) return null;
            const removed = self.pending_join_accepts.fetchRemove(hosted.provision) orelse return null;
            std.debug.assert(removed.value.hosted == hosted);
            self.deregisterJoinAcceptHost(hosted);
            hosted.accept_live = false;
            hosted.accept_peer = null;
            hosted.deadline_ns = null;
            self.refreshNextJoinDeadline();
            return .{ .allocator = self.allocator, .target = removed.value.target };
        }

        pub fn ownHostedJoin(self: *Peer, hosted: *HostedJoin) !void {
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
            ensureByteLimit(
                self.join_accept_bytes,
                hosted.provision.len,
                self.limits.max_pending_join_accept_bytes,
            ) catch return error.PeerLimitExceeded;
            try self.hosted_joins.putNoClobber(hosted, {});
            hosted.owner_record_live = true;
            self.join_accept_bytes += hosted.provision.len;
            hosted.bytes_charged = true;
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .join_accept_bytes,
                self.join_accept_bytes - hosted.provision.len,
                self.join_accept_bytes,
                self.limits.max_pending_join_accept_bytes,
            );
        }

        /// Publish canonical ownership by consuming the one record reserved for a
        /// detached complete bucket.  Capacity was checked before the JoinNetwork
        /// callback; unlike `ownHostedJoin`, this is a record-for-record exchange,
        /// not an additional admission.
        pub fn ownHostedJoinFromCompletion(self: *Peer, hosted: *HostedJoin) !void {
            ensureByteLimit(
                self.join_accept_bytes,
                hosted.provision.len,
                self.limits.max_pending_join_accept_bytes,
            ) catch return error.PeerLimitExceeded;
            try self.hosted_joins.putNoClobber(hosted, {});
            std.debug.assert(self.completing_join_records > 0);
            self.completing_join_records -= 1;
            hosted.owner_record_live = true;
            self.join_accept_bytes += hosted.provision.len;
            hosted.bytes_charged = true;
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .join_accept_bytes,
                self.join_accept_bytes - hosted.provision.len,
                self.join_accept_bytes,
                self.limits.max_pending_join_accept_bytes,
            );
        }

        /// Cancel the captured network provision once. State that can re-enter this
        /// owner is already detached; `operation_depth` prevents a nested cleanup
        /// from destroying the canonical object while the callback borrows it.
        pub fn retireHostedJoinNetwork(self: *Peer, hosted: *HostedJoin) void {
            self.enterJoinOperation();
            defer self.leaveJoinOperation();
            if (!hosted.network_live) {
                maybeDestroyHostedJoin(self, hosted);
                return;
            }
            hosted.network_live = false;
            detachHostedJoinOwnerRecordNoCallback(self, hosted);
            hosted.operation_depth += 1;
            hosted.network.cancelHostJoinResult(hosted.provision);
            hosted.operation_depth -= 1;
            maybeDestroyHostedJoin(self, hosted);
        }

        /// Remove the live owner provision record before an observer or captured
        /// network callback can re-enter either peer. A successful Accept can
        /// retire the network token while result answers still anchor the
        /// provision allocation; keep those bytes charged until the final result
        /// reference is Finished. Forced cancellation has already detached every
        /// result reference, so its bytes are refunded before the callback.
        pub fn detachHostedJoinOwnerRecordNoCallback(self: *Peer, hosted: *HostedJoin) void {
            if (hosted.owner_record_live) {
                std.debug.assert(self.hosted_joins.remove(hosted));
                hosted.owner_record_live = false;
            }
            if (hosted.cancelled or hosted.result_refs == 0) refundHostedJoinBytes(self, hosted);
        }

        pub fn refundHostedJoinBytes(self: *Peer, hosted: *HostedJoin) void {
            if (!hosted.bytes_charged) return;
            std.debug.assert(self.join_accept_bytes >= hosted.provision.len);
            self.join_accept_bytes -= hosted.provision.len;
            hosted.bytes_charged = false;
        }

        pub fn maybeDestroyHostedJoin(self: *Peer, hosted: *HostedJoin) void {
            if (hosted.owner_peer != self) return;
            if (hosted.accept_live or hosted.result_refs != 0 or hosted.operation_depth != 0) return;
            if (hosted.network_live) {
                retireHostedJoinNetwork(self, hosted);
                return;
            }
            detachHostedJoinOwnerRecordNoCallback(self, hosted);
            refundHostedJoinBytes(self, hosted);
            self.allocator.free(hosted.provision);
            self.allocator.destroy(hosted);
        }

        /// Canonical forced retirement used by expiry, Accept-host close, and
        /// owner teardown. Every map/backlink is detached before the network
        /// callback, making repeated close/deinit and callback reentrancy no-ops.
        pub fn cancelHostedJoin(self: *Peer, hosted: *HostedJoin) void {
            if (hosted.owner_peer != self) return;

            hosted.cancelled = true;

            var guards = JoinOperationGuards{};
            guards.add(self);
            if (hosted.accept_peer) |accept_peer| guards.add(accept_peer);
            guards.enter();
            defer guards.leave();

            var detached_accept: ?DetachedJoinAccept = null;
            if (hosted.accept_peer) |accept_peer| {
                detached_accept = detachHostedJoinAcceptNoCallback(accept_peer, hosted);
            }

            while (hosted.result_refs != 0) {
                var answer_id: ?u32 = null;
                var it = self.pending_join_result_answers.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.hosted == hosted) {
                        answer_id = entry.key_ptr.*;
                        break;
                    }
                }
                if (answer_id) |id| {
                    _ = self.pending_join_result_answers.remove(id);
                    hosted.result_refs -= 1;
                    continue;
                }

                // A canonical may be canceled synchronously while its JoinResult
                // fanout is still running. Neutralize those pre-reserved refs but
                // keep their answer-ID tombstones live so the outer fanout emits
                // one generic terminal per remaining answer without stale pickup.
                var completing_it = self.completing_join_answers.valueIterator();
                var detached_completing = false;
                while (completing_it.next()) |completing| {
                    if (completing.hosted != hosted) continue;
                    retireCompletingJoinAnswerAccounting(self, completing);
                    completing.hosted = null;
                    hosted.result_refs -= 1;
                    detached_completing = true;
                    break;
                }
                if (!detached_completing) {
                    std.debug.assert(hosted.result_refs == 0);
                    break;
                }
            }

            if (detached_accept) |*detached| detached.target.deinit(detached.allocator);
            retireHostedJoinNetwork(self, hosted);
        }

        pub fn cancelAllHostedJoins(self: *Peer) void {
            while (self.hosted_joins.count() != 0) {
                var it = self.hosted_joins.keyIterator();
                const hosted = (it.next() orelse break).*;
                cancelHostedJoin(self, hosted);
            }
            // A successfully consumed Accept retires the hosted provision before
            // its JoinResult answers are explicitly Finished. Those canonicals are
            // anchored only by this answer table and still need owner teardown.
            while (self.pending_join_result_answers.count() != 0) {
                var it = self.pending_join_result_answers.valueIterator();
                const hosted = (it.next() orelse break).hosted;
                cancelHostedJoin(self, hosted);
            }
        }

        pub fn sendReturnJoinResultPayload(self: *Peer, answer_id: u32, result_payload: []const u8) !void {
            const BuildCtx = struct {
                allocator: std.mem.Allocator,
                result_payload: []const u8,

                fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                    const ctx: *const @This() = localCastCtx(*const @This(), ctx_ptr);
                    var result_msg = try message.Message.initUnvalidated(ctx.allocator, ctx.result_payload);
                    defer result_msg.deinit();
                    const result = try result_msg.getRootAnyPointer();

                    var payload = try ret.payloadTyped();
                    const content = try payload.initContent();
                    try message.cloneAnyPointer(result, content);
                    _ = try ret.initCapTableTyped(0);
                }
            };

            var ctx = BuildCtx{
                .allocator = self.allocator,
                .result_payload = result_payload,
            };
            try self.sendReturnResults(answer_id, &ctx, BuildCtx.build);
        }
    };
}
