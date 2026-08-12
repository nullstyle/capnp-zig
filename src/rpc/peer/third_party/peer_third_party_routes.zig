const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const third_party = @import("../third_party.zig");
const return_routing = @import("../../promises/return_routing.zig");

/// The automatic third-party (sendResultsTo.thirdParty) routing family,
/// extracted from `peer/mod.zig` and made generic over the peer type (the
/// JoinCoordinator extraction contract): route begin/finalize/fail,
/// results/exception delivery through the route, source-marker completion,
/// operation-depth guards (incl. the join-operation pair), route
/// neutralization on source/target peers, and the sendResultsTo
/// yourself/thirdParty markers. `peer/mod.zig` keeps every caller-visible
/// name as a thunk on `Peer`.
pub fn ThirdPartyRoutes(comptime Peer: type) type {
    return struct {
        const AutomaticThirdPartyRoute = Peer.AutomaticThirdPartyRouteRecord;
        const ensureCountLimit = Peer.ensureCountLimit;
        const ensureByteLimit = Peer.ensureByteLimit;
        const captureAnyPointerPayloadHelper = third_party.captureAnyPointerPayload;
        const ReturnBuildFn = *const fn (ctx: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void;
        const ProvideTarget = state.ProvideTarget;
        const CrossPeerReturnRelayContext = Peer.CrossPeerReturnRelayCtx;
        const optionalPayloadBytes = Peer.optionalPayloadBytes;
        const automatic_third_party_target_canceled = Peer.automatic_third_party_target_canceled_reason;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        /// Interpret a locally-authored outbound payload's encoded descriptor table
        /// from the source peer's point of view. Unlike InboundCapTable.init, this
        /// does not take wire import refs: senderHosted is one of our exports and
        /// receiverHosted is one of our imports. The table exists only while the
        /// payload is cloned into cross-peer proxy exports.
        pub fn sourceResolvedCapsForPayload(self: *Peer, payload: protocol.Payload) !cap_table.InboundCapTable {
            const count: u32 = if (payload.cap_table) |list| list.len() else 0;
            var inbound = cap_table.InboundCapTable{
                .allocator = self.allocator,
                .entries = try self.allocator.alloc(cap_table.ResolvedCap, count),
                .retained = undefined,
            };
            errdefer self.allocator.free(inbound.entries);
            inbound.retained = try self.allocator.alloc(bool, count);
            errdefer self.allocator.free(inbound.retained);
            @memset(inbound.retained, false);

            const list = payload.cap_table orelse return inbound;
            var idx: u32 = 0;
            while (idx < count) : (idx += 1) {
                const descriptor = try protocol.CapDescriptor.fromReader(try list.get(idx));
                inbound.entries[idx] = switch (descriptor.tag) {
                    .none => .none,
                    .senderHosted, .senderPromise => .{ .exported = .{
                        .id = descriptor.id orelse return error.MissingCapDescriptorId,
                    } },
                    .receiverHosted => .{ .imported = .{
                        .id = descriptor.id orelse return error.MissingCapDescriptorId,
                    } },
                    .receiverAnswer => .{ .promised = descriptor.promised_answer orelse
                        return error.MissingPromisedAnswer },
                    .thirdPartyHosted => .{ .exported = .{
                        .id = (descriptor.third_party orelse
                            return error.MissingThirdPartyCapDescriptor).vine_id,
                    } },
                };
            }
            return inbound;
        }

        pub fn detachSettledAutomaticThirdPartyTarget(route: *AutomaticThirdPartyRoute) void {
            if (route.target_peer) |target| {
                if (target.incoming_automatic_third_party_routes.get(route.target_answer_id) == route) {
                    _ = target.incoming_automatic_third_party_routes.remove(route.target_answer_id);
                }
            }
            route.target_peer = null;
            route.target_outcome = .settled;
        }

        pub fn completeAutomaticThirdPartySourceMarker(
            self: *Peer,
            route: *AutomaticThirdPartyRoute,
        ) !void {
            self.sendReturnResultsSentElsewhere(route.source_answer_id) catch |err| {
                route.operation_active = false;
                route.source_marker_failed = true;
                // The target terminal is already visible. Preserve this owned
                // allocation as a tombstone while the handler error unwinds; Call
                // dispatch must not answer the ambiguous send with another Return.
                log.debug("automatic third-party redirect: source settlement send failed after target commit: {}", .{err});
                // The transport send may have delivered the marker before
                // reporting failure. Propagate a classified terminal error so Call
                // dispatch closes/fails the connection without attempting a second
                // Return for this answer id.
                return error.AutomaticThirdPartySourceSettlementFailed;
            };
            route.operation_active = false;
            // A successful marker clears routing through the deferred-clear guard.
            std.debug.assert(route.clear_requested);
            finalizeAutomaticThirdPartyRoute(self, route, null);
        }

        pub fn sendAutomaticThirdPartyResults(
            self: *Peer,
            route: *AutomaticThirdPartyRoute,
            ctx: *anyopaque,
            build: ReturnBuildFn,
        ) !void {
            enterAutomaticThirdPartyOperation(self);
            defer leaveAutomaticThirdPartyOperation(self);
            if (route.operation_active) return error.ThirdPartyRedirectReentrant;
            route.operation_active = true;
            var route_borrowed = true;
            errdefer if (route_borrowed) {
                route.operation_active = false;
            };

            switch (route.target_outcome) {
                .canceled => {
                    route_borrowed = false;
                    return completeAutomaticThirdPartySourceMarker(self, route);
                },
                .disconnected => {
                    route.operation_active = false;
                    const answer_id = route.source_answer_id;
                    route_borrowed = false;
                    finalizeAutomaticThirdPartyRoute(self, route, null);
                    try self.sendReturnExceptionTyped(
                        answer_id,
                        "automatic third-party result connection closed",
                        .disconnected,
                    );
                    return;
                },
                .settled => {
                    route.operation_active = false;
                    route_borrowed = false;
                    return error.ThirdPartyResultsAlreadyDelivered;
                },
                .connected => {},
            }
            const target = route.target_peer orelse {
                route.operation_active = false;
                return error.ThirdPartyResultPeerUnavailable;
            };
            target.enterAutomaticThirdPartyOperation();
            defer target.leaveAutomaticThirdPartyOperation();

            // Build exactly once in the handler/source peer's capability id-space,
            // then encode a temporary cap table whose descriptor variants preserve
            // that space for the cross-peer remapper below.
            var source_builder = protocol.MessageBuilder.init(self.allocator);
            defer source_builder.deinit();
            var source_ret = try source_builder.beginReturn(route.source_answer_id, .results);
            source_ret.setReleaseParamCaps(self.returnReleasesParamCaps(route.source_answer_id));
            try build(ctx, &source_ret);
            var source_effects = cap_table.OutboundCapEffects.init(self.allocator, null, null);
            defer source_effects.deinit();
            _ = try cap_table.encodeReturnPayloadCapsWithEffects(&self.caps, &source_ret, null, &source_effects);

            const source_frame = try source_builder.finish();
            defer self.allocator.free(source_frame);
            var decoded = try protocol.DecodedMessage.init(self.allocator, source_frame);
            defer decoded.deinit();
            const decoded_ret = try decoded.asReturn();
            const source_payload = decoded_ret.results orelse return error.MissingReturnResults;
            var source_caps = try sourceResolvedCapsForPayload(self, source_payload);
            defer source_caps.deinit();

            var relay = CrossPeerReturnRelayContext{
                .source_peer = self,
                .target_peer = target,
                .source = source_payload,
                .source_inbound_caps = &source_caps,
                .release_param_caps = false,
                .pin_source_caps = true,
            };
            defer relay.deinit(target.allocator);

            route.delivering_result = true;
            target.sendReturnResults(route.target_answer_id, &relay, Peer.buildCrossPeerReturnResults) catch |err| {
                route.delivering_result = false;
                route.operation_active = false;
                if (route.clear_requested) {
                    route_borrowed = false;
                    finalizeAutomaticThirdPartyRoute(self, route, null);
                }
                return err;
            };
            route.delivering_result = false;
            relay.result_proxies_committed = true;
            detachSettledAutomaticThirdPartyTarget(route);

            // Source Finish may have arrived synchronously while the target Return
            // was in flight. The target result is committed, but the canceled source
            // answer needs no resultsSentElsewhere marker.
            if (route.clear_requested) {
                route.operation_active = false;
                route_borrowed = false;
                finalizeAutomaticThirdPartyRoute(self, route, null);
                return;
            }
            route_borrowed = false;
            try completeAutomaticThirdPartySourceMarker(self, route);
        }

        pub fn sendAutomaticThirdPartyException(
            self: *Peer,
            route: *AutomaticThirdPartyRoute,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) !void {
            enterAutomaticThirdPartyOperation(self);
            defer leaveAutomaticThirdPartyOperation(self);
            if (route.operation_active) return error.ThirdPartyRedirectReentrant;
            route.operation_active = true;
            var route_borrowed = true;
            errdefer if (route_borrowed) {
                route.operation_active = false;
            };

            switch (route.target_outcome) {
                .connected => {
                    const target = route.target_peer orelse {
                        route.operation_active = false;
                        return error.ThirdPartyResultPeerUnavailable;
                    };
                    target.enterAutomaticThirdPartyOperation();
                    defer target.leaveAutomaticThirdPartyOperation();
                    route.delivering_result = true;
                    target.sendReturnExceptionNoDrain(route.target_answer_id, reason, ex_type) catch |err| {
                        route.delivering_result = false;
                        route.operation_active = false;
                        if (route.clear_requested) {
                            route_borrowed = false;
                            finalizeAutomaticThirdPartyRoute(self, route, null);
                        }
                        return err;
                    };
                    target.failQueuedPromisedCalls(route.target_answer_id, reason, ex_type);
                    route.delivering_result = false;
                    detachSettledAutomaticThirdPartyTarget(route);
                },
                .canceled => {},
                .disconnected => {
                    route.operation_active = false;
                    const answer_id = route.source_answer_id;
                    route_borrowed = false;
                    finalizeAutomaticThirdPartyRoute(self, route, null);
                    try self.sendReturnExceptionNoDrain(answer_id, reason, ex_type);
                    self.failQueuedPromisedCalls(answer_id, reason, ex_type);
                    return;
                },
                .settled => {},
            }

            if (route.clear_requested) {
                route.operation_active = false;
                route_borrowed = false;
                finalizeAutomaticThirdPartyRoute(self, route, null);
                return;
            }
            route_borrowed = false;
            try completeAutomaticThirdPartySourceMarker(self, route);
        }

        /// Send a return with results for a previously received call.
        pub fn clearSendResultsToThirdPartyPayload(self: *Peer, answer_id: u32) void {
            if (self.send_results_to_third_party.fetchRemove(answer_id)) |entry| {
                if (entry.value) |payload| self.allocator.free(payload);
            }
        }

        pub fn enterAutomaticThirdPartyOperation(self: *Peer) void {
            self.automatic_third_party_operation_depth += 1;
        }

        pub fn completeDeferredAutomaticThirdPartyLifecycle(self: *Peer) void {
            if (self.automatic_third_party_operation_depth != 0 or
                self.automatic_third_party_dispatch_depth != 0 or
                self.join_operation_depth != 0)
            {
                return;
            }
            if (self.automatic_third_party_close_deferred) {
                self.automatic_third_party_close_deferred = false;
                self.finishTransportClosedNotification();
            }
            if (self.automatic_third_party_deinit_deferred) self.deinit();
        }

        pub fn leaveAutomaticThirdPartyOperation(self: *Peer) void {
            std.debug.assert(self.automatic_third_party_operation_depth > 0);
            self.automatic_third_party_operation_depth -= 1;
            completeDeferredAutomaticThirdPartyLifecycle(self);
        }

        pub fn enterJoinOperation(self: *Peer) void {
            self.join_operation_depth += 1;
        }

        pub fn leaveJoinOperation(self: *Peer) void {
            std.debug.assert(self.join_operation_depth > 0);
            self.join_operation_depth -= 1;
            completeDeferredAutomaticThirdPartyLifecycle(self);
        }

        pub fn failAutomaticThirdPartyTargetBestEffort(
            target: *Peer,
            answer_id: u32,
            reason: []const u8,
            ex_type: protocol.ExceptionType,
        ) void {
            if (target.in_deinit) return;
            if (!target.active_inbound_questions.contains(answer_id)) return;
            if (target.is_shutting_down or target.transport_close_notified) {
                _ = target.active_inbound_questions.remove(answer_id);
                _ = target.finished_early_answers.remove(answer_id);
                return;
            }
            target.enterAutomaticThirdPartyOperation();
            defer target.leaveAutomaticThirdPartyOperation();
            target.sendReturnExceptionNoDrain(answer_id, reason, ex_type) catch |err| {
                log.debug("automatic third-party redirect: failed to settle target answer {}: {}", .{
                    answer_id,
                    err,
                });
            };
            target.failQueuedPromisedCalls(answer_id, reason, ex_type);
            // A failed best-effort wire send must not keep the synthetic answer
            // locally active forever. The recipient transport will either have
            // seen the terminal or fail independently; this route is retired.
            _ = target.active_inbound_questions.remove(answer_id);
            _ = target.finished_early_answers.remove(answer_id);
        }

        /// Destroy a source-owned automatic route. All backlinks and owned routing
        /// bytes are detached before the optional target exception is emitted, so a
        /// synchronous callback can deinitialize either peer without observing a
        /// half-live route.
        pub fn finalizeAutomaticThirdPartyRoute(
            self: *Peer,
            route: *AutomaticThirdPartyRoute,
            fail_target_reason: ?[]const u8,
        ) void {
            std.debug.assert(!route.operation_active);
            const source_answer_id = route.source_answer_id;
            const target = route.target_peer;
            const target_answer_id = route.target_answer_id;
            const target_was_connected = route.target_outcome == .connected;

            if (self.automatic_third_party_routes.get(source_answer_id) == route) {
                _ = self.automatic_third_party_routes.remove(source_answer_id);
            }
            if (target) |target_peer| {
                if (target_peer.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                    _ = target_peer.incoming_automatic_third_party_routes.remove(target_answer_id);
                }
            }
            route.source_peer = null;
            route.target_peer = null;
            clearSendResultsToThirdPartyPayload(self, source_answer_id);
            self.allocator.destroy(route);

            if (target_was_connected) {
                if (target) |target_peer| {
                    if (fail_target_reason) |reason| {
                        failAutomaticThirdPartyTargetBestEffort(
                            target_peer,
                            target_answer_id,
                            reason,
                            .failed,
                        );
                    }
                }
            }
        }

        pub fn clearSendResultsToThirdParty(self: *Peer, answer_id: u32) void {
            if (self.automatic_third_party_routes.get(answer_id)) |route| {
                if (route.operation_active) {
                    route.clear_requested = true;
                    clearSendResultsToThirdPartyPayload(self, answer_id);
                    return;
                }
                finalizeAutomaticThirdPartyRoute(
                    self,
                    route,
                    "automatic third-party redirect canceled before delivering results",
                );
                return;
            }
            clearSendResultsToThirdPartyPayload(self, answer_id);
        }

        /// Target-side Finish hook. A Finish that arrives after ThirdPartyAnswer but
        /// before the result Return cancels only the synthetic recipient answer; it
        /// does not cancel the original call. A Finish reentrant from delivery is
        /// left to normal resolved-answer cleanup.
        pub fn noteAutomaticThirdPartyTargetFinish(self: *Peer, answer_id: u32) bool {
            const route = self.incoming_automatic_third_party_routes.get(answer_id) orelse return false;
            if (route.target_outcome != .connected) return false;
            // During the ThirdPartyAnswer announcement, Finish means the adopted
            // synthetic answer was canceled before results. During the result send
            // itself it is the normal reentrant Finish-after-Return lifecycle and
            // must leave the route intact until the sender commits its reservation.
            if (route.operation_active and route.delivering_result) return false;
            // Calls already pipelined on this synthetic answer are independent
            // questions and still require their own terminal Returns. Once the
            // parent is canceled no result can resolve their promised targets, so
            // fail and release them before detaching the route.
            self.failQueuedPromisedCalls(
                answer_id,
                automatic_third_party_target_canceled,
                .failed,
            );
            _ = self.incoming_automatic_third_party_routes.remove(answer_id);
            route.target_peer = null;
            route.target_outcome = .canceled;
            return true;
        }

        /// This peer is the target/result connection and is dying. Detach its
        /// borrowed backlinks and mark each source-owned route disconnected. The
        /// source handler's eventual completion turns that state into one caller-
        /// facing exception; retaining source ownership until then also absorbs a
        /// late async handler Return without emitting a second terminal frame.
        pub fn neutralizeAutomaticThirdPartyRoutesOnTargetPeer(self: *Peer) void {
            var routes = self.incoming_automatic_third_party_routes;
            self.incoming_automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(self.allocator);
            defer routes.deinit();

            var it = routes.valueIterator();
            while (it.next()) |route_ptr| {
                const route = route_ptr.*;
                if (route.target_peer != self) continue;
                _ = self.active_inbound_questions.remove(route.target_answer_id);
                _ = self.finished_early_answers.remove(route.target_answer_id);
                route.target_peer = null;
                route.target_outcome = .disconnected;
            }
        }

        /// This peer owns the source half and is dying. Remove every target
        /// backlink before emitting best-effort terminal exceptions to synthetic
        /// recipient answers.
        pub fn neutralizeAutomaticThirdPartyRoutesOnSourcePeer(self: *Peer) void {
            var routes = self.automatic_third_party_routes;
            self.automatic_third_party_routes = std.AutoHashMap(u32, *AutomaticThirdPartyRoute).init(self.allocator);
            defer routes.deinit();

            var it = routes.valueIterator();
            while (it.next()) |route_ptr| {
                const route = route_ptr.*;
                const target = route.target_peer;
                const target_answer_id = route.target_answer_id;
                const target_was_connected = route.target_outcome == .connected;
                if (target) |target_peer| {
                    if (target_peer.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                        _ = target_peer.incoming_automatic_third_party_routes.remove(target_answer_id);
                    }
                }
                clearSendResultsToThirdPartyPayload(self, route.source_answer_id);
                route.source_peer = null;
                route.target_peer = null;
                self.allocator.destroy(route);

                if (target_was_connected) {
                    if (target) |target_peer| {
                        failAutomaticThirdPartyTargetBestEffort(
                            target_peer,
                            target_answer_id,
                            "automatic third-party source connection closed",
                            .disconnected,
                        );
                    }
                }
            }
        }

        pub fn provideTargetsEqual(a: *const ProvideTarget, b: *const ProvideTarget) bool {
            return switch (a.*) {
                .local => |local| switch (b.*) {
                    .local => |other_local| local.origin_code == other_local.origin_code and local.cap_id == other_local.cap_id,
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

        pub fn captureAnyPointerPayload(
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

        pub fn noteSendResultsToYourself(self: *Peer, answer_id: u32) !void {
            try ensureCountLimit(
                self.send_results_to_yourself.contains(answer_id),
                self.send_results_to_yourself.count(),
                self.limits.max_send_results_to_yourself,
            );
            try return_routing.noteSendResultsToYourselfForPeer(
                Peer,
                self,
                answer_id,
                clearSendResultsToThirdParty,
            );
        }

        pub fn beginAutomaticThirdPartyRoute(self: *Peer, answer_id: u32, contact_payload: []const u8) !void {
            enterAutomaticThirdPartyOperation(self);
            defer leaveAutomaticThirdPartyOperation(self);
            if (self.transport_close_notified) return error.TransportClosed;
            const network = self.vat_network orelse return error.NoVatNetwork;

            var contact_msg = try message.Message.initUnvalidated(self.allocator, contact_payload);
            defer contact_msg.deinit();
            const contact = try contact_msg.getRootAnyPointer();

            var introduced = try network.connectToIntroduced(contact);
            defer introduced.deinit(self.allocator);
            const target = introduced.peer;
            target.assertThreadAffinity();
            if (target.is_shutting_down) return error.PeerShuttingDown;
            if (target.transport_close_notified) return error.TransportClosed;
            target.enterAutomaticThirdPartyOperation();
            defer target.leaveAutomaticThirdPartyOperation();

            try ensureCountLimit(
                false,
                target.active_inbound_questions.count(),
                target.limits.max_active_inbound_questions,
            );
            const target_answer_id = try target.allocateUnusedThirdPartyAnswerId();

            // Parse the completion before publishing any cross-peer state. The
            // network owns its encoding; all the peer needs is a reader that stays
            // live through the synchronous ThirdPartyAnswer build.
            var completion_msg = try message.Message.initUnvalidated(self.allocator, introduced.completion);
            defer completion_msg.deinit();
            const completion = try completion_msg.getRootAnyPointer();

            const route = try self.allocator.create(AutomaticThirdPartyRoute);
            var route_owned = true;
            route.* = .{
                .source_peer = self,
                .source_answer_id = answer_id,
                .target_peer = target,
                .target_answer_id = target_answer_id,
            };
            var source_registered = false;
            var target_registered = false;
            var answer_registered = false;
            errdefer {
                if (answer_registered) _ = target.active_inbound_questions.remove(target_answer_id);
                if (target_registered and
                    target.incoming_automatic_third_party_routes.get(target_answer_id) == route)
                {
                    _ = target.incoming_automatic_third_party_routes.remove(target_answer_id);
                }
                if (source_registered and
                    self.automatic_third_party_routes.get(answer_id) == route)
                {
                    _ = self.automatic_third_party_routes.remove(answer_id);
                }
                if (route_owned) self.allocator.destroy(route);
            }

            try self.automatic_third_party_routes.put(answer_id, route);
            source_registered = true;
            try target.incoming_automatic_third_party_routes.put(target_answer_id, route);
            target_registered = true;
            // No params crossed onto this connection, so the synthetic answer owes
            // no explicit parameter-cap releases. It otherwise behaves exactly like
            // an ordinary inbound Call answer for promise queueing and Finish.
            try target.active_inbound_questions.put(target_answer_id, false);
            answer_registered = true;

            route.operation_active = true;
            target.sendThirdPartyAnswerWithId(target_answer_id, completion) catch |err| {
                // Unlike a results Return followed by a reentrant Finish, a
                // ThirdPartyAnswer has no protocol acknowledgement that can prove
                // whether an error happened before or after the frame became
                // visible. Roll back our unpublished transaction on every reported
                // send failure. This is load-bearing: the transport owner must
                // treat such an ambiguous send error as terminal, so a recipient
                // that did see the announcement drains its adopted await when that
                // connection closes instead of waiting forever for a Return.
                route.operation_active = false;
                return err;
            };
            route.operation_active = false;

            if (target.automatic_third_party_deinit_deferred) {
                if (target.incoming_automatic_third_party_routes.get(target_answer_id) == route) {
                    _ = target.incoming_automatic_third_party_routes.remove(target_answer_id);
                }
                _ = target.active_inbound_questions.remove(target_answer_id);
                route.target_peer = null;
                route.target_outcome = .disconnected;
                target_registered = false;
                answer_registered = false;
            }

            if (self.automatic_third_party_deinit_deferred) {
                source_registered = false;
                target_registered = false;
                answer_registered = false;
                route_owned = false;
                finalizeAutomaticThirdPartyRoute(
                    self,
                    route,
                    "automatic third-party source connection closed",
                );
                return error.PeerShuttingDown;
            }

            // A synchronous Finish on the source connection may have cleared the
            // redirect while ThirdPartyAnswer was being delivered. Retire the
            // target answer now that the outer send no longer borrows `route`.
            if (route.clear_requested) {
                source_registered = false;
                target_registered = false;
                answer_registered = false;
                route_owned = false;
                finalizeAutomaticThirdPartyRoute(
                    self,
                    route,
                    "automatic third-party redirect canceled during announcement",
                );
                return;
            }

            source_registered = false;
            target_registered = false;
            answer_registered = false;
            route_owned = false;
        }

        pub fn noteSendResultsToThirdParty(
            self: *Peer,
            answer_id: u32,
            ptr: ?message.AnyPointerReader,
        ) !void {
            _ = self.send_results_to_yourself.remove(answer_id);

            const payload = try captureAnyPointerPayload(self.allocator, ptr);
            var payload_owned = true;
            errdefer if (payload_owned) {
                if (payload) |bytes| self.allocator.free(bytes);
            };

            try ensureCountLimit(
                self.send_results_to_third_party.contains(answer_id),
                self.send_results_to_third_party.count(),
                self.limits.max_send_results_to_third_party,
            );
            try ensureByteLimit(
                self.sendResultsToThirdPartyBytesExcluding(answer_id),
                optionalPayloadBytes(payload),
                self.limits.max_send_results_to_third_party_bytes,
            );

            const entry = try self.send_results_to_third_party.getOrPut(answer_id);
            if (entry.found_existing) {
                if (entry.value_ptr.*) |existing| self.allocator.free(existing);
            }
            entry.value_ptr.* = payload;
            payload_owned = false;

            if (self.third_party_result_policy == .vat_network) {
                errdefer clearSendResultsToThirdPartyPayload(self, answer_id);
                const contact_payload = payload orelse return error.MissingThirdPartyContact;
                try beginAutomaticThirdPartyRoute(self, answer_id, contact_payload);
            }
        }
    };
}
