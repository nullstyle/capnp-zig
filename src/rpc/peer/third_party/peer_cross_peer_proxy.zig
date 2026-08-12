const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const message = @import("../../../serialization/message.zig");
const protocol = @import("../../wire/protocol.zig");
const state = @import("../state.zig");
const payload_remap = @import("../../caps/payload_remap.zig");

/// The cross-peer proxy machinery, extracted from `peer/mod.zig` and made
/// generic over the peer type (the JoinCoordinator extraction contract):
/// proxy export minting/destroy, cross-peer payload cloning and inbound-cap
/// mapping, the forwarded proxy call + its Return relay, and the cross-peer
/// Return results builder. `peer/mod.zig` keeps every caller-visible name as
/// a thunk on `Peer`.
pub fn CrossPeerProxy(comptime Peer: type) type {
    return struct {
        const CrossPeerProxyContext = Peer.CrossPeerProxyCtx;
        const ProvideTarget = state.ProvideTarget;
        const CrossPeerCapMapContext = Peer.CrossPeerCapMapCtx;
        const CrossPeerProxyCallContext = Peer.CrossPeerProxyCallCtx;
        const CrossPeerReturnRelayContext = Peer.CrossPeerReturnRelayCtx;
        const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        /// The outbound descriptor variant for one of our own exports: senderPromise
        /// for an unresolved export promise, senderHosted otherwise. Mirrors
        /// classifyCap's isExportPromise-before-hasExport ordering so the
        /// origin-carrying forward/provide paths match the app-authored path.
        pub fn exportedCapTag(self: *Peer, cap_id: u32) protocol.CapDescriptorTag {
            return if (self.caps.isExportPromise(cap_id)) .senderPromise else .senderHosted;
        }

        pub fn addCrossPeerProxyExport(
            self: *Peer,
            source_peer: *Peer,
            target: cap_table.ResolvedCap,
            release_source_import_id: ?u32,
            release_source_export_pin_id: ?u32,
            release_source_import_pin_id: ?u32,
        ) !u32 {
            self.assertThreadAffinity();
            // OWNERSHIP: every source-peer lease (the retained import ref, the
            // handoff export pin, and the handoff import pin) transfers to this
            // call — released EXACTLY ONCE on any failure. Three disjoint
            // custodians, tracked so no two can both fire:
            //   - before the ctx exists (`leases_transferred == false`): the
            //     errdefer's manual arm releases the raw leases;
            //   - while the ctx exists un-consumed (`proxy_ctx != null`): the
            //     errdefer deinits the ctx, which releases them;
            //   - after a failed registerCrossPeerProxy: the destroy sweep below
            //     consumes the ctx (its deinit releases them) — the errdefer must
            //     then release NOTHING, which is what `leases_transferred`
            //     staying true guarantees. (Without it, the manual arm fired a
            //     SECOND release here and stole a coexisting provision pin.)
            // The caller must NEVER roll any lease back after invoking.
            var proxy_ctx: ?*CrossPeerProxyContext = null;
            var leases_transferred = false;
            errdefer {
                if (proxy_ctx) |ctx| {
                    CrossPeerProxyContext.deinit(self.allocator, ctx);
                } else if (!leases_transferred) {
                    if (release_source_import_id) |import_id| {
                        source_peer.releaseImport(import_id, 1) catch |err| {
                            log.debug("cross-peer proxy: failed to release source import {} after allocation failure: {}", .{ import_id, err });
                        };
                    }
                    if (release_source_export_pin_id) |pin_id| {
                        source_peer.releaseHandoffHeldExport(pin_id);
                    }
                    if (release_source_import_pin_id) |pin_id| {
                        source_peer.releaseHandoffImportPin(pin_id) catch |err| {
                            log.debug("cross-peer proxy: failed to release source import handoff pin {} after allocation failure: {}", .{ pin_id, err });
                        };
                    }
                }
            }

            const ctx = try self.allocator.create(CrossPeerProxyContext);
            proxy_ctx = ctx;
            leases_transferred = true;
            ctx.* = .{
                .owner_peer = self,
                .source_peer = source_peer,
                .target = target,
                .release_source_import_id = release_source_import_id,
                .release_source_export_pin_id = release_source_export_pin_id,
                .release_source_import_pin_id = release_source_import_pin_id,
            };

            const id = try self.addExportWithDeinit(
                .{ .ctx = ctx, .on_call = CrossPeerProxyContext.onCall },
                CrossPeerProxyContext.deinit,
            );
            ctx.export_id = id;
            source_peer.registerCrossPeerProxy(self, id) catch |err| {
                // The destroy sweep runs the ctx deinit — the leases' single
                // release. Null the ctx so the errdefer arm fires neither branch.
                proxy_ctx = null;
                destroyUnreferencedProxyExport(self, id);
                return err;
            };
            proxy_ctx = null;
            return id;
        }

        pub fn destroyUnreferencedProxyExport(self: *Peer, id: u32) void {
            destroyUnreferencedExport(self, id);
        }

        pub fn destroyUnreferencedExport(self: *Peer, id: u32) void {
            const entry = self.exports.get(id) orelse return;
            if (entry.ref_count != 0 or entry.answer_ref_count != 0 or entry.promise_ref_count != 0 or entry.handoff_ref_count != 0) return;

            const removed = self.exports.fetchRemove(id) orelse return;
            self.caps.clearExport(id);
            if (removed.value.deinit_ctx) |deinit_ctx| {
                if (removed.value.handler) |handler| deinit_ctx(self.allocator, handler.ctx);
            }
        }

        pub fn clonePayloadAcrossPeers(
            self: *Peer,
            builder: *message.MessageBuilder,
            payload_builder: protocol.PayloadBuilder,
            source: protocol.Payload,
            inbound_peer: *Peer,
            inbound_caps: *cap_table.InboundCapTable,
            created_proxy_ids: *std.ArrayList(u32),
            pin_source_caps: bool,
        ) !void {
            var map_ctx = CrossPeerCapMapContext.init(inbound_peer, self, inbound_caps, created_proxy_ids, pin_source_caps);
            defer map_ctx.deinit();
            try payload_remap.clonePayloadWithRemappedCaps(
                CrossPeerCapMapContext,
                self.allocator,
                &map_ctx,
                builder,
                payload_builder,
                source,
                inbound_caps,
                mapCrossPeerInboundCap,
            );
        }

        pub fn mapCrossPeerInboundCap(
            ctx: *CrossPeerCapMapContext,
            _: *const cap_table.InboundCapTable,
            cap_index: u32,
        ) !?payload_remap.RemappedCap {
            if (ctx.remapped_by_index.get(cap_index)) |proxy_id| {
                return .{
                    .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
                    .cap_id = proxy_id,
                };
            }

            const entry = try ctx.inbound_caps.get(cap_index);
            if (entry == .none) return null;

            var release_source_import_id: ?u32 = null;
            var release_source_export_pin_id: ?u32 = null;
            var release_source_import_pin_id: ?u32 = null;
            if (ctx.pin_source_caps) {
                switch (entry) {
                    .exported => |cap| {
                        try ctx.inbound_peer.noteHandoffExportRef(cap.id);
                        release_source_export_pin_id = cap.id;
                    },
                    .imported => |cap| {
                        try ctx.inbound_peer.noteHandoffImportPin(cap.id);
                        release_source_import_pin_id = cap.id;
                    },
                    // A receiverAnswer's transform reader borrows the temporary
                    // source Return. Supporting it requires an owned transform in
                    // CrossPeerProxyContext; fail before publishing a proxy rather
                    // than retaining a dangling reader.
                    .promised => return error.RedirectedPromisedCapabilityUnsupported,
                    .none => return null,
                }
            } else switch (entry) {
                .imported => |cap| {
                    try ctx.inbound_caps.retainIndex(cap_index);
                    release_source_import_id = cap.id;
                },
                else => {},
            }

            var pins_transferred = false;
            errdefer if (!pins_transferred) {
                if (release_source_export_pin_id) |id| ctx.inbound_peer.rollbackHandoffExportRef(id);
                if (release_source_import_pin_id) |id| ctx.inbound_peer.rollbackHandoffImportPin(id);
            };
            // addCrossPeerProxyExport owns every supplied lease on success AND
            // failure; no caller-side rollback is legal past this point.
            pins_transferred = true;

            const proxy_id = try ctx.outbound_peer.addCrossPeerProxyExport(
                ctx.inbound_peer,
                entry,
                release_source_import_id,
                release_source_export_pin_id,
                release_source_import_pin_id,
            );
            errdefer ctx.outbound_peer.destroyUnreferencedProxyExport(proxy_id);
            try ctx.created_proxy_ids.append(ctx.outbound_peer.allocator, proxy_id);
            try ctx.remapped_by_index.put(cap_index, proxy_id);

            return .{
                .origin_code = cap_table.descriptors.originCodeForTag(.senderHosted),
                .cap_id = proxy_id,
            };
        }

        pub fn forwardCrossPeerProxyCall(
            self: *Peer,
            call: protocol.Call,
            inbound_caps: *const cap_table.InboundCapTable,
            forward_peer: *Peer,
            target: cap_table.ResolvedCap,
        ) !void {
            if (target == .none) {
                try self.sendReturnException(call.question_id, "cross-peer proxy target unavailable");
                return;
            }

            const relay = try forward_peer.allocator.create(CrossPeerProxyCallContext);
            relay.* = .{
                .recipient_peer = self,
                .recipient_answer_id = call.question_id,
                .forward_peer = forward_peer,
                .target = target,
                .source_params = call.params,
                .source_inbound_caps = @constCast(inbound_caps),
            };
            var relay_owned = true;
            errdefer if (relay_owned) CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);

            var forward_settled = false;
            relay.settled_flag = &forward_settled;

            const forwarded_question_id = sendCrossPeerProxyResolvedCall(
                forward_peer,
                target,
                call.interface_id,
                call.method_id,
                relay,
                buildCrossPeerProxyCall,
                onCrossPeerProxyReturn,
            ) catch |err| {
                if (forward_settled) {
                    relay_owned = false;
                    return;
                }
                relay_owned = false;
                CrossPeerProxyCallContext.deinit(forward_peer.allocator, relay);
                self.sendReturnException(call.question_id, @errorName(err)) catch |send_err| {
                    log.debug("cross-peer proxy: failed to fail question {}: {}", .{ call.question_id, send_err });
                };
                return;
            };

            if (forward_settled) {
                relay_owned = false;
                return;
            }
            relay.param_proxies_committed = true;
            relay.settled_flag = null;
            if (forward_peer.questions.getPtr(forwarded_question_id)) |q| {
                q.deinit_ctx = CrossPeerProxyCallContext.deinit;
                q.restore_on_return_error = false;
            }
            relay_owned = false;
        }

        pub fn sendCrossPeerProxyResolvedCall(
            self: *Peer,
            target: cap_table.ResolvedCap,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            return switch (target) {
                .imported => |cap| self.sendForwardedVineCall(cap.id, interface_id, method_id, ctx, build, on_return),
                .exported, .promised => self.sendCallResolved(target, interface_id, method_id, ctx, build, on_return),
                .none => error.CapabilityUnavailable,
            };
        }

        pub fn buildCrossPeerProxyCall(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
            const ctx: *CrossPeerProxyCallContext = localCastCtx(*CrossPeerProxyCallContext, ctx_ptr);
            const payload_builder = try call_builder.payloadTyped();
            try ctx.forward_peer.clonePayloadAcrossPeers(
                call_builder.call.builder,
                payload_builder,
                ctx.source_params,
                ctx.recipient_peer,
                ctx.source_inbound_caps,
                &ctx.created_param_proxy_ids,
                false,
            );
        }

        pub fn onCrossPeerProxyReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const ctx: *CrossPeerProxyCallContext = localCastCtx(*CrossPeerProxyCallContext, ctx_ptr);
            const recipient = ctx.recipient_peer;
            const answer_id = ctx.recipient_answer_id;
            const release_param_caps = ctx.created_param_proxy_ids.items.len == 0;
            if (ctx.settled_flag) |flag| flag.* = true;
            defer CrossPeerProxyCallContext.deinit(peer.allocator, ctx);

            relayReturnAcrossPeers(recipient, answer_id, peer, ret, inbound_caps, release_param_caps) catch |err| {
                log.debug("cross-peer proxy return relay failed for question {}: {}", .{ answer_id, err });
            };
        }

        pub fn relayReturnAcrossPeers(
            recipient: *Peer,
            answer_id: u32,
            source_peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
            release_param_caps: bool,
        ) !void {
            switch (ret.tag) {
                .results => {
                    const payload = ret.results orelse {
                        try recipient.sendReturnException(answer_id, "cross-peer forwarded call: missing results");
                        return;
                    };
                    var results_ctx = CrossPeerReturnRelayContext{
                        .source_peer = source_peer,
                        .target_peer = recipient,
                        .source = payload,
                        .source_inbound_caps = @constCast(inbound_caps),
                        .release_param_caps = release_param_caps,
                    };
                    defer results_ctx.deinit(recipient.allocator);
                    try recipient.sendReturnResults(answer_id, &results_ctx, buildCrossPeerReturnResults);
                    results_ctx.result_proxies_committed = true;
                },
                .exception => {
                    const reason = if (ret.exception) |e| e.reason else "cross-peer forwarded call failed";
                    // Relay the origin's type verbatim: a `disconnected` upstream
                    // must not reach the caller as a generic `failed`, or the
                    // caller cannot tell a retryable loss from an application error.
                    const ex_type = if (ret.exception) |e| e.kind() else protocol.ExceptionType.failed;
                    try recipient.sendReturnExceptionTyped(answer_id, reason, ex_type);
                },
                else => {
                    try recipient.sendReturnException(answer_id, "cross-peer forwarded call: unexpected return");
                },
            }
        }

        pub fn buildCrossPeerReturnResults(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) anyerror!void {
            const ctx: *CrossPeerReturnRelayContext = localCastCtx(*CrossPeerReturnRelayContext, ctx_ptr);
            ret_builder.setReleaseParamCaps(ctx.release_param_caps);
            const payload_builder = try ret_builder.payloadTyped();
            try ctx.target_peer.clonePayloadAcrossPeers(
                ret_builder.ret.builder,
                payload_builder,
                ctx.source,
                ctx.source_peer,
                ctx.source_inbound_caps,
                &ctx.created_result_proxy_ids,
                ctx.pin_source_caps,
            );
        }
    };
}
