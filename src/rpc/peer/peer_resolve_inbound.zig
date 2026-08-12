const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const resolve = @import("./resolve.zig");
const third_party = @import("./third_party.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");

/// Inbound Resolve handling and the L3 recipient auto-pickup, extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract): the Resolve dispatch arm, thirdPartyHosted
/// auto-pickup (embargo decision per rpc.capnp:885-888), accept-embargo id
/// minting, in-flight promise-call detection, and the handoff
/// accept-Disembargo send + Return callback. `peer/mod.zig` keeps every
/// caller-visible name as a thunk on `Peer`.
pub fn ResolveInbound(comptime Peer: type) type {
    return struct {
        const HandoffPickupContext = Peer.HandoffPickupContext;
        const releaseResolvedCap = Peer.releaseResolvedCap;
        const storeResolvedImport = Peer.storeResolvedImport;
        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        pub fn handleResolve(self: *Peer, resolve_msg: protocol.Resolve) !void {
            // Level-3 recipient auto-pickup: an inbound `thirdPartyHosted` Resolve for
            // a promise we hold, when a VatNetwork + pickup handler are configured,
            // is picked up directly from the third vat instead of proxied through the
            // vine. This branch is fully isolated from the two-party/reflected-
            // loopback resolve path below — it only fires for the thirdPartyHosted
            // descriptor tag with both L3 seams attached, and returns before the
            // generic ops run. Every other resolve (cap/exception, incl. the fragile
            // reflected-loopback `receiverHosted`/`senderHosted` cases) is unchanged.
            if (resolve_msg.tag == .cap) {
                if (resolve_msg.cap) |descriptor| {
                    if (descriptor.tag == .thirdPartyHosted and
                        self.vat_network != null and
                        self.on_handoff_pickup != null and
                        self.caps.imports.contains(resolve_msg.promise_id))
                    {
                        if (try tryAutoPickupThirdParty(self, resolve_msg.promise_id, descriptor)) {
                            return;
                        }
                        // tryAutoPickup returned false: not actionable (e.g. the
                        // network could not resolve the contact). Fall through to the
                        // Level-1/2 vine fallback so the promise still resolves.
                    }
                }
            }

            const ops = resolve.ResolveOps(Peer){
                .has_known_promise = resolve.hasKnownResolvePromiseForPeerFn(Peer),
                .resolve_cap_descriptor = resolve.resolveCapDescriptorForPeerFn(Peer),
                .release_resolved_cap = releaseResolvedCap,
                .alloc_embargo_id = resolve.allocateEmbargoIdForPeerFn(Peer),
                .remember_pending_embargo = Peer.rememberPendingEmbargo,
                .forget_pending_embargo = resolve.forgetPendingEmbargoForPeerFn(Peer),
                .send_disembargo_sender_loopback = peer_outbound_control.sendDisembargoSenderLoopbackViaSendFrameForPeerFn(Peer, Peer.sendFrame),
                .store_resolved_import = storeResolvedImport,
            };
            try resolve.handleResolveWithOps(Peer, self, resolve_msg, ops);
        }

        /// Attempt the Level-3 recipient auto-pickup for an inbound `thirdPartyHosted`
        /// Resolve. Returns true if the pickup was initiated (an `Accept` was sent and
        /// ownership of the vine reference transferred to the pickup flow); false if
        /// the handoff is not actionable and the caller should fall back to the
        /// Level-1/2 vine proxy. On a true return the promise import's vine reference
        /// has been noted and will be released when the Accept `Return` arrives.
        pub fn tryAutoPickupThirdParty(self: *Peer, promise_id: u32, descriptor: protocol.CapDescriptor) !bool {
            const third = descriptor.third_party orelse return false;
            const contact = third.id orelse return false;
            const network = self.vat_network orelse return false;
            const user_cb = self.on_handoff_pickup orelse return false;
            const user_ctx = self.handoff_pickup_ctx orelse return false;

            // Resolve the ThirdPartyToContact to a live peer connected to the third
            // vat plus the completion to present in the Accept, BEFORE noting the vine
            // import. A network that cannot place the contact returns false, leaving
            // the caller on the vine-proxy fallback (which does its own noteImport) —
            // so we must not have taken a vine ref yet on that path. Every early
            // return below this point is either an error (handled by errdefer) or
            // happens before `noteImport`.
            var introduced = network.connectToIntroduced(contact) catch |err| {
                log.debug("auto-pickup connectToIntroduced failed: {}; using vine fallback", .{err});
                return false;
            };
            defer introduced.deinit(self.allocator);
            const accept_peer = introduced.peer;
            accept_peer.assertThreadAffinity();
            if (accept_peer.is_shutting_down) return false;

            // Account for the wire reference the descriptor hands us on the vine. We
            // own it now and release it once the direct cap is in hand (below /
            // onHandoffAcceptReturn). This is the ref whose eventual Release signals
            // VatB to Finish its Provide (rpc.capnp:1232-1237). Past this point every
            // remaining failure is an error return, so the errdefer covers rollback;
            // there is no non-error `return false` that could leak the ref.
            try self.caps.noteImport(third.vine_id);
            var vine_owned = true;
            errdefer if (vine_owned) {
                self.releaseImport(third.vine_id, 1) catch |err| {
                    log.debug("auto-pickup vine rollback release failed: {}", .{err});
                };
            };

            var completion_msg = try message.Message.initUnvalidated(self.allocator, introduced.completion);
            defer completion_msg.deinit();
            const provision = try completion_msg.getRootAnyPointer();

            const heap = try self.allocator.create(HandoffPickupContext);
            heap.* = .{
                .allocator = self.allocator,
                .promise_peer = self,
                .promise_id = promise_id,
                .vine_id = third.vine_id,
                .user_ctx = user_ctx,
                .user_cb = user_cb,
            };
            var heap_owned = true;
            errdefer if (heap_owned) heap.deinitSelf();
            try self.registerHandoffPickup(heap);
            heap.vine_owned = true;
            vine_owned = false;
            var pickup_settled = false;
            heap.settled_flag = &pickup_settled;

            // PHASE 4 — embargo/disembargo ordering during a live-promise handoff.
            //
            // If VatA (this peer, holding the promise import) still has a pipelined
            // call in flight against the promise being resolved, the accepted cap
            // MUST be embargoed so that the pipelined call (which VatB will forward
            // to VatC on the old path) is delivered to VatC BEFORE any post-pickup
            // direct call — preserving Alice's e-order (rpc.capnp:885-903). We:
            //   (1) allocate an opaque accept-embargo byte id,
            //   (2) send `Accept{embargo=id}` to VatC (VatC holds the Return + any
            //       pipelined calls until the disembargo arrives), and
            //   (3) send `Disembargo{context.accept=id}` to VatB on the promise path;
            //       VatB forwards it to VatC behind the already-forwarded pipelined
            //       call, releasing the held Accept in e-order.
            // If NO call is in flight, keep the P3 fast path (`embargo = null`): there
            // is nothing to order the handoff against.
            var accept_embargo_buf: [ACCEPT_EMBARGO_ID_LEN]u8 = undefined;
            const embargo: ?[]const u8 = if (promiseImportHasInFlightCall(self, promise_id))
                nextAcceptEmbargoId(self, &accept_embargo_buf)
            else
                null;

            // Send the Accept on the third-vat connection. The pickup callback owns
            // and frees `heap`, so allocate the question with no restore from the
            // start: synchronous loopback can deliver the Accept Return before this
            // send call returns, and a callback/post-callback error must not restore
            // a question whose ctx has already been freed. Auto-Finish is also
            // suppressed so the callback can Finish the Accept answer after the
            // handler/vine cleanup while the context is still valid, with a bounded
            // retry for transient send failures.
            const question_id = accept_peer.sendAcceptNoRestore(provision, embargo, heap, onHandoffAcceptReturn, true) catch |err| {
                if (pickup_settled) {
                    heap_owned = false;
                    log.debug("auto-pickup Accept settled before send returned trailing error: {}", .{err});
                    if (heap.accept_answer_id) |answer_id| {
                        heap.finishAcceptAnswer(accept_peer, answer_id, heap.accept_no_finish_needed);
                    }
                    heap.releaseDeferredFailedImports(accept_peer);
                    heap.deinitSelf();
                    return true;
                }
                return err;
            };
            if (pickup_settled) {
                heap_owned = false;
                heap.finishAcceptAnswer(accept_peer, question_id, heap.accept_no_finish_needed);
                heap.releaseDeferredFailedImports(accept_peer);
                heap.deinitSelf();
                return true;
            }
            heap.settled_flag = null;
            heap_owned = false;
            accept_peer.setQuestionDeinitCtx(question_id, HandoffPickupContext.deinitCtx);

            // (3) Emit the paired `context.accept` Disembargo on the promise path to
            // VatB. Best-effort AFTER the Accept is safely on the wire and ownership
            // has transferred: a failed Disembargo cannot roll back the sent Accept
            // (that would desync VatC's provide table), and VatC would then simply
            // hold the embargoed Accept until a later Finish/teardown drains it. We
            // therefore log rather than propagate.
            if (embargo) |embargo_bytes| {
                sendHandoffAcceptDisembargo(self, promise_id, embargo_bytes) catch |err| {
                    log.warn("auto-pickup: accept-disembargo send failed for promise {}: {}", .{ promise_id, err });
                };
            }

            log.debug("auto-pickup: sent Accept question={} for promise={} vine={} embargoed={}", .{
                question_id,
                promise_id,
                third.vine_id,
                embargo != null,
            });
            return true;
        }

        /// Length of an opaque accept-embargo byte id. A big-endian encoding of the
        /// per-peer `next_accept_embargo_id` counter: unique per handoff on this
        /// peer. The HOST keys embargoes per provision, so id collisions across
        /// recipients cannot co-drain — but the spec asks for globally-unique,
        /// entropy-rich ids (rpc.capnp:776-778), which the `EntropySource` path
        /// provides.
        const ACCEPT_EMBARGO_ID_LEN = 16;

        /// Fill `buf` with the next opaque accept-embargo byte id and return it:
        /// 16 random bytes when an entropy source is installed, else the legacy
        /// 8-byte big-endian per-peer counter (wasm32-freestanding and detached
        /// test peers run unchanged).
        pub fn nextAcceptEmbargoId(self: *Peer, buf: *[ACCEPT_EMBARGO_ID_LEN]u8) []const u8 {
            if (self.entropy) |source| {
                source.fill(source.ctx, buf[0..]);
                return buf[0..];
            }
            const id = self.next_accept_embargo_id;
            self.next_accept_embargo_id +%= 1;
            std.mem.writeInt(u64, buf[0..8], id, .big);
            return buf[0..8];
        }

        /// True if this peer has an outbound Call still in flight (Return not yet
        /// received) that targeted the promise import `promise_id`. The mark is set
        /// on the `Question` by `sendCall` (see `target_promise_import`) and cleared
        /// implicitly when the question leaves the table, so a live matching question
        /// is exactly an in-flight pipelined call against the promise.
        pub fn promiseImportHasInFlightCall(self: *Peer, promise_id: u32) bool {
            var it = self.questions.valueIterator();
            while (it.next()) |q| {
                if (q.cancelled) continue;
                if (q.target_promise_import == promise_id) return true;
            }
            return false;
        }

        /// Send a `Disembargo{context.accept}` on the promise path (this peer, the
        /// VatA↔VatB connection) targeting the promise import being handed off. VatB
        /// forwards it to VatC (see `handleAcceptDisembargo`/`forwardAcceptDisembargo`),
        /// which releases the matching held `Accept`. The target names the promise import
        /// (an entry in the sender's import table → the receiver's export table),
        /// matching how VatB keys its promise export.
        pub fn sendHandoffAcceptDisembargo(self: *Peer, promise_id: u32, embargo: []const u8) !void {
            const target = protocol.MessageTarget{
                .tag = .importedCap,
                .imported_cap = promise_id,
                .promised_answer = null,
            };
            var builder = protocol.MessageBuilder.init(self.allocator);
            defer builder.deinit();
            try builder.buildDisembargoAccept(target, embargo);
            try self.sendBuilder(&builder);
        }

        /// Return callback for the auto-pickup `Accept`. Runs on the third-vat peer
        /// (VatA↔VatC). Delivers the accepted direct capability to the application's
        /// pickup handler, then releases the vine on the promise peer (VatA↔VatB) —
        /// the wire signal that drives VatB's Provide Finish.
        pub fn onHandoffAcceptReturn(
            ctx_ptr: *anyopaque,
            accept_peer: *Peer,
            ret: protocol.Return,
            accept_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const ctx: *HandoffPickupContext = localCastCtx(*HandoffPickupContext, ctx_ptr);
            const defer_to_sender = ctx.settled_flag != null;
            ctx.accept_answer_id = ret.answer_id;
            ctx.accept_no_finish_needed = ret.no_finish_needed;
            defer if (!defer_to_sender) ctx.deinitSelf();
            defer if (!defer_to_sender) ctx.finishAcceptAnswer(accept_peer, ret.answer_id, ret.no_finish_needed);
            const promise_peer = ctx.promise_peer orelse {
                if (ctx.settled_flag) |flag| flag.* = true;
                return;
            };

            // Deliver the direct cap to the app FIRST (so it retains the accepted
            // import on `accept_peer` before we touch the vine), then release the
            // vine regardless of handler outcome — the pickup is complete on the wire
            // and the vine's job (holding the third-party cap alive until pickup) is
            // done. Releasing it drives VatB's Provide Finish.
            var handler_err: ?anyerror = null;
            ctx.user_cb(ctx.user_ctx, promise_peer, ctx.promise_id, accept_peer, ret, accept_caps) catch |err| {
                handler_err = err;
                log.debug("auto-pickup handler failed for promise {}: {}", .{ ctx.promise_id, err });
            };

            // The user callback is unrestricted and may close/deinit the promise
            // peer. Re-check the registered borrow before touching it again.
            if (ctx.promise_peer) |live_promise_peer| {
                live_promise_peer.deregisterHandoffPickup(ctx);
                ctx.promise_peer = null;
                if (ctx.vine_owned) {
                    ctx.vine_owned = false;
                    live_promise_peer.releaseImport(ctx.vine_id, 1) catch |err| {
                        log.debug("auto-pickup vine release failed for promise {}: {}", .{ ctx.promise_id, err });
                    };
                }
            }

            if (ctx.settled_flag) |flag| flag.* = true;
            if (handler_err) |err| {
                ctx.retainFailedUnretainedImports(accept_caps) catch |retain_err| {
                    log.debug("auto-pickup failed-handler deferred release capture failed for promise {}: {}", .{
                        ctx.promise_id,
                        retain_err,
                    });
                    return retain_err;
                };
                log.debug("auto-pickup handler error cleaned up for promise {}: {}", .{ ctx.promise_id, err });
                if (!defer_to_sender) ctx.releaseDeferredFailedImports(accept_peer);
            }
        }
    };
}
