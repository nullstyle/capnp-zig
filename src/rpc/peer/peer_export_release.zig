const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const events = @import("../events.zig");
const protocol = @import("../wire/protocol.zig");
const pending_calls = @import("../promises/pending_calls.zig");
const promises_promised_answer = @import("../promises/promised_answer.zig");
const peer_cap_lifecycle = @import("./peer_cap_lifecycle.zig");
const peer_inbound_release = @import("./peer_inbound_release.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");
const resolve = @import("./resolve.zig");
const state = @import("./state.zig");
const third_party = @import("./third_party.zig");

/// The cap-table refcount/release and frame-send family, extracted from
/// `peer/mod.zig` and made generic over the peer type (the JoinCoordinator
/// extraction contract): import Release sends (incl. the handoff-pin deferral
/// and host-forget variants), export/answer/promise/handoff reference notes
/// and rollbacks, export release finalization, inbound/result cap releases,
/// resolved-import storage and release, third-party Return buffering, and the
/// builder/frame control send paths. `peer/mod.zig` keeps every
/// caller-visible name as a thunk on `Peer` — including the FROZEN
/// `releaseImport` — so signatures, hook fn-types, and the api-snapshot
/// rendering are unchanged.
pub fn ExportRelease(comptime Peer: type) type {
    return struct {
        const PendingCall = state.PendingCall;
        const ExportEntry = @FieldType(@FieldType(Peer, "exports").KV, "value");
        const ResolvedImport = state.ResolvedImport;
        const ensureCountLimit = Peer.ensureCountLimit;
        const ensureByteLimit = Peer.ensureByteLimit;

        pub fn releaseImport(self: *Peer, import_id: u32, count: u32) anyerror!void {
            self.assertThreadAffinity();
            log.debug("releasing import id={} count={}", .{ import_id, count });
            try peer_cap_lifecycle.releaseImport(
                Peer,
                self,
                import_id,
                count,
                peer_cap_lifecycle.importRefCountForPeerFn(Peer),
                peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
                releaseResolvedImport,
                // The withhold seam: defers the Release while a handoff pin
                // lives (see sendReleaseDeferringHandoffPin).
                sendReleaseDeferringHandoffPin,
            );
        }

        /// Send a Release message on behalf of a host integration, bypassing the
        /// peer's own import tracking.
        pub fn sendReleaseForHost(self: *Peer, import_id: u32, count: u32) !void {
            self.assertThreadAffinity();
            try peer_outbound_control.sendReleaseViaSendFrame(
                Peer,
                self,
                import_id,
                count,
                Peer.sendFrameControl,
            );
        }

        /// Drop wire references the peer holds on an import WITHOUT sending a
        /// Release frame. Used by host integrations when ownership of the
        /// references transfers to the host (a relayed host-call Return with
        /// `releaseParamCaps = false`): the host keeps the remote capability
        /// alive and later sends its own Release, so the peer must forget its
        /// bookkeeping silently or the reference would be spent twice.
        pub fn forgetImportRefsForHost(self: *Peer, import_id: u32, count: u32) !void {
            self.assertThreadAffinity();
            try peer_cap_lifecycle.releaseImport(
                Peer,
                self,
                import_id,
                count,
                peer_cap_lifecycle.importRefCountForPeerFn(Peer),
                peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
                releaseResolvedImport,
                noopSendReleaseForForgottenImport,
            );
        }

        fn noopSendReleaseForForgottenImport(_: *Peer, _: u32, _: u32) anyerror!void {}

        /// Send a Finish message on behalf of a host integration, with explicit
        /// control over `releaseResultCaps` and `requireEarlyCancellation` flags.
        pub fn sendFinishForHost(
            self: *Peer,
            question_id: u32,
            release_result_caps: bool,
            require_early_cancellation: bool,
        ) !void {
            self.assertThreadAffinity();
            try peer_outbound_control.sendFinishWithFlagsViaSendFrame(
                Peer,
                self,
                question_id,
                release_result_caps,
                require_early_cancellation,
                Peer.sendFrameControl,
            );
        }

        pub fn sendBuilder(self: *Peer, builder: *protocol.MessageBuilder) !void {
            const bytes = try builder.finish();
            defer self.allocator.free(bytes);
            try sendFrame(self, bytes);
        }

        pub fn sendBuilderControl(self: *Peer, builder: *protocol.MessageBuilder) !void {
            const bytes = try builder.finish();
            defer self.allocator.free(bytes);
            try sendFrameControl(self, bytes);
        }

        /// Send a protocol-mandated frame (Return, Finish, Release, Resolve,
        /// Disembargo, Abort, Unimplemented).
        ///
        /// Unlike caller-initiated sends — where a full write queue surfaces
        /// `error.WriteQueueFull` / `error.WriteQueueBytesExceeded` to the
        /// caller and the connection stays healthy — dropping a control frame
        /// desynchronizes protocol state with the remote. The only safe
        /// recovery is closing the connection, so enqueue overflow here emits
        /// a peer-level backpressure event and initiates transport close.
        pub fn sendFrameControl(self: *Peer, frame: []const u8) !void {
            sendFrame(self, frame) catch |err| {
                switch (err) {
                    error.WriteQueueFull, error.WriteQueueBytesExceeded => {
                        events.emitBackpressure(
                            self.observer,
                            .peer,
                            .unknown,
                            if (err == error.WriteQueueFull) .write_queue_items else .write_queue_bytes,
                            frame.len,
                            null,
                            err,
                        );
                        log.warn("control frame dropped by saturated write queue; closing connection", .{});
                        self.closeAttachedTransport();
                    },
                    else => {},
                }
                return err;
            };
        }

        pub fn sendFrame(self: *Peer, frame: []const u8) !void {
            if (self.send_frame_override) |cb| {
                const ctx = self.send_frame_ctx orelse {
                    log.debug("send frame override missing callback context", .{});
                    return error.MissingCallbackContext;
                };
                try cb(ctx, frame);
                events.emitFrame(self.observer, .peer, .unknown, .enqueued, frame.len);
                return;
            }
            self.transport.sendFrame(frame) catch |err| {
                if (err == error.TransportNotAttached) {
                    log.debug("cannot send frame: transport not attached", .{});
                }
                return err;
            };
            events.emitFrame(self.observer, .peer, .unknown, .enqueued, frame.len);
        }

        pub fn onOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) anyerror!void {
            const peer: *Peer = @ptrCast(@alignCast(ctx));
            switch (tag) {
                .senderHosted, .senderPromise => try peer.noteExportRef(id),
                else => {},
            }
        }

        pub fn rollbackOutboundCap(ctx: *anyopaque, tag: protocol.CapDescriptorTag, id: u32) void {
            const peer: *Peer = @ptrCast(@alignCast(ctx));
            switch (tag) {
                .senderHosted, .senderPromise => peer.rollbackExportRef(id),
                else => {},
            }
        }

        pub fn noteExportRef(self: *Peer, id: u32) !void {
            try peer_cap_lifecycle.noteExportRef(
                ExportEntry,
                &self.exports,
                id,
            );
        }

        pub fn rollbackExportRef(self: *Peer, id: u32) void {
            var entry = self.exports.getEntry(id) orelse return;
            if (entry.value_ptr.ref_count == 0) return;
            entry.value_ptr.ref_count -= 1;
        }

        pub fn noteAnswerExportRef(self: *Peer, id: u32) !void {
            try peer_cap_lifecycle.noteAnswerExportRef(
                ExportEntry,
                &self.exports,
                id,
            );
        }

        pub fn rollbackAnswerExportRef(self: *Peer, id: u32) void {
            var entry = self.exports.getEntry(id) orelse return;
            if (entry.value_ptr.answer_ref_count == 0) return;
            entry.value_ptr.answer_ref_count -= 1;
        }

        pub fn notePromiseExportRef(self: *Peer, id: u32) !void {
            try peer_cap_lifecycle.notePromiseExportRef(
                ExportEntry,
                &self.exports,
                id,
            );
        }

        /// Take one handoff-held (Release-immune) reference on an export. See the
        /// `handoff_ref_count` field doc on `ExportEntry`.
        pub fn noteHandoffExportRef(self: *Peer, id: u32) !void {
            try peer_cap_lifecycle.noteHandoffExportRef(
                ExportEntry,
                &self.exports,
                id,
            );
        }

        /// Release one handoff-held reference; destroys the entry only when every
        /// ref class is zero AND the entry was wire-granted at least once (so a
        /// Provide+Finish cycle can never destroy an app-held, never-emitted
        /// export). Best-effort by design: unknown id / underflow are logged.
        pub fn releaseHandoffHeldExport(self: *Peer, id: u32) void {
            const promise_target = promiseTargetOf(self, id);
            const import_target = promiseImportTargetOf(self, id);
            peer_cap_lifecycle.releaseHandoffHeldExport(
                Peer,
                ExportEntry,
                PendingCall,
                self,
                self.allocator,
                &self.exports,
                &self.pending_export_promises,
                self.bootstrap_export_id,
                id,
                peer_cap_lifecycle.clearExportForPeerFn(Peer),
                pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
            );
            finalizeExportRelease(self, id, promise_target, import_target);
        }

        /// Take one handoff pin on one of this peer's IMPORTS (the receiverHosted
        /// lift). See `CapTable.noteHandoffImportPin` for the retention/withhold
        /// contract.
        pub fn noteHandoffImportPin(self: *Peer, id: u32) !void {
            try self.caps.noteHandoffImportPin(id);
        }

        /// Roll back a handoff import pin taken by an OOM ladder that has NOT yet
        /// transferred ownership (mirror of `rollbackHandoffExportRef`, import
        /// table).
        pub fn rollbackHandoffImportPin(self: *Peer, id: u32) void {
            self.caps.rollbackHandoffImportPin(id);
        }

        /// Release one handoff pin on import `id`. When the LAST pin drops this
        /// emits the withheld (deferred) `Release` via the RAW sender —
        /// deliberately NOT `Peer.releaseImport`/the generic helper, which
        /// early-returns with released == 0 before any send once the local wire
        /// count is already 0 — and then performs the removal + resolved-import
        /// cleanup that retention-under-pin deferred (`CapTable.releaseImport`
        /// returns false for a pin-retained drain, so the generic's
        /// `release_resolved_import` hook never fired). The deferred tally is
        /// taken (zeroed) BEFORE the send, so a failed emission can never be
        /// re-emitted stale by a second pin/unpin cycle.
        ///
        /// FAILURE POSTURE: fallible. Non-deinit callers must PROPAGATE — a lost
        /// deferred Release breaks granted == released at the introducer.
        /// Teardown/deinit callers catch-log under the documented deinit
        /// best-effort exception: the connection is dying and the remote
        /// reconciles at disconnect.
        pub fn releaseHandoffImportPin(self: *Peer, id: u32) anyerror!void {
            const unpin = self.caps.releaseHandoffImportPin(id);
            if (!unpin.last_pin_released) return;
            if (unpin.deferred_release > 0) {
                try peer_outbound_control.sendReleaseViaSendFrame(
                    Peer,
                    self,
                    id,
                    unpin.deferred_release,
                    Peer.sendFrameControl,
                );
            }
            if (self.caps.removeImportIfFullyReleased(id)) {
                try releaseResolvedImport(self, id);
            }
        }

        /// The `send_release` binding for BOTH generic import-release walks
        /// (`Peer.releaseImport` and the inbound-cap release in
        /// `releaseInboundCaps`): WITHHOLDS the Release frame while the import is
        /// pinned by a live handoff (`handoff_pin_count > 0`), accumulating the
        /// released count into the entry's `deferred_release` for
        /// `releaseHandoffImportPin` to emit at the last unpin. A receiverHosted
        /// provide target is a capability this vat merely imports and its
        /// descriptor granted the introducer nothing to hold on our behalf, so an
        /// eagerly-emitted Release here would let the introducer destroy the very
        /// capability a pending Accept still has to reach. An import with no
        /// handoff pin — including one alive on `promise_ref_count` alone — still
        /// releases eagerly: today's behaviour, bit for bit. The raw senders stay
        /// eager on purpose: `sendReleaseForHost` (a deliberate host bypass) and
        /// `releaseAllImports` (the deinit sweep) must never defer.
        fn sendReleaseDeferringHandoffPin(self: *Peer, import_id: u32, count: u32) anyerror!void {
            if (try self.caps.deferReleaseWhilePinned(import_id, count)) return;
            try peer_outbound_control.sendReleaseViaSendFrame(
                Peer,
                self,
                import_id,
                count,
                Peer.sendFrameControl,
            );
        }

        pub fn rollbackPromiseExportRef(self: *Peer, id: u32) void {
            var entry = self.exports.getEntry(id) orelse return;
            if (entry.value_ptr.promise_ref_count == 0) return;
            entry.value_ptr.promise_ref_count -= 1;
        }

        /// The resolution-target export id of a promise export that has resolved to
        /// a concrete export, or null for any other export (unresolved, resolved to
        /// an exception, or a plain hosted cap). Only such a resolution takes a
        /// promise-held ref on its target (see `resolvePromiseExportToExport`), so
        /// this is exactly the set of exports that must release one when destroyed.
        fn promiseTargetOf(self: *Peer, id: u32) ?u32 {
            const entry = self.exports.getEntry(id) orelse return null;
            const resolved = entry.value_ptr.resolved orelse return null;
            return switch (resolved) {
                .exported => |cap| cap.id,
                else => null,
            };
        }

        /// The resolution-target IMPORT id of a promise export that has resolved to
        /// an imported cap (the remote's own export), or null otherwise. Only
        /// `resolvePromiseExportToImport` produces such a resolution, and it takes a
        /// promise-held ref on that import; this is exactly the set of exports that
        /// must release one when destroyed. Parallel to `promiseTargetOf`, but for
        /// the import table rather than the export table.
        fn promiseImportTargetOf(self: *Peer, id: u32) ?u32 {
            const entry = self.exports.getEntry(id) orelse return null;
            const resolved = entry.value_ptr.resolved orelse return null;
            return switch (resolved) {
                .imported => |cap| cap.id,
                else => null,
            };
        }

        /// Called after a release lowered one of export `id`'s ref classes. If that
        /// dropped `id` out of the export table, free its persistence state and —
        /// when `id` was a promise that had resolved — release the promise-held ref
        /// it pinned on its resolution target: `promise_target` for an exported
        /// target (cascading destruction down a resolution chain), or `import_target`
        /// for an imported target (dropping the promise-held import pin). At most one
        /// is set, since a resolution is either exported or imported. Both must be
        /// captured *before* the release (the entry, and its `resolved` field, is
        /// gone by the time we get here).
        pub fn finalizeExportRelease(self: *Peer, id: u32, promise_target: ?u32, import_target: ?u32) void {
            if (self.exports.contains(id)) return;
            self.dropPersistenceStateForRemovedExport(id);
            if (promise_target) |target_id| {
                if (target_id != id) {
                    releasePromiseHeldCap(self, target_id) catch |err| {
                        log.warn("cascade promise-held release for export {} failed: {}", .{ target_id, err });
                    };
                }
            }
            if (import_target) |import_id| {
                // Drop the promise-held import pin this destroyed promise export
                // took in resolvePromiseExportToImport. This is a LOCAL lease, not a
                // wire reference: releasing it never sends a Release. The import's
                // own wire references are owned and released separately by whoever
                // received it as a call/return cap.
                _ = self.caps.releasePromiseImportRef(import_id);
            }
        }

        /// Release one promise-held reference on export `id`, taken by
        /// `resolvePromiseExportToExport` when a promise export resolved to `id` as
        /// its target. Destroys the export once no wire, answer, or promise
        /// references remain, cascading if `id` was itself a resolved promise.
        pub fn releasePromiseHeldCap(self: *Peer, id: u32) !void {
            const promise_target = promiseTargetOf(self, id);
            const import_target = promiseImportTargetOf(self, id);
            try peer_cap_lifecycle.releasePromiseHeldExport(
                Peer,
                ExportEntry,
                PendingCall,
                self,
                self.allocator,
                &self.exports,
                &self.pending_export_promises,
                self.bootstrap_export_id,
                id,
                peer_cap_lifecycle.clearExportForPeerFn(Peer),
                pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
            );
            finalizeExportRelease(self, id, promise_target, import_target);
        }

        /// Release one answer-held reference on export `id` (the `count` from the
        /// releaseResultCaps-style frame walk is always 1 per descriptor).
        fn releaseAnswerHeldCap(self: *Peer, id: u32, count: u32) !void {
            _ = count;
            const promise_target = promiseTargetOf(self, id);
            const import_target = promiseImportTargetOf(self, id);
            try peer_cap_lifecycle.releaseAnswerHeldExport(
                Peer,
                ExportEntry,
                PendingCall,
                self,
                self.allocator,
                &self.exports,
                &self.pending_export_promises,
                self.bootstrap_export_id,
                id,
                peer_cap_lifecycle.clearExportForPeerFn(Peer),
                pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
            );
            finalizeExportRelease(self, id, promise_target, import_target);
        }

        pub fn releaseExport(self: *Peer, id: u32, count: u32) !void {
            const promise_target = promiseTargetOf(self, id);
            const import_target = promiseImportTargetOf(self, id);
            try peer_cap_lifecycle.releaseExport(
                Peer,
                ExportEntry,
                PendingCall,
                self,
                self.allocator,
                &self.exports,
                &self.pending_export_promises,
                self.bootstrap_export_id,
                id,
                count,
                peer_cap_lifecycle.clearExportForPeerFn(Peer),
                pending_calls.deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall),
            );
            finalizeExportRelease(self, id, promise_target, import_target);
        }

        pub fn releaseInboundCaps(self: *Peer, inbound: *cap_table.InboundCapTable) !void {
            try peer_inbound_release.releaseInboundCaps(
                Peer,
                self.allocator,
                self,
                inbound,
                peer_cap_lifecycle.releaseImportRefForPeerFn(Peer),
                releaseResolvedImport,
                // The withhold seam: defers the Release while a handoff pin
                // lives (see sendReleaseDeferringHandoffPin).
                sendReleaseDeferringHandoffPin,
            );
        }

        pub fn storeResolvedImport(
            self: *Peer,
            promise_id: u32,
            cap: ?cap_table.ResolvedCap,
            embargo_id: ?u32,
            embargoed: bool,
        ) !void {
            const resolved_before = self.resolved_imports.count();
            try ensureCountLimit(
                self.resolved_imports.contains(promise_id),
                resolved_before,
                self.limits.max_resolved_imports,
            );
            try peer_cap_lifecycle.storeResolvedImport(
                Peer,
                ResolvedImport,
                cap_table.ResolvedCap,
                self,
                &self.resolved_imports,
                &self.pending_embargoes,
                promise_id,
                cap,
                embargo_id,
                embargoed,
                Peer.releaseResolvedCap,
            );
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .resolved_imports,
                resolved_before,
                self.resolved_imports.count(),
                self.limits.max_resolved_imports,
            );
        }

        pub fn rememberPendingEmbargo(self: *Peer, embargo_id: u32, promise_id: u32) !void {
            try ensureCountLimit(
                self.pending_embargoes.contains(embargo_id),
                self.pending_embargoes.count(),
                self.limits.max_pending_embargoes,
            );
            try resolve.rememberPendingEmbargoForPeer(Peer, self, embargo_id, promise_id);
        }

        pub fn releaseResolvedImport(self: *Peer, promise_id: u32) anyerror!void {
            try peer_cap_lifecycle.releaseResolvedImport(
                Peer,
                ResolvedImport,
                cap_table.ResolvedCap,
                self,
                &self.resolved_imports,
                &self.pending_embargoes,
                promise_id,
                Peer.releaseResolvedCap,
            );
        }

        pub fn bufferPendingThirdPartyReturn(self: *Peer, answer_id: u32, frame: []const u8) !void {
            try ensureCountLimit(
                self.pending_third_party_returns.contains(answer_id),
                self.pending_third_party_returns.count(),
                self.limits.max_pending_third_party_returns,
            );
            try ensureByteLimit(
                self.pendingThirdPartyReturnBytesExcluding(answer_id),
                frame.len,
                self.limits.max_pending_third_party_return_bytes,
            );
            try third_party.returns.bufferPendingReturnForPeer(Peer, self, answer_id, frame);
        }

        pub fn handleMissingReturnQuestion(self: *Peer, frame: []const u8, answer_id: u32) !void {
            try third_party.handleMissingReturnQuestion(
                Peer,
                self,
                frame,
                answer_id,
                third_party.isThirdPartyAnswerId,
                third_party.returns.hasPendingReturnForPeerFn(Peer),
                bufferPendingThirdPartyReturn,
            );
        }

        pub fn releaseResolvedCap(self: *Peer, resolved: cap_table.ResolvedCap) anyerror!void {
            switch (resolved) {
                .imported => |cap| try releaseImport(self, cap.id, 1),
                else => {},
            }
        }

        pub fn deliverLoopbackReturn(self: *Peer, frame: []const u8) !void {
            var decoded = try protocol.DecodedMessage.init(self.allocator, frame);
            defer decoded.deinit();
            if (decoded.tag != .@"return") return error.UnexpectedMessage;
            try self.handleReturn(frame, try decoded.asReturn());
        }

        /// Re-resolve a stored (ops-based) provide target against this peer's own
        /// resolved-answer table — the id is consumed only on the peer whose
        /// answer space it names; nothing reader-backed is constructed from ops.
        pub fn resolveProvidePromisedOps(self: *Peer, question_id: u32, ops: []const protocol.PromisedAnswerOp) !cap_table.ResolvedCap {
            const entry = self.resolved_answers.get(question_id) orelse return error.PromiseUnresolved;
            var decoded = try protocol.DecodedMessage.init(self.allocator, entry.frame);
            defer decoded.deinit();
            const ret = try decoded.asReturn();
            if (ret.tag != .results or ret.results == null) return error.PromisedAnswerMissing;
            return switch (try promises_promised_answer.resolvePromisedAnswerOps(ret.results.?, ops)) {
                .none => .none,
                .exported_id => |id| .{ .exported = .{ .id = id } },
                .imported_id => |id| .{ .imported = .{ .id = id } },
                .promised => |promised| .{ .promised = promised },
            };
        }

        pub fn resolvePromisedAnswer(self: *Peer, promised: protocol.PromisedAnswer) !cap_table.ResolvedCap {
            const entry = self.resolved_answers.get(promised.question_id) orelse return error.PromiseUnresolved;
            var decoded = try protocol.DecodedMessage.init(self.allocator, entry.frame);
            defer decoded.deinit();
            const ret = try decoded.asReturn();
            if (ret.tag != .results or ret.results == null) return error.PromisedAnswerMissing;
            return cap_table.resolvePromisedAnswer(ret.results.?, promised.transform);
        }

        pub fn releaseResultCaps(self: *Peer, frame: []const u8) !void {
            try peer_cap_lifecycle.releaseResultCaps(
                Peer,
                self,
                self.allocator,
                frame,
                releaseExport,
            );
        }

        /// Release the answer-held references a recorded resolved answer took on
        /// the exports in its results (see reserveResolvedAnswer). Same frame
        /// walk as releaseResultCaps, but spending answer references, which an
        /// inbound Release message can never touch.
        pub fn releaseAnswerHeldResultCaps(self: *Peer, frame: []const u8) !void {
            try peer_cap_lifecycle.releaseResultCaps(
                Peer,
                self,
                self.allocator,
                frame,
                releaseAnswerHeldCap,
            );
        }
    };
}
