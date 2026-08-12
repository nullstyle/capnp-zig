const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const resolve = @import("./resolve.zig");
const third_party = @import("./third_party.zig");
const peer_outbound_control = @import("./peer_outbound_control.zig");

/// Promise-export resolution senders, extracted from `peer/mod.zig` and made
/// generic over the peer type (the JoinCoordinator extraction contract):
/// resolve a promise export to a concrete export, an import (reflected
/// resolve), a third party (origination + retained-answer form), or an
/// exception. `peer/mod.zig` keeps every caller-visible name as a thunk on
/// `Peer` — including the FROZEN resolvePromiseExportToExport /
/// resolvePromiseExportToException entries.
pub fn PromiseExports(comptime Peer: type) type {
    return struct {
        const ProvideTarget = state.ProvideTarget;
        const ProvideHandle = Peer.ProvideHandleRecord;
        const ProvideOriginationTarget = Peer.ProvideOriginationTargetRecord;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        /// Resolve a previously exported promise to point at a concrete export.
        pub fn resolvePromiseExportToExport(self: *Peer, promise_id: u32, export_id: u32) !void {
            self.assertThreadAffinity();
            var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
            if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
            if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
            if (!self.exports.contains(export_id)) return error.UnknownExport;

            const descriptor = protocol.CapDescriptor{
                .tag = if (self.caps.isExportPromise(export_id)) .senderPromise else .senderHosted,
                .id = export_id,
                .promised_answer = null,
                .attached_fd = null,
            };

            // The Resolve's cap descriptor hands the remote a reference to the
            // target export, exactly like a call/return descriptor would: the
            // remote spends it with a wire Release once it drops the resolved cap
            // (see resolveDescriptor's noteImport in caps/inbound.zig), or — if
            // it does not implement Resolve — echoes the message back inside an
            // Unimplemented and handleUnimplementedResolve releases it there.
            // Rolled back only while the send can still fail: past a successful
            // send the remote holds the descriptor and owns the ref (matching
            // handleBootstrap's pattern).
            try self.noteExportRef(export_id);
            var rollback_wire_ref = true;
            errdefer if (rollback_wire_ref) self.rollbackExportRef(export_id);

            // Pin the resolution target for the promise export's own lifetime. Once
            // the promise routes inbound calls at `export_id` (via
            // replayResolvedPromiseExport / handleResolvedExportedCall below), an
            // inbound Release that zeroes the target's wire ref_count — or the
            // echoed-Unimplemented cleanup above, which also spends a wire ref —
            // must not destroy the target out from under the still-live promise
            // export. This promise-held reference is separate from the wire ref
            // taken above (which the remote owns and spends): it is released only
            // when the promise export is itself destroyed (see
            // finalizeExportRelease / releasePromiseHeldCap). Rolled back only
            // while the send can still fail; past a committed resolution the
            // promise owns it and its destruction releases it.
            try self.notePromiseExportRef(export_id);
            var rollback_promise_ref = true;
            errdefer if (rollback_promise_ref) self.rollbackPromiseExportRef(export_id);

            try peer_outbound_control.sendResolveCapViaSendFrame(
                Peer,
                self,
                promise_id,
                descriptor,
                Peer.sendFrame,
            );
            rollback_wire_ref = false;

            promise_entry.value_ptr.resolved = .{ .exported = .{ .id = export_id } };
            // The resolution is committed: the promise export now routes to
            // `export_id` and its eventual destruction releases this promise-held
            // ref, so keep it even if replay below fails.
            rollback_promise_ref = false;
            self.caps.clearExportPromise(promise_id);
            try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
        }

        /// Resolve a previously exported promise to a capability the *remote* peer
        /// hosts (one we hold as an import). This is the "reflected capability"
        /// case: the promise resolves to a cap reached by a different path than the
        /// promise itself, so a conformant remote will run the embargo/Disembargo
        /// handshake against it. Mirror of `resolvePromiseExportToExport`, but the
        /// resolution target lives in the import table, not the export table.
        pub fn resolvePromiseExportToImport(self: *Peer, promise_id: u32, import_id: u32) !void {
            self.assertThreadAffinity();
            var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
            if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
            if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
            if (!self.caps.hasImport(import_id)) return error.UnknownImport;

            // `receiverHosted` names the REMOTE's own export (our import), so — unlike
            // resolvePromiseExportToExport's senderHosted/senderPromise descriptor —
            // this does NOT hand the remote a fresh wire reference on one of OUR
            // exports. The remote already owns `import_id`; it will not spend a
            // Release against us on account of receiving it in the Resolve. So we
            // take NO outbound wire ref here.
            const descriptor = protocol.CapDescriptor{
                .tag = .receiverHosted,
                .id = import_id,
                .promised_answer = null,
                .attached_fd = null,
            };

            // Pin the resolution target (our import) for the promise export's own
            // lifetime. Once the promise routes inbound calls back out to
            // `import_id` (via replayResolvedPromiseExport / forwardResolvedCall
            // below), the host handler that originally received this import as a
            // call/return cap may drop its own wire reference before the promise
            // export is gone — an inbound path that zeroes the import's wire
            // `ref_count` must not remove the import entry out from under the
            // still-live promise export. This is a purely LOCAL pin
            // (`promise_ref_count`), NOT a `noteImport`: a `receiverHosted`
            // descriptor creates no new wire reference, so dropping this pin must
            // never send a wire Release. It is released when the promise export is
            // destroyed (see finalizeExportRelease's import-target cascade, which
            // calls releasePromiseImportRef — local, no frame). Rolled back only
            // while the send can still fail; past a committed resolution the promise
            // owns it.
            try self.caps.notePromiseImportRef(import_id);
            var rollback_import_ref = true;
            errdefer if (rollback_import_ref) {
                _ = self.caps.releasePromiseImportRef(import_id);
            };

            try peer_outbound_control.sendResolveCapViaSendFrame(
                Peer,
                self,
                promise_id,
                descriptor,
                Peer.sendFrame,
            );

            promise_entry.value_ptr.resolved = .{ .imported = .{ .id = import_id } };
            // The resolution is committed: the promise export now routes to
            // `import_id` and its eventual destruction drops this promise-held pin
            // (no wire Release), so keep it even if replay below fails.
            rollback_import_ref = false;
            self.caps.clearExportPromise(promise_id);
            try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);
        }

        /// Resolve a previously exported promise to a capability hosted by a THIRD
        /// vat (Level-3 three-party handoff ORIGINATION). Mirror of
        /// `resolvePromiseExportToImport`, but the resolution target lives on neither
        /// this peer's export table nor its import table — it is hosted by VatC, so
        /// the Resolve carries a `thirdPartyHosted{ id = ThirdPartyToContact, vineId }`
        /// descriptor and this vat originates the paired `Provide` to VatC.
        ///
        /// `self` is the host-of-recipient connection (VatB↔VatA), where the promise
        /// export lives, the vine is minted, and the Resolve is sent. `provide_peer`
        /// is the host-of-provided-cap connection (VatB↔VatC), where the held-open
        /// `Provide` is sent naming `provided_target` (VatC's Carol) and `recipient`
        /// (the `ThirdPartyToAwait`). `contact_payload` is the serialized
        /// `ThirdPartyToContact` VatA will redeem via its VatNetwork to reach VatC.
        ///
        /// This slice sends the Resolve with `embargo = null` semantics: there is no
        /// in-flight-promise embargo/disembargo during the handoff (Phase 4).
        ///
        /// Returns the `ProvideHandle` (Provide question id + vine id). The vine's
        /// wire reference is held by the emitted `thirdPartyHosted` descriptor; when
        /// VatA releases the vine (after picking up Carol directly), `handleRelease`
        /// Finishes the Provide on `provide_peer`.
        pub fn resolvePromiseExportToThirdParty(
            self: *Peer,
            promise_id: u32,
            provide_peer: *Peer,
            provided_target: protocol.MessageTarget,
            recipient: message.AnyPointerReader,
            contact_payload: []const u8,
        ) !ProvideHandle {
            return resolvePromiseExportToThirdPartyInternal(
                self,
                promise_id,
                provide_peer,
                .{ .message_target = provided_target },
                recipient,
                contact_payload,
            );
        }

        /// Resolve an exported promise through a Level-3 handoff whose provided
        /// target is an open, explicitly retained answer on `provide_peer`.
        /// `ops` selects the capability within that answer. A successful Provide
        /// transfers the answer lifetime into the vine coupling; callers must not
        /// subsequently Finish it directly.
        pub fn resolvePromiseExportToThirdPartyFromRetainedAnswer(
            self: *Peer,
            promise_id: u32,
            provide_peer: *Peer,
            retained_question_id: u32,
            ops: []const protocol.PromisedAnswerOp,
            recipient: message.AnyPointerReader,
            contact_payload: []const u8,
        ) !ProvideHandle {
            return resolvePromiseExportToThirdPartyInternal(
                self,
                promise_id,
                provide_peer,
                .{ .retained_answer = .{
                    .question_id = retained_question_id,
                    .ops = ops,
                } },
                recipient,
                contact_payload,
            );
        }

        pub fn resolvePromiseExportToThirdPartyInternal(
            self: *Peer,
            promise_id: u32,
            provide_peer: *Peer,
            provided_target: ProvideOriginationTarget,
            recipient: message.AnyPointerReader,
            contact_payload: []const u8,
        ) !ProvideHandle {
            self.assertThreadAffinity();
            provide_peer.assertThreadAffinity();
            if (self.is_shutting_down or provide_peer.is_shutting_down) return error.PeerShuttingDown;

            {
                // Validate the promise export up front, but do NOT hold the entry
                // pointer across `sendProvide`: it mints the vine into `self.exports`,
                // which can rehash and invalidate any captured entry pointer. Re-fetch
                // after origination (below) before mutating the promise entry.
                const promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
                if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
                if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;
            }

            // Originate the handoff: mint the vine on `self`, mark it
            // third-party-hosted with the contact, couple vine → held-open Provide
            // on `provide_peer`, and send the Provide to VatC. `sendProvide` rolls
            // back all of that on any failure before it returns.
            const retained_source_question_id: ?u32 = switch (provided_target) {
                .message_target => null,
                .retained_answer => |retained| retained.question_id,
            };
            const handle = switch (provided_target) {
                .message_target => |target| try provide_peer.sendProvide(target, recipient, self, contact_payload),
                .retained_answer => |retained| try provide_peer.sendProvideFromRetainedAnswer(
                    retained.question_id,
                    retained.ops,
                    recipient,
                    self,
                    contact_payload,
                ),
            };
            // Undo the whole origination if the Resolve emission below fails: destroy
            // the vine export + its handoff mark, drop the coupling, and Finish the
            // Provide we just sent so VatC does not leak the provision.
            var origination_owned = true;
            errdefer if (origination_owned) {
                if (self.outbound_provides.fetchRemove(handle.vine_id)) |removed| {
                    var op = removed.value;
                    op.deinit(self.allocator);
                }
                self.caps.clearThirdPartyHosted(handle.vine_id);
                self.releaseVineExport(handle.vine_id);
                // Drop the liveness back-link `sendProvide` registered on the
                // provide_peer (BUG #55) so unwinding leaves no stale coupling.
                provide_peer.deregisterCoupledVine(self, handle.vine_id);
                self.finishOriginatedProvide(provide_peer, handle.question_id, retained_source_question_id);
            };

            // Build the Resolve's resolved descriptor: thirdPartyHosted{ id, vineId }.
            // The recipient takes a wire reference on the VINE (the handoff anchor),
            // exactly as the payload emitter does (caps/outbound.zig), so account for
            // that reference here — buildResolveCap does NOT run the outbound-cap
            // callback that would otherwise note it.
            var contact_msg = try message.Message.initUnvalidated(self.allocator, contact_payload);
            defer contact_msg.deinit();
            const contact = try contact_msg.getRootAnyPointer();
            const descriptor = protocol.CapDescriptor{
                .tag = .thirdPartyHosted,
                .id = null,
                .promised_answer = null,
                .third_party = .{ .id = contact, .vine_id = handle.vine_id },
                .attached_fd = null,
            };

            try self.noteExportRef(handle.vine_id);
            var rollback_wire_ref = true;
            errdefer if (rollback_wire_ref) self.rollbackExportRef(handle.vine_id);

            // Record the resolving promise export on the coupling BEFORE emitting the
            // Resolve. Two reasons: (1) the vine teardown clears the promise's
            // resolution target (see handleRelease); (2) — critically for Phase 4 —
            // emitting the Resolve can synchronously drive the recipient's auto-pickup
            // to send a `context.accept` Disembargo straight back to us on the promise
            // path (single-threaded loopback), and `handleAcceptDisembargo` must find
            // this coupling by `resolved_promise_export_id` to forward the Disembargo
            // on to the capability host. If we set it only after the Resolve returns,
            // a synchronous Disembargo would find no coupling and never reach VatC.
            if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
                op.resolved_promise_export_id = promise_id;
            }
            errdefer if (origination_owned) {
                if (self.outbound_provides.getPtr(handle.vine_id)) |op| {
                    op.resolved_promise_export_id = null;
                }
            };

            try peer_outbound_control.sendResolveCapViaSendFrame(
                Peer,
                self,
                promise_id,
                descriptor,
                Peer.sendFrame,
            );
            rollback_wire_ref = false;
            origination_owned = false;

            // The Resolve is on the wire: the recipient's synchronous auto-pickup
            // may already have sent back a `context.accept` Disembargo, which
            // `handleAcceptDisembargo` STASHED on the coupling (e-order: it must
            // not reach the host before the parked-call replay below). From here,
            // EVERY exit — including the two error returns below — must flush the
            // stash, or the recipient's embargoed Accept hangs forever.
            errdefer self.flushStashedAcceptDisembargo(handle.vine_id);

            // Re-fetch the promise entry: the vine insert above may have rehashed
            // `self.exports`, invalidating the pointer captured during validation.
            var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;

            // With no in-flight promise call, direct auto-pickup can synchronously
            // Accept and Release the vine inside the Resolve send above. In that
            // case `handleRelease` has already drained the coupling and destroyed
            // the vine; never publish a resolved export pointing at that dead id.
            // The remote recipient already owns the direct cap, so locally settle
            // the obsolete promise export to none and fail any impossible late
            // parked calls cleanly.
            if (!self.outbound_provides.contains(handle.vine_id) or
                !self.exports.contains(handle.vine_id))
            {
                promise_entry.value_ptr.resolved = .none;
                self.caps.clearExportPromise(promise_id);
                try self.replayResolvedPromiseExport(promise_id, .none);
                self.flushStashedAcceptDisembargo(handle.vine_id);
                log.debug("resolved promise export {} completed during synchronous third-party pickup", .{promise_id});
                return handle;
            }

            // Route pipelined calls that arrive on the promise export through the
            // vine. The coupling owns either the imported or promised-answer target,
            // so replay FORWARDS each parked call to VatC over the B↔C connection
            // (issue #56, `maybeForwardVineCall`); only a coupling whose provide peer
            // has torn down falls back to the vine's rejecting handler. NO
            // promise-held pin is taken on
            // the vine: its lifetime is driven solely by VatA's wire Release (the
            // handoff-completion signal), which must destroy it to Finish the Provide.
            // A pin would keep the vine alive past that Release and stall the Finish.
            promise_entry.value_ptr.resolved = .{ .exported = .{ .id = handle.vine_id } };
            self.caps.clearExportPromise(promise_id);
            try self.replayResolvedPromiseExport(promise_id, promise_entry.value_ptr.resolved.?);

            // Parked pre-resolution calls are now on their way to the host; the
            // recipient's Disembargo (if one was stashed during the Resolve send)
            // may follow them. This also arms immediate forwarding for any later
            // Disembargo on this coupling.
            self.flushStashedAcceptDisembargo(handle.vine_id);

            log.debug("resolved promise export {} to third party via vine {}", .{ promise_id, handle.vine_id });
            return handle;
        }

        /// Resolve a previously exported promise to an exception.
        pub fn resolvePromiseExportToException(self: *Peer, promise_id: u32, reason: []const u8) !void {
            self.assertThreadAffinity();
            var promise_entry = self.exports.getEntry(promise_id) orelse return error.UnknownExport;
            if (!promise_entry.value_ptr.is_promise) return error.ExportIsNotPromise;
            if (promise_entry.value_ptr.resolved != null) return error.PromiseAlreadyResolved;

            try peer_outbound_control.sendResolveExceptionViaSendFrame(
                Peer,
                self,
                promise_id,
                reason,
                Peer.sendFrame,
            );
            promise_entry.value_ptr.resolved = .none;
            self.caps.clearExportPromise(promise_id);
            try self.replayResolvedPromiseExport(promise_id, .none);
        }
    };
}
