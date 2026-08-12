const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../../caps/table.zig");
const protocol = @import("../../wire/protocol.zig");
const peer_call_sender = @import("./peer_call_sender.zig");
const retained_question_state = @import("../retained_questions.zig");
const provide_forward_target = @import("../provide/forward_target.zig");
const state = @import("../state.zig");

/// The outbound Call send family, extracted from `peer/mod.zig` and made
/// generic over the peer type (the JoinCoordinator extraction contract): the
/// 15 sendCall* overloads (kept as-is — consolidation is an API-design
/// question, deliberately out of scope) plus the forwarded L3 vine-call pair
/// (issue #56 machinery: `restore_on_return_error = false` because the relay
/// ctx is freed by its return callback). `peer/mod.zig` keeps every
/// caller-visible name as a thunk on `Peer`, so the frozen sendCall entry
/// points, generated-code callers, and the api-snapshot rendering are
/// unchanged.
pub fn CallSend(comptime Peer: type) type {
    return struct {
        const CallBuildFn = *const fn (ctx: *anyopaque, call: *protocol.CallBuilder) anyerror!void;
        const QuestionCallback = *const fn (ctx: *anyopaque, peer: *Peer, ret: protocol.Return, caps: *const cap_table.InboundCapTable) anyerror!void;
        const CallOptions = retained_question_state.CallOptions;
        const ensureCountLimit = Peer.ensureCountLimit;
        const Question = state.Question(QuestionCallback);

        pub fn sendCall(
            self: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            return sendCallWithOptions(self, target_id, interface_id, method_id, ctx, build, on_return, .{});
        }

        /// Send a call with an explicit result-lifetime policy. `.automatic`
        /// preserves `sendCall` exactly; `.retained` withholds Finish after Return
        /// until `finishRetainedQuestion` or an ownership transfer completes.
        pub fn sendCallWithOptions(
            self: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallWithOptionsRestore(
                self,
                target_id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                true,
            );
        }

        /// Generator plumbing for heap-owned typed call contexts. Unlike the raw
        /// callback API, generated callbacks own and free their context, so a
        /// synchronous callback error must never restore a question that points at
        /// freed memory.
        pub fn sendCallGeneratedWithOptions(
            self: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallWithOptionsRestore(
                self,
                target_id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                false,
            );
        }

        fn sendCallWithOptionsRestore(
            self: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
            restore_on_return_error: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            log.debug("sendCall target_id={} interface_id=0x{x} method_id={}", .{ target_id, interface_id, method_id });
            if (self.resolved_imports.get(target_id)) |entry| {
                if (!entry.embargoed and entry.cap != null) {
                    return sendCallResolvedWithOptionsRestore(
                        self,
                        entry.cap.?,
                        interface_id,
                        method_id,
                        ctx,
                        build,
                        on_return,
                        options,
                        restore_on_return_error,
                    );
                }
            }

            const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
                .retained => Peer.allocateRetainedQuestion,
            };

            const question_id = try peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                Peer.onOutboundCap,
                Peer.rollbackOutboundCap,
                self,
                target_id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                allocate_question,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            );
            // Record the promise-import target on the question so the Level-3
            // recipient auto-pickup can observe an in-flight pipelined call against
            // this (as-yet unresolved) import — the spec condition for embargoing
            // the handoff Accept (rpc.capnp:885-888). Only calls to imports NOT yet
            // in `resolved_imports` reach here (resolved caps take the fast path
            // above), so this marks exactly the still-in-flight-promise targets. The
            // mark is dropped implicitly when the question leaves the table.
            if (self.questions.getPtr(question_id)) |q| {
                q.target_promise_import = target_id;
            }
            return question_id;
        }

        /// Send a forwarded L3 vine call to a plain import (issue #56). Mirrors the
        /// import path of `sendCall` but allocates the question with
        /// `restore_on_return_error = false` (`allocateQuestionNoRestore`): the return
        /// callback (`forwardVineReturn`) frees the relay ctx, so a restore after a
        /// post-callback error would re-reference freed memory. It deliberately skips
        /// `sendCall`'s resolved-import fast path and the `target_promise_import` mark:
        /// the forward targets B's own (non-promise, non-embargoed) import of VatC's
        /// cap and must not be treated as an in-flight promise for auto-pickup.
        pub fn sendForwardedVineCall(
            self: *Peer,
            target_id: u32,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            return peer_call_sender.sendCallToImport(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                Peer.onOutboundCap,
                Peer.rollbackOutboundCap,
                self,
                target_id,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                Peer.allocateQuestionNoRestore,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            );
        }

        /// Forward a fallback call to the exact target named by the original
        /// Provide. The promised-answer branch deliberately uses the no-restore
        /// allocator for the same callback-owns-context invariant as the imported
        /// branch above.
        pub fn sendForwardedVineCallTarget(
            self: *Peer,
            target: provide_forward_target.ForwardTarget,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            return switch (target) {
                .imported => |target_id| sendForwardedVineCall(
                    self,
                    target_id,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                ),
                .promised => |promised| peer_call_sender.sendCallPromisedWithOps(
                    Peer,
                    CallBuildFn,
                    QuestionCallback,
                    self.allocator,
                    &self.caps,
                    self,
                    Peer.onOutboundCap,
                    Peer.rollbackOutboundCap,
                    self,
                    promised.question_id,
                    promised.ops,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    Peer.allocateQuestionNoRestore,
                    Peer.removeQuestion,
                    Peer.recordQuestionParamExports,
                    Peer.sendBuilder,
                ),
            };
        }

        /// Send a call to a resolved (non-promise) capability. Dispatches to the
        /// appropriate path based on the resolved cap type (imported, exported, or promised).
        pub fn sendCallResolved(
            self: *Peer,
            target: cap_table.ResolvedCap,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            return sendCallResolvedWithOptions(self, target, interface_id, method_id, ctx, build, on_return, .{});
        }

        pub fn sendCallResolvedWithOptions(
            self: *Peer,
            target: cap_table.ResolvedCap,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallResolvedWithOptionsRestore(
                self,
                target,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                true,
            );
        }

        fn sendCallResolvedWithOptionsRestore(
            self: *Peer,
            target: cap_table.ResolvedCap,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
            restore_on_return_error: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
                .retained => Peer.allocateRetainedQuestion,
            };
            return switch (target) {
                .imported => |cap| peer_call_sender.sendCallToImport(
                    Peer,
                    CallBuildFn,
                    QuestionCallback,
                    self.allocator,
                    &self.caps,
                    self,
                    Peer.onOutboundCap,
                    Peer.rollbackOutboundCap,
                    self,
                    cap.id,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    allocate_question,
                    Peer.removeQuestion,
                    Peer.recordQuestionParamExports,
                    Peer.sendBuilder,
                ),
                .promised => |promised| sendCallPromisedWithOptionsRestore(
                    self,
                    promised,
                    interface_id,
                    method_id,
                    ctx,
                    build,
                    on_return,
                    options,
                    restore_on_return_error,
                ),
                .exported => |cap| blk: {
                    try ensureCountLimit(false, self.loopback_questions.count(), self.limits.max_loopback_questions);
                    const allocate_loopback_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                        .automatic => if (restore_on_return_error) Peer.allocateLoopbackQuestion else Peer.allocateLoopbackQuestionNoRestore,
                        .retained => Peer.allocateRetainedLoopbackQuestion,
                    };
                    break :blk peer_call_sender.sendCallToExport(
                        Peer,
                        Question,
                        CallBuildFn,
                        QuestionCallback,
                        self.allocator,
                        &self.caps,
                        self,
                        Peer.onOutboundCap,
                        Peer.rollbackOutboundCap,
                        self,
                        &self.questions,
                        &self.loopback_questions,
                        cap.id,
                        interface_id,
                        method_id,
                        ctx,
                        build,
                        on_return,
                        allocate_loopback_question,
                        Peer.removeQuestion,
                        Peer.handleLoopbackFrame,
                    );
                },
                .none => error.CapabilityUnavailable,
            };
        }

        pub fn sendCallPromised(
            self: *Peer,
            promised: protocol.PromisedAnswer,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            return sendCallPromisedWithOptions(self, promised, interface_id, method_id, ctx, build, on_return, .{});
        }

        pub fn sendCallPromisedWithOptions(
            self: *Peer,
            promised: protocol.PromisedAnswer,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallPromisedWithOptionsRestore(
                self,
                promised,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                true,
            );
        }

        fn sendCallPromisedWithOptionsRestore(
            self: *Peer,
            promised: protocol.PromisedAnswer,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
            restore_on_return_error: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
                .retained => Peer.allocateRetainedQuestion,
            };
            return peer_call_sender.sendCallPromised(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                Peer.onOutboundCap,
                Peer.rollbackOutboundCap,
                self,
                promised,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                allocate_question,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            );
        }

        /// Like `sendCallPromised` but takes a question ID and a slice of
        /// `PromisedAnswerOp` directly, avoiding the need to build a reader-backed
        /// `PromisedAnswer`. Used by generated pipeline code.
        pub fn sendCallPromisedWithOps(
            self: *Peer,
            question_id_target: u32,
            ops: []const protocol.PromisedAnswerOp,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
        ) !u32 {
            return sendCallPromisedWithOpsWithOptions(
                self,
                question_id_target,
                ops,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                .{},
            );
        }

        pub fn sendCallPromisedWithOpsWithOptions(
            self: *Peer,
            question_id_target: u32,
            ops: []const protocol.PromisedAnswerOp,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallPromisedWithOpsWithOptionsRestore(
                self,
                question_id_target,
                ops,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                true,
            );
        }

        /// Generator plumbing for pipelined typed calls with heap-owned contexts;
        /// see `sendCallGeneratedWithOptions`.
        pub fn sendCallPromisedWithOpsGeneratedWithOptions(
            self: *Peer,
            question_id_target: u32,
            ops: []const protocol.PromisedAnswerOp,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
        ) !u32 {
            return sendCallPromisedWithOpsWithOptionsRestore(
                self,
                question_id_target,
                ops,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                options,
                false,
            );
        }

        fn sendCallPromisedWithOpsWithOptionsRestore(
            self: *Peer,
            question_id_target: u32,
            ops: []const protocol.PromisedAnswerOp,
            interface_id: u64,
            method_id: u16,
            ctx: *anyopaque,
            build: ?CallBuildFn,
            on_return: QuestionCallback,
            options: CallOptions,
            restore_on_return_error: bool,
        ) !u32 {
            self.assertThreadAffinity();
            if (self.is_shutting_down) return error.PeerShuttingDown;
            const allocate_question: *const fn (*Peer, *anyopaque, QuestionCallback) anyerror!u32 = switch (options.result_lifetime) {
                .automatic => if (restore_on_return_error) Peer.allocateQuestion else Peer.allocateQuestionNoRestore,
                .retained => Peer.allocateRetainedQuestion,
            };
            return peer_call_sender.sendCallPromisedWithOps(
                Peer,
                CallBuildFn,
                QuestionCallback,
                self.allocator,
                &self.caps,
                self,
                Peer.onOutboundCap,
                Peer.rollbackOutboundCap,
                self,
                question_id_target,
                ops,
                interface_id,
                method_id,
                ctx,
                build,
                on_return,
                allocate_question,
                Peer.removeQuestion,
                Peer.recordQuestionParamExports,
                Peer.sendBuilder,
            );
        }
    };
}
