const std = @import("std");
const log = std.log.scoped(.rpc_peer);

const cap_table = @import("../caps/table.zig");
const message = @import("../../serialization/message.zig");
const protocol = @import("../wire/protocol.zig");
const state = @import("./state.zig");
const persistence = @import("./persistence.zig");
const events = @import("../events.zig");

/// Persistence hosting hooks, extracted from `peer/mod.zig` and made generic
/// over the peer type (the JoinCoordinator extraction contract): persistent
/// export save handlers, the vat restorer, outbound Save/Restore questions
/// with their param builders and Return callbacks, and the per-export
/// PersistenceState lifecycle. The pinned `RestoreOutcome` type and its
/// declaration site in `peer/persistence.zig` are untouched. `peer/mod.zig`
/// keeps every caller-visible name as a thunk on `Peer`.
pub fn PersistenceHooks(comptime Peer: type) type {
    return struct {
        const SaveHandler = Peer.SaveHandlerFn;
        const RestoreHandler = Peer.RestoreHandlerFn;
        const SaveResponseCallback = Peer.SaveResponseCallbackFn;
        const RestoreResponseCallback = Peer.RestoreResponseCallbackFn;
        const SaveHook = Peer.SaveHookRecord;
        const RestoreHook = Peer.RestoreHookRecord;
        const PersistenceState = Peer.PersistenceStateRecord;
        const ensureCountLimit = Peer.ensureCountLimit;
        const SaveResponse = Peer.SaveResponseUnion;
        const RestoreResponse = Peer.RestoreResponseUnion;

        fn localCastCtx(comptime Ptr: type, ctx: *anyopaque) Ptr {
            return @ptrCast(@alignCast(ctx));
        }

        /// Mark an existing export persistent: when the remote calls
        /// `Persistent.save()` on it, `on_save` produces the sturdy-ref payload
        /// for the `SaveResults` Return. The save call is dispatched through the
        /// normal inbound-call path; every other interface still reaches the
        /// export's original handler. Calling this again replaces the hooks.
        pub fn setPersistentExport(self: *Peer, export_id: u32, ctx: *anyopaque, on_save: SaveHandler) !void {
            self.assertThreadAffinity();
            const st = try ensurePersistenceState(self, export_id);
            st.save = .{ .ctx = ctx, .on_save = on_save };
            log.debug("export {} marked persistent", .{export_id});
        }

        /// Remove the save hook from an export. The original call handler is
        /// restored once no persistence hooks remain on the export.
        pub fn clearPersistentExport(self: *Peer, export_id: u32) void {
            self.assertThreadAffinity();
            const st = self.persistent_exports.get(export_id) orelse return;
            st.save = null;
            dropPersistenceStateIfUnused(self, export_id, st);
        }

        /// Install the vat-level restorer hook on the bootstrap capability.
        ///
        /// Restore is vat-specific per the RPC spec; capnp-zig serves it as a
        /// call on the bootstrap export using the `Restorer` convention in
        /// `peer/persistence.zig` (`restore(sturdyRef :Data) -> (cap :Capability)`).
        /// `on_restore` maps the sturdy-ref bytes to a capability (or `.unknown`,
        /// which answers with an "unknown sturdy ref" exception). Requires
        /// `setBootstrap` to have been called; call it again after replacing the
        /// bootstrap export.
        pub fn setRestorer(self: *Peer, ctx: *anyopaque, on_restore: RestoreHandler) !void {
            self.assertThreadAffinity();
            const bootstrap_id = self.bootstrap_export_id orelse return error.BootstrapNotConfigured;
            const st = try ensurePersistenceState(self, bootstrap_id);
            st.restore = .{ .ctx = ctx, .on_restore = on_restore };
            self.restorer_export_id = bootstrap_id;
            log.debug("restorer installed on bootstrap export {}", .{bootstrap_id});
        }

        /// Remove the restorer hook installed by `setRestorer`.
        pub fn clearRestorer(self: *Peer) void {
            self.assertThreadAffinity();
            const export_id = self.restorer_export_id orelse return;
            self.restorer_export_id = null;
            const st = self.persistent_exports.get(export_id) orelse return;
            st.restore = null;
            dropPersistenceStateIfUnused(self, export_id, st);
        }

        /// Call `Persistent.save()` on an imported capability. The callback
        /// receives the sturdy-ref payload from the remote's `SaveResults`
        /// (or the exception/other Return outcome). Returns the question ID,
        /// which supports `cancelQuestion` / `setQuestionDeadline` as usual.
        pub fn sendSave(self: *Peer, target_id: u32, ctx: *anyopaque, on_response: SaveResponseCallback) !u32 {
            self.assertThreadAffinity();
            const heap = try self.allocator.create(SaveQuestionContext);
            heap.* = .{ .user_ctx = ctx, .callback = on_response };
            var heap_owned = true;
            errdefer if (heap_owned) self.allocator.destroy(heap);
            const question_id = try self.sendCall(
                target_id,
                persistence.persistent_interface_id,
                persistence.save_method_id,
                heap,
                buildSaveCallParams,
                onSaveReturn,
            );
            heap_owned = false;
            if (self.questions.getPtr(question_id)) |q| {
                q.deinit_ctx = SaveQuestionContext.deinitCtx;
                q.restore_on_return_error = false;
            }
            return question_id;
        }

        /// Call the vat-level restore method on an imported capability
        /// (normally the remote's bootstrap, obtained via `sendBootstrap`).
        /// `sturdy_ref` is copied into the outbound frame before this returns.
        /// The callback receives the restored capability, already retained;
        /// release it via `releaseImport` when done. Documented client flow:
        /// connect -> `sendBootstrap` -> `sendRestore(ref)` -> resume calling.
        pub fn sendRestore(
            self: *Peer,
            target_id: u32,
            sturdy_ref: []const u8,
            ctx: *anyopaque,
            on_response: RestoreResponseCallback,
        ) !u32 {
            self.assertThreadAffinity();
            const heap = try self.allocator.create(RestoreQuestionContext);
            heap.* = .{ .user_ctx = ctx, .callback = on_response, .sturdy_ref = sturdy_ref };
            var heap_owned = true;
            errdefer if (heap_owned) self.allocator.destroy(heap);
            const question_id = try self.sendCall(
                target_id,
                persistence.restorer_interface_id,
                persistence.restore_method_id,
                heap,
                buildRestoreCallParams,
                onRestoreReturn,
            );
            heap_owned = false;
            if (self.questions.getPtr(question_id)) |q| {
                q.deinit_ctx = RestoreQuestionContext.deinitCtx;
                q.restore_on_return_error = false;
            }
            return question_id;
        }

        /// Look up or create the persistence state for an export, swapping the
        /// export's stored handler for the persistence trampoline. The original
        /// handler keeps serving every non-persistence interface.
        pub fn ensurePersistenceState(self: *Peer, export_id: u32) !*PersistenceState {
            const entry = self.exports.getEntry(export_id) orelse return error.UnknownExport;
            if (self.persistent_exports.get(export_id)) |st| return st;
            const original = entry.value_ptr.handler orelse return error.ExportHasNoHandler;

            const persistent_before = self.persistent_exports.count();
            try ensureCountLimit(false, persistent_before, self.limits.max_persistent_exports);
            const st = try self.allocator.create(PersistenceState);
            errdefer self.allocator.destroy(st);
            st.* = .{ .export_id = export_id, .original = original };
            try self.persistent_exports.put(export_id, st);
            entry.value_ptr.handler = .{ .ctx = st, .on_call = persistenceOnCall };
            events.emitPressureCrossing(
                self.observer,
                .peer,
                .unknown,
                .persistent_exports,
                persistent_before,
                self.persistent_exports.count(),
                self.limits.max_persistent_exports,
            );
            return st;
        }

        /// Drop a persistence state once both hook sets are cleared, restoring
        /// the export's original handler.
        pub fn dropPersistenceStateIfUnused(self: *Peer, export_id: u32, st: *PersistenceState) void {
            if (st.save != null or st.restore != null) return;
            if (self.exports.getEntry(export_id)) |entry| {
                if (entry.value_ptr.handler) |handler| {
                    if (handler.on_call == persistenceOnCall and handler.ctx == @as(*anyopaque, @ptrCast(st))) {
                        entry.value_ptr.handler = st.original;
                    }
                }
            }
            _ = self.persistent_exports.remove(export_id);
            self.allocator.destroy(st);
        }

        /// Free persistence state for an export that left the exports table
        /// (remote released it). The original handler is gone with the export.
        pub fn dropPersistenceStateForRemovedExport(self: *Peer, export_id: u32) void {
            if (self.persistent_exports.fetchRemove(export_id)) |removed| {
                self.allocator.destroy(removed.value);
                if (self.restorer_export_id == export_id) self.restorer_export_id = null;
            }
        }

        /// Trampoline installed as the export handler for persistent exports.
        /// Serves `Persistent.save()` and vat-level restore; forwards every
        /// other interface to the export's original handler.
        pub fn persistenceOnCall(
            ctx: *anyopaque,
            peer: *Peer,
            call: protocol.Call,
            caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const st: *PersistenceState = localCastCtx(*PersistenceState, ctx);
            if (call.interface_id == persistence.persistent_interface_id) {
                if (st.save) |hook| {
                    if (call.method_id == persistence.save_method_id) {
                        return servePersistentSave(peer, st.export_id, hook, call);
                    }
                    return peer.sendReturnException(call.question_id, "unknown method");
                }
            } else if (call.interface_id == persistence.restorer_interface_id) {
                if (st.restore) |hook| {
                    if (call.method_id == persistence.restore_method_id) {
                        return serveRestore(peer, hook, call);
                    }
                    return peer.sendReturnException(call.question_id, "unknown method");
                }
            }
            return st.original.on_call(st.original.ctx, peer, call, caps);
        }

        pub fn servePersistentSave(self: *Peer, export_id: u32, hook: SaveHook, call: protocol.Call) !void {
            const seal_for = persistence.readSealFor(call.params);
            const sturdy_ref = hook.on_save(hook.ctx, self, export_id, seal_for) catch |err| {
                if (err == error.OutOfMemory) return err;
                log.debug("save handler failed for export {}: {}", .{ export_id, err });
                return self.sendReturnException(call.question_id, @errorName(err));
            };
            defer self.allocator.free(sturdy_ref);
            if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .sturdy_ref_bytes,
                    sturdy_ref.len,
                    self.limits.max_sturdy_ref_bytes,
                    error.PeerLimitExceeded,
                );
                return self.sendReturnException(call.question_id, "sturdy ref too large");
            }
            var build_ctx = persistence.SturdyRefReturnContext{ .sturdy_ref = sturdy_ref };
            try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildSaveResultsReturn);
            self.saves_served +%= 1;
        }

        pub fn serveRestore(self: *Peer, hook: RestoreHook, call: protocol.Call) !void {
            const sturdy_ref = persistence.readSturdyRefParam(call.params) catch |err| {
                return self.sendReturnException(call.question_id, @errorName(err));
            };
            if (sturdy_ref.len > self.limits.max_sturdy_ref_bytes) {
                events.emitResourceRejection(
                    self.observer,
                    .peer,
                    .unknown,
                    .sturdy_ref_bytes,
                    sturdy_ref.len,
                    self.limits.max_sturdy_ref_bytes,
                    error.PeerLimitExceeded,
                );
                return self.sendReturnException(call.question_id, "sturdy ref too large");
            }
            const outcome = hook.on_restore(hook.ctx, self, sturdy_ref) catch |err| {
                if (err == error.OutOfMemory) return err;
                log.debug("restore handler failed: {}", .{err});
                return self.sendReturnException(call.question_id, @errorName(err));
            };
            var hosted_export_id: ?u32 = null;
            const export_id: u32 = switch (outcome) {
                .unknown => return self.sendReturnException(call.question_id, "unknown sturdy ref"),
                .existing => |id| blk: {
                    if (!self.exports.contains(id)) {
                        return self.sendReturnException(call.question_id, "unknown sturdy ref");
                    }
                    break :blk id;
                },
                .host => |exported| blk: {
                    const id = try self.addExport(exported);
                    hosted_export_id = id;
                    break :blk id;
                },
            };
            errdefer if (hosted_export_id) |id| self.destroyUnreferencedExport(id);
            var build_ctx = persistence.CapabilityReturnContext{ .cap_id = export_id };
            try self.sendReturnResults(call.question_id, &build_ctx, persistence.buildCapabilityReturn);
            self.restores_served +%= 1;
        }

        const SaveQuestionContext = struct {
            user_ctx: *anyopaque,
            callback: SaveResponseCallback,

            fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
                const ctx: *SaveQuestionContext = @ptrCast(@alignCast(ctx_ptr));
                allocator.destroy(ctx);
            }
        };

        const RestoreQuestionContext = struct {
            user_ctx: *anyopaque,
            callback: RestoreResponseCallback,
            /// Borrowed from the caller; valid only while `sendRestore` runs
            /// (the build callback copies it into the frame synchronously).
            sturdy_ref: []const u8,

            fn deinitCtx(allocator: std.mem.Allocator, ctx_ptr: *anyopaque) void {
                const ctx: *RestoreQuestionContext = @ptrCast(@alignCast(ctx_ptr));
                allocator.destroy(ctx);
            }
        };

        pub fn buildSaveCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
            _ = ctx_ptr;
            try persistence.writeEmptySaveParams(call_builder);
        }

        pub fn buildRestoreCallParams(ctx_ptr: *anyopaque, call_builder: *protocol.CallBuilder) anyerror!void {
            const ctx: *const RestoreQuestionContext = localCastCtx(*const RestoreQuestionContext, ctx_ptr);
            try persistence.writeRestoreParams(call_builder, ctx.sturdy_ref);
        }

        pub fn onSaveReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            _ = inbound_caps;
            const ctx: *SaveQuestionContext = localCastCtx(*SaveQuestionContext, ctx_ptr);
            defer peer.allocator.destroy(ctx);
            const response: SaveResponse = switch (ret.tag) {
                .results => blk: {
                    const payload = ret.results orelse return error.MissingReturnPayload;
                    break :blk .{ .sturdy_ref = try persistence.readSturdyRefResult(payload) };
                },
                .exception => .{ .exception = ret.exception orelse return error.MissingException },
                else => .{ .other = ret.tag },
            };
            try ctx.callback(ctx.user_ctx, peer, response);
        }

        pub fn onRestoreReturn(
            ctx_ptr: *anyopaque,
            peer: *Peer,
            ret: protocol.Return,
            inbound_caps: *const cap_table.InboundCapTable,
        ) anyerror!void {
            const ctx: *RestoreQuestionContext = localCastCtx(*RestoreQuestionContext, ctx_ptr);
            defer peer.allocator.destroy(ctx);
            var retained_cap: ?cap_table.ResolvedCap = null;
            const response: RestoreResponse = switch (ret.tag) {
                .results => blk: {
                    const payload = ret.results orelse return error.MissingReturnPayload;
                    if (payload.content.isNull()) return error.MissingRestoredCapability;
                    const results_struct = payload.content.getStruct() catch return error.MissingRestoredCapability;
                    const field = results_struct.readAnyPointer(0) catch return error.MissingRestoredCapability;
                    const cap = field.getCapability() catch return error.MissingRestoredCapability;
                    var mutable_caps = inbound_caps.*;
                    try mutable_caps.retainCapability(cap);
                    const resolved = try inbound_caps.resolveCapability(cap);
                    retained_cap = resolved;
                    break :blk .{ .cap = resolved };
                },
                .exception => .{ .exception = ret.exception orelse return error.MissingException },
                else => .{ .other = ret.tag },
            };
            ctx.callback(ctx.user_ctx, peer, response) catch |err| {
                if (retained_cap) |resolved| {
                    peer.releaseResolvedCap(resolved) catch |release_err| {
                        if (release_err == error.OutOfMemory) return error.OutOfMemory;
                        log.debug("failed to release restored cap after callback error: {}", .{release_err});
                    };
                }
                return err;
            };
        }
    };
}
