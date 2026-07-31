const std = @import("std");
const log = std.log.scoped(.rpc_host);
const events = @import("../events.zig");
const message = @import("../../serialization/message.zig");
const peer_mod = @import("../peer/mod.zig");
const protocol = @import("../wire/protocol.zig");
const cap_table = @import("../caps/table.zig");

pub const HostPeer = struct {
    const MAX_CAPTURED_FRAME_BYTES: usize = 16 * 1024 * 1024;

    pub const Limits = struct {
        pub const default_outbound_count_limit: usize = 1024;
        pub const default_outbound_bytes_limit: usize = MAX_CAPTURED_FRAME_BYTES;
        pub const default_host_call_count_limit: usize = 1024;
        pub const default_host_call_bytes_limit: usize = MAX_CAPTURED_FRAME_BYTES;

        // A zero value means "unlimited".
        outbound_count_limit: usize = default_outbound_count_limit,
        // A zero value means "unlimited".
        outbound_bytes_limit: usize = default_outbound_bytes_limit,
        // A zero value means "unlimited".
        host_call_count_limit: usize = default_host_call_count_limit,
        // A zero value means "unlimited".
        host_call_bytes_limit: usize = default_host_call_bytes_limit,
    };

    pub const ErrorDisclosurePolicy = struct {
        pub const default_generic_reason = "host call failed";
        pub const default_max_reason_bytes: usize = 256;

        reveal_details: bool = false,
        max_reason_bytes: usize = default_max_reason_bytes,
        generic_reason: []const u8 = default_generic_reason,
    };

    const SanitizedReason = struct {
        bytes: []const u8,
        owned: ?[]u8 = null,

        fn deinit(self: SanitizedReason, allocator: std.mem.Allocator) void {
            if (self.owned) |owned| allocator.free(owned);
        }
    };

    pub const HostCall = struct {
        question_id: u32,
        interface_id: u64,
        method_id: u16,
        frame: []u8,
    };

    allocator: std.mem.Allocator,
    outgoing_allocator: std.mem.Allocator,
    peer: peer_mod.Peer,
    outgoing: std.ArrayList([]u8),
    outgoing_bytes: usize = 0,
    limits: Limits = .{},
    error_disclosure: ErrorDisclosurePolicy = .{},
    host_calls: std.ArrayList(HostCall),
    host_call_bytes: usize = 0,
    pending_host_call_questions: std.AutoHashMap(u32, void),
    /// Param-cap import ids (one entry per capTable occurrence) retained for
    /// each queued host call, keyed by question id. Populated by `onHostCall`,
    /// settled when the host answers — see `settleHostCallParamImports`, whose
    /// choice of wire Release vs silent forget is dictated by the answering
    /// Return's `releaseParamCaps`.
    pending_host_call_param_imports: std.AutoHashMap(u32, []u32),
    current_inbound_frame: ?[]const u8 = null,
    host_bridge_enabled: bool = false,
    wired_override: bool = false,
    observer: ?events.Observer = null,

    pub fn init(allocator: std.mem.Allocator) HostPeer {
        return initWithOutgoingAllocator(allocator, allocator);
    }

    pub fn initWithOutgoingAllocator(
        allocator: std.mem.Allocator,
        outgoing_allocator: std.mem.Allocator,
    ) HostPeer {
        return .{
            .allocator = allocator,
            .outgoing_allocator = outgoing_allocator,
            .peer = peer_mod.Peer.initDetached(allocator),
            .outgoing = std.ArrayList([]u8).empty,
            .host_calls = std.ArrayList(HostCall).empty,
            .pending_host_call_questions = std.AutoHashMap(u32, void).init(allocator),
            .pending_host_call_param_imports = std.AutoHashMap(u32, []u32).init(allocator),
        };
    }

    pub fn deinit(self: *HostPeer) void {
        self.peer.assertThreadAffinity();
        // `Peer.deinit` is SEND-BEARING: `forceCancelAllQuestions` emits Finish
        // frames and `releaseAllImports` emits Release frames whenever a send
        // path is available, and `ensureOverride` has installed one that lands
        // in `self.outgoing`. So the peer must be torn down BEFORE the queues
        // its override writes into, or those sends append to freed storage.
        //
        // That ordering bug was invisible for as long as it existed: in Debug
        // and ReleaseSafe `ArrayList.deinit` poisons the handle, and the tests
        // that reach it happened not to trip a safety check, while ReleaseFast
        // leaves the freed pointer intact so the append silently lands in
        // freed memory. `SafeAllocator` caught it only under ReleaseFast.
        self.peer.deinit();
        self.clearOutgoing();
        self.outgoing.deinit(self.allocator);
        self.clearHostCalls();
        self.host_calls.deinit(self.allocator);
        self.pending_host_call_questions.deinit();
        var record_it = self.pending_host_call_param_imports.valueIterator();
        while (record_it.next()) |ids| self.allocator.free(ids.*);
        self.pending_host_call_param_imports.deinit();
    }

    pub fn start(
        self: *HostPeer,
        cb_ctx: ?*anyopaque,
        on_error: ?*const fn (ctx: ?*anyopaque, peer: *peer_mod.Peer, err: anyerror) void,
        on_close: ?*const fn (ctx: ?*anyopaque, peer: *peer_mod.Peer) void,
    ) void {
        self.peer.assertThreadAffinity();
        self.ensureOverride();
        events.emitConnection(self.observer, .host_peer, .unknown, .started);
        self.peer.start(cb_ctx, on_error, on_close);
    }

    pub fn setObserver(self: *HostPeer, observer: ?events.Observer) void {
        self.peer.assertThreadAffinity();
        self.observer = observer;
        self.peer.setObserver(observer);
    }

    pub fn pushFrame(self: *HostPeer, frame: []const u8) !void {
        self.peer.assertThreadAffinity();
        self.ensureOverride();
        events.emitFrame(self.observer, .host_peer, .unknown, .received, frame.len);
        self.current_inbound_frame = frame;
        defer self.current_inbound_frame = null;
        log.debug("pushing inbound frame ({} bytes)", .{frame.len});
        try self.peer.handleFrame(frame);
    }

    pub fn popOutgoingFrame(self: *HostPeer) ?[]u8 {
        self.peer.assertThreadAffinity();
        if (self.outgoing.items.len == 0) return null;
        const frame = self.outgoing.orderedRemove(0);
        self.outgoing_bytes -= frame.len;
        return frame;
    }

    pub fn pendingOutgoingCount(self: *const HostPeer) usize {
        self.peer.assertThreadAffinity();
        return self.outgoing.items.len;
    }

    pub fn pendingOutgoingBytes(self: *const HostPeer) usize {
        self.peer.assertThreadAffinity();
        return self.outgoing_bytes;
    }

    pub fn setLimits(self: *HostPeer, limits: Limits) void {
        self.peer.assertThreadAffinity();
        self.limits = limits;
    }

    pub fn getLimits(self: *const HostPeer) Limits {
        self.peer.assertThreadAffinity();
        return self.limits;
    }

    pub fn setErrorDisclosurePolicy(self: *HostPeer, policy: ErrorDisclosurePolicy) void {
        self.peer.assertThreadAffinity();
        self.error_disclosure = policy;
    }

    pub fn getErrorDisclosurePolicy(self: *const HostPeer) ErrorDisclosurePolicy {
        self.peer.assertThreadAffinity();
        return self.error_disclosure;
    }

    pub fn enableHostCallBridge(self: *HostPeer) !void {
        self.peer.assertThreadAffinity();
        if (self.host_bridge_enabled) return;

        _ = try self.peer.setBootstrap(.{
            .ctx = self,
            .on_call = onHostCall,
        });
        self.host_bridge_enabled = true;
    }

    pub fn pendingHostCallCount(self: *const HostPeer) usize {
        self.peer.assertThreadAffinity();
        return self.host_calls.items.len;
    }

    pub fn pendingHostCallBytes(self: *const HostPeer) usize {
        self.peer.assertThreadAffinity();
        return self.host_call_bytes;
    }

    pub fn popHostCall(self: *HostPeer) ?HostCall {
        self.peer.assertThreadAffinity();
        if (self.host_calls.items.len == 0) return null;
        const call = self.host_calls.orderedRemove(0);
        self.host_call_bytes -= call.frame.len;
        return call;
    }

    pub fn freeHostCallFrame(self: *HostPeer, frame: []u8) void {
        self.allocator.free(frame);
    }

    pub fn clearHostCalls(self: *HostPeer) void {
        self.peer.assertThreadAffinity();
        for (self.host_calls.items) |call| {
            self.allocator.free(call.frame);
            _ = self.pending_host_call_questions.remove(call.question_id);
            // The queued call will never be answered; drop its retained
            // param-cap record without wire traffic (teardown/reset path).
            if (self.pending_host_call_param_imports.fetchRemove(call.question_id)) |record| {
                self.allocator.free(record.value);
            }
        }
        self.host_calls.clearRetainingCapacity();
        self.host_call_bytes = 0;
    }

    /// Settle the param-cap imports retained for a host call once its Return
    /// is on the wire.
    ///
    /// Exactly ONE settlement signal may reach the remote per param grant, and
    /// `send_explicit_release` picks which one this is:
    ///
    ///   * `true` — the answering Return carries `releaseParamCaps = false`, so
    ///     the remote is still holding its exports for us and the wire Release
    ///     emitted here is the one and only signal. This is the peer-built
    ///     Return paths (`respondHostCallResults`, `respondHostCallException`),
    ///     where the host has no channel to express retention.
    ///   * `false` — nothing may go on the wire from here, only the peer's own
    ///     import bookkeeping is dropped. Either the Return already released
    ///     the caps (`releaseParamCaps = true`, and rpc.capnp: "the sender must
    ///     not send separate `Release` messages for them"), or the host claimed
    ///     retention (`false`) and owes its own `sendReleaseForHost` later.
    ///
    /// Emitting a Release alongside a `releaseParamCaps = true` Return spends
    /// the remote's export twice; a compliant peer answers that with "Tried to
    /// release invalid export ID" and drops the connection.
    fn settleHostCallParamImports(self: *HostPeer, question_id: u32, send_explicit_release: bool) void {
        const record = self.pending_host_call_param_imports.fetchRemove(question_id) orelse return;
        defer self.allocator.free(record.value);
        for (record.value) |import_id| {
            if (send_explicit_release) {
                self.peer.releaseImport(import_id, 1) catch |err| {
                    log.debug("failed to release host-call param import {}: {}", .{ import_id, err });
                };
            } else {
                self.peer.forgetImportRefsForHost(import_id, 1) catch |err| {
                    log.debug("failed to forget host-call param import {}: {}", .{ import_id, err });
                };
            }
        }
    }

    pub fn respondHostCallException(self: *HostPeer, question_id: u32, reason: []const u8) !void {
        self.peer.assertThreadAffinity();
        if (!self.pending_host_call_questions.contains(question_id)) return error.UnknownQuestion;
        try self.sendSanitizedReturnException(question_id, false, false, reason);
        _ = self.pending_host_call_questions.remove(question_id);
        // A failed call cannot legitimately retain its params; release them
        // back to the remote. The exception Return above says
        // `releaseParamCaps = false`, so the explicit Release frames this
        // settle emits are the one and only release signal (spec-consistent).
        self.settleHostCallParamImports(question_id, true);
    }

    pub fn respondHostCallResults(self: *HostPeer, question_id: u32, payload_frame: []const u8) !void {
        self.peer.assertThreadAffinity();
        if (!self.pending_host_call_questions.contains(question_id)) return error.UnknownQuestion;

        // A hostile guest can hand us a small payload whose pointer graph
        // aliases one blob thousands of times, or a deep pointer chain, then
        // rely on cloneAnyPointer (no traversal budget, no visited-set) to
        // amplify the work into an effectively unbounded clone while we hold
        // the ABI global mutex. Bound the raw frame size and run the full
        // pointer-graph validation walk (the same bounded budget the sibling
        // respondHostCallReturnFrame path gets via protocol.DecodedMessage.init)
        // so the traversal limit defeats aliasing amplification before we clone.
        if (payload_frame.len > MAX_CAPTURED_FRAME_BYTES) {
            log.debug("host call result payload too large: {} bytes", .{payload_frame.len});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .frame_bytes,
                payload_frame.len,
                MAX_CAPTURED_FRAME_BYTES,
                error.FrameTooLarge,
            );
            return error.FrameTooLarge;
        }

        var payload_msg = try message.Message.init(self.allocator, payload_frame, .{});
        defer payload_msg.deinit();
        const payload_any = try payload_msg.getRootAnyPointer();

        const BuildCtx = struct {
            any: message.AnyPointerReader,

            fn build(ctx_ptr: *anyopaque, ret: *protocol.ReturnBuilder) anyerror!void {
                const ctx: *const @This() = @ptrCast(@alignCast(ctx_ptr));
                var payload = try ret.payloadTyped();
                const out = try payload.initContent();
                try message.cloneAnyPointer(ctx.any, out);
                _ = try ret.initCapTableTyped(0);
            }
        };

        var ctx = BuildCtx{ .any = payload_any };
        try self.peer.sendReturnResults(question_id, &ctx, BuildCtx.build);
        _ = self.pending_host_call_questions.remove(question_id);
        // This legacy path cannot express retention; params are released back
        // to the remote once the Return is on the wire. `sendReturnResults`
        // stamps `releaseParamCaps = false` for any call whose params granted
        // import refs (Peer.returnReleasesParamCaps), which is exactly the set
        // this record can be non-empty for — so explicit Release frames are
        // the matching signal.
        self.settleHostCallParamImports(question_id, true);
    }

    pub fn respondHostCallReturnFrame(self: *HostPeer, return_frame: []const u8) !void {
        self.peer.assertThreadAffinity();
        var decoded = try protocol.DecodedMessage.init(self.allocator, return_frame);
        defer decoded.deinit();

        const ret = decoded.asReturn() catch |err| switch (err) {
            error.UnexpectedMessage => return error.HostCallReturnNotReturn,
            else => return err,
        };

        try validateHostCallReturnSemantics(ret);
        if (!self.pending_host_call_questions.contains(ret.answer_id)) return error.UnknownQuestion;

        if (ret.tag == .exception) {
            const ex = ret.exception orelse return error.InvalidReturnSemantics;
            try self.sendSanitizedReturnException(
                ret.answer_id,
                ret.release_param_caps,
                ret.no_finish_needed,
                ex.reason,
            );
        } else {
            var created = try self.registerHostReturnExports(ret);
            defer created.deinit(self.allocator);
            errdefer for (created.items) |id| self.peer.removeUnreferencedExport(id);
            try self.peer.sendPrebuiltReturnFrame(ret, return_frame);
        }
        _ = self.pending_host_call_questions.remove(ret.answer_id);
        // The host built this Return itself, so it owns BOTH halves of the
        // settlement and the peer puts nothing on the wire either way:
        // `releaseParamCaps = true` already released the param caps (rpc.capnp
        // forbids a second signal), and `false` claims retention — the host
        // sends its own `sendReleaseForHost` when it is done. All the peer does
        // is drop its now-transferred import bookkeeping.
        self.settleHostCallParamImports(ret.answer_id, false);
    }

    /// Ensure every senderHosted capability named by a host-built Return
    /// frame has a dispatchable entry in the peer's export table.
    ///
    /// In relay mode the trusted host owns the export-id space: its handlers
    /// mint new capability ids and reference them in Return cap tables
    /// without the peer ever seeing an addExport call. Each unknown id is
    /// registered as a host-bridge export (the same `onHostCall` handler the
    /// bootstrap bridge uses) so `sendPrebuiltReturnFrame`'s cap-ref
    /// accounting accepts the frame and later inbound Calls targeting the id
    /// are queued back to the host.
    ///
    /// senderPromise ids are NOT auto-registered: a promise export carries a
    /// Resolve obligation the host bridge has no way to satisfy, so those
    /// still fail with UnknownExport in `sendPrebuiltReturnFrame`.
    ///
    /// Returns the ids newly created here (in list order) so a caller that
    /// fails before the frame takes its first reference can roll them back
    /// via `removeUnreferencedExport`. On error every id already created is
    /// rolled back before returning.
    fn registerHostReturnExports(self: *HostPeer, ret: protocol.Return) !std.ArrayList(u32) {
        var created = std.ArrayList(u32).empty;
        errdefer {
            for (created.items) |id| self.peer.removeUnreferencedExport(id);
            created.deinit(self.allocator);
        }

        if (ret.tag != .results) return created;
        const payload = ret.results orelse return created;
        const cap_list = payload.cap_table orelse return created;

        var idx: u32 = 0;
        while (idx < cap_list.len()) : (idx += 1) {
            const desc = try protocol.CapDescriptor.fromReader(try cap_list.get(idx));
            if (desc.tag != .senderHosted) continue;
            const id = desc.id orelse return error.MissingCapDescriptorId;
            try created.ensureUnusedCapacity(self.allocator, 1);
            const added = try self.peer.ensureExportAt(id, .{
                .ctx = self,
                .on_call = onHostCall,
            });
            if (added) created.appendAssumeCapacity(id);
        }
        return created;
    }

    pub fn freeFrame(self: *HostPeer, frame: []u8) void {
        self.peer.assertThreadAffinity();
        self.outgoing_allocator.free(frame);
    }

    pub fn clearOutgoing(self: *HostPeer) void {
        self.peer.assertThreadAffinity();
        for (self.outgoing.items) |frame| self.outgoing_allocator.free(frame);
        self.outgoing.clearRetainingCapacity();
        self.outgoing_bytes = 0;
    }

    fn ensureOverride(self: *HostPeer) void {
        if (self.wired_override) return;
        self.peer.setSendFrameOverride(self, captureOutgoingFrame);
        self.wired_override = true;
    }

    fn onHostCall(
        ctx: *anyopaque,
        called_peer: *peer_mod.Peer,
        call: protocol.Call,
        inbound_caps: *const cap_table.InboundCapTable,
    ) anyerror!void {
        _ = called_peer;

        const self: *HostPeer = @ptrCast(@alignCast(ctx));
        const inbound_frame = self.current_inbound_frame orelse return error.MissingHostInboundFrame;

        if (inbound_frame.len > MAX_CAPTURED_FRAME_BYTES) {
            log.debug("host call frame too large: {} bytes", .{inbound_frame.len});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .frame_bytes,
                inbound_frame.len,
                MAX_CAPTURED_FRAME_BYTES,
                error.FrameTooLarge,
            );
            return error.FrameTooLarge;
        }
        if (self.pending_host_call_questions.contains(call.question_id)) return error.DuplicateQuestionId;
        if (self.limits.host_call_count_limit != 0 and self.host_calls.items.len >= self.limits.host_call_count_limit) {
            log.debug("host call queue count limit exceeded", .{});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .host_call_frames,
                self.host_calls.items.len + 1,
                self.limits.host_call_count_limit,
                error.HostCallQueueLimitExceeded,
            );
            return error.HostCallQueueLimitExceeded;
        }
        if (self.limits.host_call_bytes_limit != 0) {
            const next = std.math.add(usize, self.host_call_bytes, inbound_frame.len) catch {
                log.debug("host call bytes limit exceeded", .{});
                events.emitResourceRejection(
                    self.observer,
                    .host_peer,
                    .unknown,
                    .host_call_bytes,
                    inbound_frame.len,
                    self.limits.host_call_bytes_limit,
                    error.HostCallBytesLimitExceeded,
                );
                return error.HostCallBytesLimitExceeded;
            };
            if (next > self.limits.host_call_bytes_limit) {
                log.debug("host call bytes limit exceeded", .{});
                events.emitResourceRejection(
                    self.observer,
                    .host_peer,
                    .unknown,
                    .host_call_bytes,
                    next,
                    self.limits.host_call_bytes_limit,
                    error.HostCallBytesLimitExceeded,
                );
                return error.HostCallBytesLimitExceeded;
            }
        }

        // Collect the param-cap import ids (one per capTable occurrence) so
        // the call can retain them for its whole lifetime. All fallible work
        // happens before the queue is mutated; the retention marks and record
        // insert run in the infallible tail below.
        var param_import_ids = std.ArrayList(u32).empty;
        defer param_import_ids.deinit(self.allocator);
        var cap_idx: u32 = 0;
        while (cap_idx < inbound_caps.len()) : (cap_idx += 1) {
            switch (try inbound_caps.get(cap_idx)) {
                .imported => |cap| try param_import_ids.append(self.allocator, cap.id),
                else => {},
            }
        }
        var owned_import_ids: ?[]u32 = null;
        errdefer if (owned_import_ids) |ids| self.allocator.free(ids);
        if (param_import_ids.items.len > 0) {
            owned_import_ids = try self.allocator.dupe(u32, param_import_ids.items);
            try self.pending_host_call_param_imports.ensureUnusedCapacity(1);
        }

        const frame_copy = try self.allocator.alloc(u8, inbound_frame.len);
        errdefer self.allocator.free(frame_copy);
        std.mem.copyForwards(u8, frame_copy, inbound_frame);

        const pending = try self.pending_host_call_questions.getOrPut(call.question_id);
        if (pending.found_existing) return error.DuplicateQuestionId;
        errdefer _ = self.pending_host_call_questions.remove(call.question_id);

        try self.host_calls.append(self.allocator, .{
            .question_id = call.question_id,
            .interface_id = call.interface_id,
            .method_id = call.method_id,
            .frame = frame_copy,
        });
        self.host_call_bytes += inbound_frame.len;

        if (owned_import_ids) |ids| {
            // Mark every imported param cap retained so the dispatch-time
            // release pass leaves it alone: the references stay live until
            // the host answers and `settleHostCallParamImports` settles them.
            // `inbound_caps` is const by handler contract, but its `retained`
            // slice aliases the dispatching frame's mutable table — the same
            // shared-storage retention pattern native handlers use.
            var mutable_caps = inbound_caps.*;
            cap_idx = 0;
            while (cap_idx < inbound_caps.len()) : (cap_idx += 1) {
                switch (inbound_caps.get(cap_idx) catch unreachable) {
                    .imported => mutable_caps.retainIndex(cap_idx) catch unreachable,
                    else => {},
                }
            }
            self.pending_host_call_param_imports.putAssumeCapacity(call.question_id, ids);
            owned_import_ids = null;
        }
    }

    fn captureOutgoingFrame(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *HostPeer = @ptrCast(@alignCast(ctx));
        if (frame.len > MAX_CAPTURED_FRAME_BYTES) {
            log.debug("outgoing frame too large: {} bytes", .{frame.len});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .frame_bytes,
                frame.len,
                MAX_CAPTURED_FRAME_BYTES,
                error.FrameTooLarge,
            );
            return error.FrameTooLarge;
        }

        if (self.limits.outbound_count_limit != 0 and self.outgoing.items.len >= self.limits.outbound_count_limit) {
            log.debug("outgoing queue count limit exceeded", .{});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .host_outbound_frames,
                self.outgoing.items.len + 1,
                self.limits.outbound_count_limit,
                error.OutgoingQueueLimitExceeded,
            );
            return error.OutgoingQueueLimitExceeded;
        }

        try self.ensureOutgoingByteBudget(frame.len);
        const owned = try self.copyOutgoingFrame(frame);
        errdefer self.outgoing_allocator.free(owned);
        try self.ensureOutgoingByteBudget(owned.len);

        try self.outgoing.append(self.allocator, owned);
        self.outgoing_bytes += owned.len;
        events.emitFrame(self.observer, .host_peer, .unknown, .enqueued, owned.len);
    }

    fn ensureOutgoingByteBudget(self: *HostPeer, added_len: usize) !void {
        if (self.limits.outbound_bytes_limit == 0) return;
        const next = std.math.add(usize, self.outgoing_bytes, added_len) catch {
            log.debug("outgoing bytes limit exceeded", .{});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .host_outbound_bytes,
                added_len,
                self.limits.outbound_bytes_limit,
                error.OutgoingBytesLimitExceeded,
            );
            return error.OutgoingBytesLimitExceeded;
        };
        if (next > self.limits.outbound_bytes_limit) {
            log.debug("outgoing bytes limit exceeded", .{});
            events.emitResourceRejection(
                self.observer,
                .host_peer,
                .unknown,
                .host_outbound_bytes,
                next,
                self.limits.outbound_bytes_limit,
                error.OutgoingBytesLimitExceeded,
            );
            return error.OutgoingBytesLimitExceeded;
        }
    }

    fn sendSanitizedReturnException(
        self: *HostPeer,
        answer_id: u32,
        release_param_caps: bool,
        no_finish_needed: bool,
        reason: []const u8,
    ) !void {
        const sanitized = try self.sanitizeExternalReason(reason);
        defer sanitized.deinit(self.allocator);

        var builder = protocol.MessageBuilder.init(self.allocator);
        defer builder.deinit();

        var ret_builder = try builder.beginReturn(answer_id, .exception);
        ret_builder.setReleaseParamCaps(release_param_caps);
        ret_builder.setNoFinishNeeded(no_finish_needed);
        try ret_builder.setException(sanitized.bytes);

        const bytes = try builder.finish();
        defer self.allocator.free(bytes);

        const ret = protocol.Return{
            .answer_id = answer_id,
            .release_param_caps = release_param_caps,
            .no_finish_needed = no_finish_needed,
            .tag = .exception,
            .results = null,
            .exception = .{
                .reason = sanitized.bytes,
                .trace = "",
                .type_value = 0,
            },
            .take_from_other_question = null,
        };
        try self.peer.sendPrebuiltReturnFrame(ret, bytes);
    }

    fn copyOutgoingFrame(self: *HostPeer, frame: []const u8) ![]u8 {
        if (try self.sanitizedReturnExceptionFrame(frame)) |sanitized| return sanitized;
        if (try self.sanitizedAbortFrame(frame)) |sanitized| return sanitized;

        const owned = try self.outgoing_allocator.alloc(u8, frame.len);
        std.mem.copyForwards(u8, owned, frame);
        return owned;
    }

    fn sanitizedReturnExceptionFrame(self: *HostPeer, frame: []const u8) !?[]u8 {
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return null,
        };
        defer decoded.deinit();

        if (decoded.tag != .@"return") return null;
        const ret = decoded.asReturn() catch return null;
        if (ret.tag != .exception) return null;
        const ex = ret.exception orelse return null;

        const sanitized = try self.sanitizeExternalReason(ex.reason);
        defer sanitized.deinit(self.allocator);

        var builder = protocol.MessageBuilder.init(self.outgoing_allocator);
        defer builder.deinit();

        var ret_builder = try builder.beginReturn(ret.answer_id, .exception);
        ret_builder.setReleaseParamCaps(ret.release_param_caps);
        ret_builder.setNoFinishNeeded(ret.no_finish_needed);
        try ret_builder.setException(sanitized.bytes);

        return @constCast(try builder.finish());
    }

    fn sanitizedAbortFrame(self: *HostPeer, frame: []const u8) !?[]u8 {
        var decoded = protocol.DecodedMessage.init(self.allocator, frame) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return null,
        };
        defer decoded.deinit();

        if (decoded.tag != .abort) return null;
        const abort = decoded.asAbort() catch return null;

        const sanitized = try self.sanitizeExternalReason(abort.exception.reason);
        defer sanitized.deinit(self.allocator);

        var builder = protocol.MessageBuilder.init(self.outgoing_allocator);
        defer builder.deinit();
        try builder.buildAbort(sanitized.bytes);

        return @constCast(try builder.finish());
    }

    fn sanitizeExternalReason(self: *HostPeer, reason: []const u8) !SanitizedReason {
        const policy = self.error_disclosure;
        const source = if (policy.reveal_details) reason else policy.generic_reason;
        const len = @min(source.len, policy.max_reason_bytes);
        var needs_copy = source.len > len;

        for (source[0..len]) |byte| {
            if (!isSafeReasonByte(byte)) {
                needs_copy = true;
                break;
            }
        }

        if (!needs_copy) {
            return .{ .bytes = source[0..len] };
        }

        const owned = try self.allocator.alloc(u8, len);
        for (source[0..len], 0..) |byte, i| {
            owned[i] = if (isSafeReasonByte(byte)) byte else ' ';
        }

        return .{ .bytes = owned, .owned = owned };
    }

    fn isSafeReasonByte(byte: u8) bool {
        return byte >= 0x20 and byte != 0x7f;
    }

    fn validateHostCallReturnSemantics(ret: protocol.Return) !void {
        switch (ret.tag) {
            .results => if (ret.results == null) return error.InvalidReturnSemantics,
            .exception => if (ret.exception == null) return error.InvalidReturnSemantics,
            .takeFromOtherQuestion => if (ret.take_from_other_question == null) return error.InvalidReturnSemantics,
            else => {},
        }
    }
};
