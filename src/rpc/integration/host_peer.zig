const std = @import("std");
const log = std.log.scoped(.rpc_host);
const message = @import("../../serialization/message.zig");
const peer_mod = @import("../level3/peer.zig");
const protocol = @import("../level0/protocol.zig");
const cap_table = @import("../level0/cap_table.zig");

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
    current_inbound_frame: ?[]const u8 = null,
    host_bridge_enabled: bool = false,
    wired_override: bool = false,

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
        };
    }

    pub fn deinit(self: *HostPeer) void {
        self.peer.assertThreadAffinity();
        self.clearOutgoing();
        self.outgoing.deinit(self.allocator);
        self.clearHostCalls();
        self.host_calls.deinit(self.allocator);
        self.pending_host_call_questions.deinit();
        self.peer.deinit();
    }

    pub fn start(
        self: *HostPeer,
        on_error: ?*const fn (peer: *peer_mod.Peer, err: anyerror) void,
        on_close: ?*const fn (peer: *peer_mod.Peer) void,
    ) void {
        self.peer.assertThreadAffinity();
        self.ensureOverride();
        self.peer.start(on_error, on_close);
    }

    pub fn pushFrame(self: *HostPeer, frame: []const u8) !void {
        self.peer.assertThreadAffinity();
        self.ensureOverride();
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
        }
        self.host_calls.clearRetainingCapacity();
        self.host_call_bytes = 0;
    }

    pub fn respondHostCallException(self: *HostPeer, question_id: u32, reason: []const u8) !void {
        self.peer.assertThreadAffinity();
        if (!self.pending_host_call_questions.contains(question_id)) return error.UnknownQuestion;
        try self.sendSanitizedReturnException(question_id, false, false, reason);
        _ = self.pending_host_call_questions.remove(question_id);
    }

    pub fn respondHostCallResults(self: *HostPeer, question_id: u32, payload_frame: []const u8) !void {
        self.peer.assertThreadAffinity();
        if (!self.pending_host_call_questions.contains(question_id)) return error.UnknownQuestion;
        var payload_msg = try message.Message.initUnvalidated(self.allocator, payload_frame);
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
            try self.peer.sendPrebuiltReturnFrame(ret, return_frame);
        }
        _ = self.pending_host_call_questions.remove(ret.answer_id);
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
        _ = inbound_caps;

        const self: *HostPeer = @ptrCast(@alignCast(ctx));
        const inbound_frame = self.current_inbound_frame orelse return error.MissingHostInboundFrame;

        if (inbound_frame.len > MAX_CAPTURED_FRAME_BYTES) {
            log.debug("host call frame too large: {} bytes", .{inbound_frame.len});
            return error.FrameTooLarge;
        }
        if (self.pending_host_call_questions.contains(call.question_id)) return error.DuplicateQuestionId;
        if (self.limits.host_call_count_limit != 0 and self.host_calls.items.len >= self.limits.host_call_count_limit) {
            log.debug("host call queue count limit exceeded", .{});
            return error.HostCallQueueLimitExceeded;
        }
        if (self.limits.host_call_bytes_limit != 0) {
            const next = std.math.add(usize, self.host_call_bytes, inbound_frame.len) catch {
                log.debug("host call bytes limit exceeded", .{});
                return error.HostCallBytesLimitExceeded;
            };
            if (next > self.limits.host_call_bytes_limit) {
                log.debug("host call bytes limit exceeded", .{});
                return error.HostCallBytesLimitExceeded;
            }
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
    }

    fn captureOutgoingFrame(ctx: *anyopaque, frame: []const u8) anyerror!void {
        const self: *HostPeer = @ptrCast(@alignCast(ctx));
        if (frame.len > MAX_CAPTURED_FRAME_BYTES) {
            log.debug("outgoing frame too large: {} bytes", .{frame.len});
            return error.FrameTooLarge;
        }

        if (self.limits.outbound_count_limit != 0 and self.outgoing.items.len >= self.limits.outbound_count_limit) {
            log.debug("outgoing queue count limit exceeded", .{});
            return error.OutgoingQueueLimitExceeded;
        }

        try self.ensureOutgoingByteBudget(frame.len);
        const owned = try self.copyOutgoingFrame(frame);
        errdefer self.outgoing_allocator.free(owned);
        try self.ensureOutgoingByteBudget(owned.len);

        try self.outgoing.append(self.allocator, owned);
        self.outgoing_bytes += owned.len;
    }

    fn ensureOutgoingByteBudget(self: *HostPeer, added_len: usize) !void {
        if (self.limits.outbound_bytes_limit == 0) return;
        const next = std.math.add(usize, self.outgoing_bytes, added_len) catch {
            log.debug("outgoing bytes limit exceeded", .{});
            return error.OutgoingBytesLimitExceeded;
        };
        if (next > self.limits.outbound_bytes_limit) {
            log.debug("outgoing bytes limit exceeded", .{});
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
