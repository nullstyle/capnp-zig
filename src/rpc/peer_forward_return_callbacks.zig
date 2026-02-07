const std = @import("std");
const message = @import("../message.zig");
const peer_control = @import("peer_control.zig");
const protocol = @import("protocol.zig");

pub fn handleForwardedReturnWithPeerCallbacks(
    comptime PeerType: type,
    comptime InboundCapsType: type,
    comptime BuildCtxType: type,
    comptime build_forwarded_return: *const fn (*anyopaque, *protocol.ReturnBuilder) anyerror!void,
    peer: *PeerType,
    mode: peer_control.ForwardedReturnMode,
    answer_id: u32,
    ret: protocol.Return,
    inbound_caps: *const InboundCapsType,
    capture_payload: *const fn (*PeerType, ?message.AnyPointerReader) anyerror!?[]u8,
    free_payload: *const fn (*PeerType, []u8) void,
    context_third_party_payload: ?[]const u8,
    comptime send_return_tag: *const fn (*PeerType, u32, protocol.ReturnTag) anyerror!void,
    comptime lookup_forwarded_question: *const fn (*PeerType, u32) ?u32,
    comptime send_take_from_other_question: *const fn (*PeerType, u32, u32) anyerror!void,
    comptime send_accept_from_third_party: *const fn (*PeerType, u32, ?[]const u8) anyerror!void,
) !void {
    const Adapters = struct {
        fn sendReturnResults(
            p: *PeerType,
            forwarded_answer_id: u32,
            payload: protocol.Payload,
            caps: *const InboundCapsType,
        ) !void {
            var build_ctx = BuildCtxType{
                .peer = p,
                .payload = payload,
                .inbound_caps = caps,
            };
            p.sendReturnResults(forwarded_answer_id, &build_ctx, build_forwarded_return) catch |err| {
                try p.sendReturnException(forwarded_answer_id, @errorName(err));
            };
        }

        fn sendReturnException(p: *PeerType, forwarded_answer_id: u32, reason: []const u8) !void {
            try p.sendReturnException(forwarded_answer_id, reason);
        }

        fn sendReturnTag(p: *PeerType, forwarded_answer_id: u32, tag: protocol.ReturnTag) !void {
            try send_return_tag(p, forwarded_answer_id, tag);
        }

        fn lookupForwardedQuestion(p: *PeerType, local_question_id: u32) ?u32 {
            return lookup_forwarded_question(p, local_question_id);
        }

        fn sendTakeFromOtherQuestion(
            p: *PeerType,
            forwarded_answer_id: u32,
            other_question_id: u32,
        ) !void {
            try send_take_from_other_question(p, forwarded_answer_id, other_question_id);
        }

        fn sendAcceptFromThirdParty(
            p: *PeerType,
            forwarded_answer_id: u32,
            await_payload: ?[]const u8,
        ) !void {
            try send_accept_from_third_party(p, forwarded_answer_id, await_payload);
        }
    };

    try peer_control.handleForwardedReturn(
        PeerType,
        InboundCapsType,
        peer,
        mode,
        answer_id,
        ret,
        inbound_caps,
        Adapters.sendReturnResults,
        Adapters.sendReturnException,
        Adapters.sendReturnTag,
        Adapters.lookupForwardedQuestion,
        Adapters.sendTakeFromOtherQuestion,
        capture_payload,
        free_payload,
        Adapters.sendAcceptFromThirdParty,
        context_third_party_payload,
    );
}

test "peer_forward_return_callbacks results fallback sends exception on sendReturnResults failure" {
    const InboundCaps = struct {};

    const FakePeer = struct {
        allocator: std.mem.Allocator,
        forwarded_questions: std.AutoHashMap(u32, u32),
        send_results_calls: usize = 0,
        send_exception_calls: usize = 0,
        last_exception_reason: ?[]u8 = null,
        fail_send_results: bool = true,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            if (self.last_exception_reason) |reason| self.allocator.free(reason);
            self.forwarded_questions.deinit();
        }

        fn sendReturnResults(
            self: *@This(),
            answer_id: u32,
            ctx_ptr: *anyopaque,
            build: *const fn (*anyopaque, *protocol.ReturnBuilder) anyerror!void,
        ) !void {
            _ = answer_id;
            _ = ctx_ptr;
            _ = build;
            self.send_results_calls += 1;
            if (self.fail_send_results) return error.TestExpectedError;
        }

        fn sendReturnException(self: *@This(), answer_id: u32, reason: []const u8) !void {
            _ = answer_id;
            self.send_exception_calls += 1;
            const reason_copy = try self.allocator.dupe(u8, reason);
            if (self.last_exception_reason) |existing| self.allocator.free(existing);
            self.last_exception_reason = reason_copy;
        }

        fn sendReturnTag(self: *@This(), answer_id: u32, tag: protocol.ReturnTag) !void {
            _ = self;
            _ = answer_id;
            _ = tag;
        }

        fn sendReturnTakeFromOtherQuestion(self: *@This(), answer_id: u32, other_question_id: u32) !void {
            _ = self;
            _ = answer_id;
            _ = other_question_id;
        }

        fn sendReturnAcceptFromThirdParty(self: *@This(), answer_id: u32, await_payload: ?[]const u8) !void {
            _ = self;
            _ = answer_id;
            _ = await_payload;
        }
    };

    const BuildCtx = struct {
        peer: *FakePeer,
        payload: protocol.Payload,
        inbound_caps: *const InboundCaps,
    };

    const Hooks = struct {
        fn capture(peer: *FakePeer, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = peer;
            _ = ptr;
            return null;
        }

        fn free(peer: *FakePeer, payload: []u8) void {
            peer.allocator.free(payload);
        }

        fn build(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) !void {
            _ = ctx_ptr;
            _ = ret_builder;
        }

        fn lookup(peer: *FakePeer, local_question_id: u32) ?u32 {
            return peer.forwarded_questions.get(local_question_id);
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const inbound = InboundCaps{};
    const ret = protocol.Return{
        .answer_id = 99,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results,
        .results = .{
            .content = undefined,
            .cap_table = null,
        },
        .exception = null,
        .take_from_other_question = null,
    };

    try handleForwardedReturnWithPeerCallbacks(
        FakePeer,
        InboundCaps,
        BuildCtx,
        Hooks.build,
        &peer,
        .translate_to_caller,
        44,
        ret,
        &inbound,
        Hooks.capture,
        Hooks.free,
        null,
        FakePeer.sendReturnTag,
        Hooks.lookup,
        FakePeer.sendReturnTakeFromOtherQuestion,
        FakePeer.sendReturnAcceptFromThirdParty,
    );

    try std.testing.expectEqual(@as(usize, 1), peer.send_results_calls);
    try std.testing.expectEqual(@as(usize, 1), peer.send_exception_calls);
    try std.testing.expectEqualStrings("TestExpectedError", peer.last_exception_reason orelse return error.MissingException);
}

test "peer_forward_return_callbacks translates takeFromOtherQuestion via forwarded map" {
    const InboundCaps = struct {};

    const FakePeer = struct {
        forwarded_questions: std.AutoHashMap(u32, u32),
        take_calls: usize = 0,
        take_answer_id: u32 = 0,
        take_other_id: u32 = 0,

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.forwarded_questions.deinit();
        }

        fn sendReturnResults(
            self: *@This(),
            answer_id: u32,
            ctx_ptr: *anyopaque,
            build: *const fn (*anyopaque, *protocol.ReturnBuilder) anyerror!void,
        ) !void {
            _ = self;
            _ = answer_id;
            _ = ctx_ptr;
            _ = build;
        }

        fn sendReturnException(self: *@This(), answer_id: u32, reason: []const u8) !void {
            _ = self;
            _ = answer_id;
            _ = reason;
        }

        fn sendReturnTag(self: *@This(), answer_id: u32, tag: protocol.ReturnTag) !void {
            _ = self;
            _ = answer_id;
            _ = tag;
        }

        fn sendReturnTakeFromOtherQuestion(self: *@This(), answer_id: u32, other_question_id: u32) !void {
            self.take_calls += 1;
            self.take_answer_id = answer_id;
            self.take_other_id = other_question_id;
        }

        fn sendReturnAcceptFromThirdParty(self: *@This(), answer_id: u32, await_payload: ?[]const u8) !void {
            _ = self;
            _ = answer_id;
            _ = await_payload;
        }
    };

    const BuildCtx = struct {
        peer: *FakePeer,
        payload: protocol.Payload,
        inbound_caps: *const InboundCaps,
    };

    const Hooks = struct {
        fn capture(peer: *FakePeer, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = peer;
            _ = ptr;
            return null;
        }

        fn free(peer: *FakePeer, payload: []u8) void {
            _ = peer;
            _ = payload;
        }

        fn build(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) !void {
            _ = ctx_ptr;
            _ = ret_builder;
        }

        fn lookup(peer: *FakePeer, local_question_id: u32) ?u32 {
            return peer.forwarded_questions.get(local_question_id);
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();
    try peer.forwarded_questions.put(91, 500);

    const inbound = InboundCaps{};
    const ret = protocol.Return{
        .answer_id = 77,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .take_from_other_question,
        .results = null,
        .exception = null,
        .take_from_other_question = 91,
    };

    try handleForwardedReturnWithPeerCallbacks(
        FakePeer,
        InboundCaps,
        BuildCtx,
        Hooks.build,
        &peer,
        .translate_to_caller,
        42,
        ret,
        &inbound,
        Hooks.capture,
        Hooks.free,
        null,
        FakePeer.sendReturnTag,
        Hooks.lookup,
        FakePeer.sendReturnTakeFromOtherQuestion,
        FakePeer.sendReturnAcceptFromThirdParty,
    );

    try std.testing.expectEqual(@as(usize, 1), peer.take_calls);
    try std.testing.expectEqual(@as(u32, 42), peer.take_answer_id);
    try std.testing.expectEqual(@as(u32, 500), peer.take_other_id);
}

test "peer_forward_return_callbacks propagate-accept mode uses context payload on resultsSentElsewhere" {
    const InboundCaps = struct {};

    const FakePeer = struct {
        accept_calls: usize = 0,
        accept_answer_id: u32 = 0,
        accept_payload: ?[]const u8 = null,
        forwarded_questions: std.AutoHashMap(u32, u32),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .forwarded_questions = std.AutoHashMap(u32, u32).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            self.forwarded_questions.deinit();
        }

        fn sendReturnResults(
            self: *@This(),
            answer_id: u32,
            ctx_ptr: *anyopaque,
            build: *const fn (*anyopaque, *protocol.ReturnBuilder) anyerror!void,
        ) !void {
            _ = self;
            _ = answer_id;
            _ = ctx_ptr;
            _ = build;
        }

        fn sendReturnException(self: *@This(), answer_id: u32, reason: []const u8) !void {
            _ = self;
            _ = answer_id;
            _ = reason;
        }

        fn sendReturnTag(self: *@This(), answer_id: u32, tag: protocol.ReturnTag) !void {
            _ = self;
            _ = answer_id;
            _ = tag;
        }

        fn sendReturnTakeFromOtherQuestion(self: *@This(), answer_id: u32, other_question_id: u32) !void {
            _ = self;
            _ = answer_id;
            _ = other_question_id;
        }

        fn sendReturnAcceptFromThirdParty(self: *@This(), answer_id: u32, await_payload: ?[]const u8) !void {
            self.accept_calls += 1;
            self.accept_answer_id = answer_id;
            self.accept_payload = await_payload;
        }
    };

    const BuildCtx = struct {
        peer: *FakePeer,
        payload: protocol.Payload,
        inbound_caps: *const InboundCaps,
    };

    const Hooks = struct {
        fn capture(peer: *FakePeer, ptr: ?message.AnyPointerReader) !?[]u8 {
            _ = peer;
            _ = ptr;
            return null;
        }

        fn free(peer: *FakePeer, payload: []u8) void {
            _ = peer;
            _ = payload;
        }

        fn build(ctx_ptr: *anyopaque, ret_builder: *protocol.ReturnBuilder) !void {
            _ = ctx_ptr;
            _ = ret_builder;
        }

        fn lookup(peer: *FakePeer, local_question_id: u32) ?u32 {
            return peer.forwarded_questions.get(local_question_id);
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const inbound = InboundCaps{};
    const ret = protocol.Return{
        .answer_id = 88,
        .release_param_caps = false,
        .no_finish_needed = false,
        .tag = .results_sent_elsewhere,
        .results = null,
        .exception = null,
        .take_from_other_question = null,
    };

    const context_payload = "context-third-party";
    try handleForwardedReturnWithPeerCallbacks(
        FakePeer,
        InboundCaps,
        BuildCtx,
        Hooks.build,
        &peer,
        .propagate_accept_from_third_party,
        55,
        ret,
        &inbound,
        Hooks.capture,
        Hooks.free,
        context_payload,
        FakePeer.sendReturnTag,
        Hooks.lookup,
        FakePeer.sendReturnTakeFromOtherQuestion,
        FakePeer.sendReturnAcceptFromThirdParty,
    );

    try std.testing.expectEqual(@as(usize, 1), peer.accept_calls);
    try std.testing.expectEqual(@as(u32, 55), peer.accept_answer_id);
    try std.testing.expectEqualStrings(
        context_payload,
        peer.accept_payload orelse return error.MissingThirdPartyPayload,
    );
}
