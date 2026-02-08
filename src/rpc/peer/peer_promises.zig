const std = @import("std");
const cap_table = @import("../cap_table.zig");
const protocol = @import("../protocol.zig");

pub fn queuePendingCall(
    comptime PendingCallType: type,
    comptime InboundCapsType: type,
    allocator: std.mem.Allocator,
    pending_calls: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    key: u32,
    frame: []const u8,
    inbound_caps: InboundCapsType,
) !void {
    const copy = try allocator.alloc(u8, frame.len);
    errdefer allocator.free(copy);
    std.mem.copyForwards(u8, copy, frame);

    var entry = try pending_calls.getOrPut(key);
    if (!entry.found_existing) {
        entry.value_ptr.* = std.ArrayList(PendingCallType){};
    }
    try entry.value_ptr.append(allocator, .{ .frame = copy, .caps = inbound_caps });
}

pub fn deinitPendingCallOwnedFrame(comptime PendingCallType: type, pending_call: *PendingCallType, allocator: std.mem.Allocator) void {
    pending_call.caps.deinit();
    allocator.free(pending_call.frame);
}

pub fn deinitPendingCallOwnedFrameForPeerFn(
    comptime PeerType: type,
    comptime PendingCallType: type,
) *const fn (*PeerType, *PendingCallType, std.mem.Allocator) void {
    return struct {
        fn call(peer: *PeerType, pending_call: *PendingCallType, allocator: std.mem.Allocator) void {
            _ = peer;
            deinitPendingCallOwnedFrame(PendingCallType, pending_call, allocator);
        }
    }.call;
}

pub fn recordResolvedAnswer(
    comptime PeerType: type,
    comptime ResolvedAnswerType: type,
    comptime PendingCallType: type,
    comptime InboundCapsType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    question_id: u32,
    frame: []u8,
    resolved_answers: *std.AutoHashMap(u32, ResolvedAnswerType),
    pending_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    resolve_promised_answer: *const fn (*PeerType, protocol.PromisedAnswer) anyerror!cap_table.ResolvedCap,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) !void {
    if (resolved_answers.fetchRemove(question_id)) |existing| {
        allocator.free(existing.value.frame);
    }
    _ = try resolved_answers.put(question_id, .{ .frame = frame });

    var pending = pending_promises.fetchRemove(question_id) orelse return;
    defer pending.value.deinit(allocator);

    for (pending.value.items) |*pending_call| {
        defer pending_call.caps.deinit();
        defer allocator.free(pending_call.frame);

        var decoded = try protocol.DecodedMessage.init(allocator, pending_call.frame);
        defer decoded.deinit();
        if (decoded.tag != .call) continue;
        const call = try decoded.asCall();
        const promised = call.target.promised_answer orelse continue;
        const resolved = resolve_promised_answer(peer, promised) catch |err| {
            try send_return_exception(peer, call.question_id, @errorName(err));
            continue;
        };
        handle_resolved_call(peer, call, &pending_call.caps, resolved) catch |err| {
            report_nonfatal_error(peer, err);
        };
        release_inbound_caps(peer, &pending_call.caps) catch |err| {
            report_nonfatal_error(peer, err);
        };
    }
}

pub fn replayResolvedPromiseExport(
    comptime PeerType: type,
    comptime PendingCallType: type,
    comptime InboundCapsType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    export_id: u32,
    resolved: cap_table.ResolvedCap,
    pending_export_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    handle_resolved_call: *const fn (*PeerType, protocol.Call, *const InboundCapsType, cap_table.ResolvedCap) anyerror!void,
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
) !void {
    var pending = pending_export_promises.fetchRemove(export_id) orelse return;
    defer pending.value.deinit(allocator);

    for (pending.value.items) |*pending_call| {
        defer pending_call.caps.deinit();
        defer allocator.free(pending_call.frame);

        var decoded = try protocol.DecodedMessage.init(allocator, pending_call.frame);
        defer decoded.deinit();
        if (decoded.tag != .call) continue;
        const call = try decoded.asCall();

        if (resolved == .none) {
            try send_return_exception(peer, call.question_id, "promise broken");
        } else {
            handle_resolved_call(peer, call, &pending_call.caps, resolved) catch |err| {
                report_nonfatal_error(peer, err);
            };
        }

        release_inbound_caps(peer, &pending_call.caps) catch |err| {
            report_nonfatal_error(peer, err);
        };
    }
}

test "peer_promises queuePendingCall clones frame and appends" {
    const DummyCaps = struct {
        fn deinit(_: *@This()) void {}
    };
    const PendingCall = struct {
        frame: []u8,
        caps: DummyCaps,
    };

    var pending = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(std.testing.allocator);
    defer {
        var it = pending.valueIterator();
        while (it.next()) |list| {
            for (list.items) |item| {
                std.testing.allocator.free(item.frame);
            }
            list.deinit(std.testing.allocator);
        }
        pending.deinit();
    }

    var source_a = [_]u8{ 1, 2, 3 };
    try queuePendingCall(
        PendingCall,
        DummyCaps,
        std.testing.allocator,
        &pending,
        17,
        source_a[0..],
        .{},
    );
    source_a[0] = 9;

    var source_b = [_]u8{ 4, 5 };
    try queuePendingCall(
        PendingCall,
        DummyCaps,
        std.testing.allocator,
        &pending,
        17,
        source_b[0..],
        .{},
    );

    const entry = pending.getPtr(17) orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(usize, 2), entry.items.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3 }, entry.items[0].frame);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 4, 5 }, entry.items[1].frame);
}

test "peer_promises deinitPendingCallOwnedFrameForPeerFn releases caps and frame" {
    const State = struct {
        deinit_calls: usize = 0,
    };
    const DummyCaps = struct {
        state: *State,

        fn deinit(self: *@This()) void {
            self.state.deinit_calls += 1;
        }
    };
    const PendingCall = struct {
        frame: []u8,
        caps: DummyCaps,
    };
    const Peer = struct {};

    var state = State{};
    const frame = try std.testing.allocator.alloc(u8, 3);
    var pending_call = PendingCall{
        .frame = frame,
        .caps = .{ .state = &state },
    };
    var peer = Peer{};

    const deinit_pending = deinitPendingCallOwnedFrameForPeerFn(Peer, PendingCall);
    deinit_pending(&peer, &pending_call, std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), state.deinit_calls);
}

test "peer_promises replayResolvedPromiseExport none sends exception and releases caps" {
    const DummyCaps = struct {
        fn deinit(_: *@This()) void {}
    };
    const PendingCall = struct {
        frame: []u8,
        caps: DummyCaps,
    };
    const FakePeer = struct {
        exception_count: usize = 0,
        release_count: usize = 0,
        handled_count: usize = 0,
        nonfatal_count: usize = 0,
        last_question_id: u32 = 0,
        last_reason: []const u8 = "",
    };

    const Hooks = struct {
        fn handleResolvedCall(
            peer: *FakePeer,
            call: protocol.Call,
            inbound_caps: *const DummyCaps,
            resolved: cap_table.ResolvedCap,
        ) !void {
            _ = call;
            _ = inbound_caps;
            _ = resolved;
            peer.handled_count += 1;
        }

        fn sendReturnException(peer: *FakePeer, question_id: u32, reason: []const u8) !void {
            peer.exception_count += 1;
            peer.last_question_id = question_id;
            peer.last_reason = reason;
        }

        fn releaseInboundCaps(peer: *FakePeer, inbound_caps: *DummyCaps) !void {
            _ = inbound_caps;
            peer.release_count += 1;
        }

        fn reportNonfatal(peer: *FakePeer, err: anyerror) void {
            _ = err;
            peer.nonfatal_count += 1;
        }
    };

    var pending = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    var call_builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(41, 0xAA, 2);
    try call.setTargetImportedCap(7);
    try call.setEmptyCapTable();
    const call_frame = try call_builder.finish();
    defer std.testing.allocator.free(call_frame);

    try queuePendingCall(
        PendingCall,
        DummyCaps,
        std.testing.allocator,
        &pending,
        99,
        call_frame,
        .{},
    );

    var peer = FakePeer{};
    try replayResolvedPromiseExport(
        FakePeer,
        PendingCall,
        DummyCaps,
        std.testing.allocator,
        &peer,
        99,
        .none,
        &pending,
        Hooks.handleResolvedCall,
        Hooks.sendReturnException,
        Hooks.releaseInboundCaps,
        Hooks.reportNonfatal,
    );

    try std.testing.expectEqual(@as(usize, 1), peer.exception_count);
    try std.testing.expectEqual(@as(u32, 41), peer.last_question_id);
    try std.testing.expectEqualStrings("promise broken", peer.last_reason);
    try std.testing.expectEqual(@as(usize, 1), peer.release_count);
    try std.testing.expectEqual(@as(usize, 0), peer.handled_count);
    try std.testing.expectEqual(@as(usize, 0), peer.nonfatal_count);
    try std.testing.expect(!pending.contains(99));
}
