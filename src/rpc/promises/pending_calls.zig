const std = @import("std");
const log = std.log.scoped(.rpc_peer);
const cap_table = @import("../caps/table.zig");
const protocol = @import("../wire/protocol.zig");

pub fn queuePendingCall(
    comptime PendingCallType: type,
    comptime InboundCapsType: type,
    allocator: std.mem.Allocator,
    pending_calls: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    key: u32,
    frame: []const u8,
    inbound_caps: InboundCapsType,
) !void {
    // Ownership contract: on SUCCESS the queued entry takes over `inbound_caps`
    // (its slices), so the caller must not deinit it. On FAILURE the CALLER
    // retains ownership and is responsible for releasing/deiniting it — we do
    // NOT deinit it here. `inbound_caps` is passed by value but its slices are
    // shared with the caller's copy, so deiniting here would double-free the
    // slices the caller's error path also frees.
    const inbound_caps_owned = inbound_caps;

    const copy = try allocator.alloc(u8, frame.len);
    errdefer allocator.free(copy);
    @memcpy(copy, frame);

    // Decode the call's question id once, here, so the per-inbound-message
    // duplicate-id and cancellation scans can match on the stored id instead of
    // re-validating-parsing every queued frame (an O(n)-decode amplifier). A
    // non-call/undecodable frame stores null; question id zero is valid and must
    // remain distinguishable so cancellation/failure drains still settle it.
    // Out of memory is not "undecodable": queueing a real call with a null id
    // would let the failure drain skip its terminal Return, so the enqueue
    // fails instead and the caller answers the call.
    const call_question_id: ?u32 = blk: {
        var decoded = protocol.DecodedMessage.init(allocator, copy) catch |err| {
            if (err == error.OutOfMemory) return error.OutOfMemory;
            break :blk null;
        };
        defer decoded.deinit();
        if (decoded.tag != .call) break :blk null;
        const call = decoded.asCall() catch break :blk null;
        break :blk call.question_id;
    };

    var entry = try pending_calls.getOrPut(key);
    const inserted = !entry.found_existing;
    if (!entry.found_existing) {
        entry.value_ptr.* = std.ArrayList(PendingCallType).empty;
    }
    errdefer if (inserted and entry.value_ptr.items.len == 0) {
        _ = pending_calls.remove(key);
    };
    try entry.value_ptr.append(allocator, .{ .frame = copy, .caps = inbound_caps_owned, .question_id = call_question_id });
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

/// Replays one parked call on a promised-answer target after that answer
/// resolved, running the same target plan a fresh call runs (see
/// `routePromisedTargetCall`). Returns true when the call moved to another
/// queue, which now owns its caps; false when it was answered or dispatched
/// (the drain still owns the caps and releases them). An error means the call
/// was neither queued nor dispatched: the drain answers it with an exception.
pub fn ReplayPromisedCallFn(comptime PeerType: type, comptime InboundCapsType: type) type {
    return *const fn (*PeerType, []const u8, protocol.Call, *InboundCapsType) anyerror!bool;
}

/// Replays one call parked on a promise export after that export resolved to
/// `resolved` (never `.none`; the replay answers those "promise broken"
/// itself). Same return contract as `ReplayPromisedCallFn`.
pub fn ReplayExportCallFn(comptime PeerType: type, comptime InboundCapsType: type) type {
    return *const fn (*PeerType, []const u8, protocol.Call, *InboundCapsType, cap_table.ResolvedCap) anyerror!bool;
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
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    replay_promised_call: ReplayPromisedCallFn(PeerType, InboundCapsType),
) !void {
    const resolved_entry = try resolved_answers.getOrPut(question_id);
    storeResolvedFrame(ResolvedAnswerType, allocator, question_id, resolved_entry, frame);

    drainPendingPromises(
        PeerType,
        PendingCallType,
        InboundCapsType,
        allocator,
        peer,
        question_id,
        pending_promises,
        send_return_exception,
        release_inbound_caps,
        report_nonfatal_error,
        replay_promised_call,
    );
}

/// Like `recordResolvedAnswer`, but the caller must have already reserved one
/// unused slot in `resolved_answers` (via `ensureUnusedCapacity`). This makes
/// the whole operation infallible so it can run AFTER the Return frame is on
/// the wire — a propagating error there would drive the call-dispatch catch to
/// send a second (exception) Return for the same answer, violating the
/// exactly-one-Return-per-call invariant. See `Peer.reserveResolvedAnswer`.
pub fn recordResolvedAnswerAssumeCapacity(
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
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    replay_promised_call: ReplayPromisedCallFn(PeerType, InboundCapsType),
) void {
    const resolved_entry = resolved_answers.getOrPutAssumeCapacity(question_id);
    storeResolvedFrame(ResolvedAnswerType, allocator, question_id, resolved_entry, frame);

    drainPendingPromises(
        PeerType,
        PendingCallType,
        InboundCapsType,
        allocator,
        peer,
        question_id,
        pending_promises,
        send_return_exception,
        release_inbound_caps,
        report_nonfatal_error,
        replay_promised_call,
    );
}

fn storeResolvedFrame(
    comptime ResolvedAnswerType: type,
    allocator: std.mem.Allocator,
    question_id: u32,
    resolved_entry: anytype,
    frame: []u8,
) void {
    if (resolved_entry.found_existing) {
        log.debug("duplicate resolved answer for question_id={d}, replacing", .{question_id});
        allocator.free(resolved_entry.value_ptr.frame);
    }
    resolved_entry.value_ptr.* = ResolvedAnswerType{ .frame = frame };
}

/// Replay, in arrival order, every call parked on answer `question_id`.
/// A replayed call runs the full target plan again, so one whose answer
/// turned out to be an unresolved promise export parks on that export (H10)
/// instead of failing.
fn drainPendingPromises(
    comptime PeerType: type,
    comptime PendingCallType: type,
    comptime InboundCapsType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    question_id: u32,
    pending_promises: *std.AutoHashMap(u32, std.ArrayList(PendingCallType)),
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    replay_promised_call: ReplayPromisedCallFn(PeerType, InboundCapsType),
) void {
    var pending = pending_promises.fetchRemove(question_id) orelse return;
    defer pending.value.deinit(allocator);

    for (pending.value.items) |*pending_call| {
        var caps_owned = true;
        defer if (caps_owned) pending_call.caps.deinit();
        defer allocator.free(pending_call.frame);

        var decoded = protocol.DecodedMessage.init(allocator, pending_call.frame) catch |err| {
            report_nonfatal_error(peer, err);
            continue;
        };
        defer decoded.deinit();
        if (decoded.tag != .call) continue;
        const call = decoded.asCall() catch |err| {
            report_nonfatal_error(peer, err);
            continue;
        };
        if (call.target.promised_answer == null) continue;
        const requeued = replay_promised_call(peer, pending_call.frame, call, &pending_call.caps) catch |err| blk: {
            send_return_exception(peer, call.question_id, @errorName(err)) catch |send_err| {
                report_nonfatal_error(peer, send_err);
            };
            break :blk false;
        };
        if (requeued) {
            caps_owned = false;
            continue;
        }
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
    send_return_exception: *const fn (*PeerType, u32, []const u8) anyerror!void,
    release_inbound_caps: *const fn (*PeerType, *InboundCapsType) anyerror!void,
    report_nonfatal_error: *const fn (*PeerType, anyerror) void,
    replay_export_call: ReplayExportCallFn(PeerType, InboundCapsType),
) !void {
    var pending = pending_export_promises.fetchRemove(export_id) orelse return;
    defer pending.value.deinit(allocator);

    for (pending.value.items) |*pending_call| {
        var caps_owned = true;
        defer if (caps_owned) pending_call.caps.deinit();
        defer allocator.free(pending_call.frame);

        var decoded = protocol.DecodedMessage.init(allocator, pending_call.frame) catch |err| {
            report_nonfatal_error(peer, err);
            continue;
        };
        defer decoded.deinit();
        if (decoded.tag != .call) continue;
        const call = decoded.asCall() catch |err| {
            report_nonfatal_error(peer, err);
            continue;
        };

        if (resolved == .none) {
            send_return_exception(peer, call.question_id, "promise broken") catch |err| {
                report_nonfatal_error(peer, err);
            };
        } else {
            const requeued = replay_export_call(peer, pending_call.frame, call, &pending_call.caps, resolved) catch |err| blk: {
                send_return_exception(peer, call.question_id, @errorName(err)) catch |send_err| {
                    report_nonfatal_error(peer, send_err);
                };
                break :blk false;
            };
            if (requeued) {
                caps_owned = false;
                continue;
            }
        }

        release_inbound_caps(peer, &pending_call.caps) catch |err| {
            report_nonfatal_error(peer, err);
        };
    }
}

test "pending_calls queuePendingCall clones frame and appends" {
    const DummyCaps = struct {
        fn deinit(_: *@This()) void {}
    };
    const PendingCall = struct {
        frame: []u8,
        caps: DummyCaps,
        question_id: ?u32 = null,
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
    try std.testing.expectEqual(@as(?u32, null), entry.items[0].question_id);
    try std.testing.expectEqual(@as(?u32, null), entry.items[1].question_id);
}

test "pending_calls deinitPendingCallOwnedFrameForPeerFn releases caps and frame" {
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
        question_id: ?u32 = null,
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

test "pending_calls replayResolvedPromiseExport none sends exception and releases caps" {
    const DummyCaps = struct {
        fn deinit(_: *@This()) void {}
    };
    const PendingCall = struct {
        frame: []u8,
        caps: DummyCaps,
        question_id: ?u32 = null,
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
        fn replayExportCall(
            peer: *FakePeer,
            frame: []const u8,
            call: protocol.Call,
            inbound_caps: *DummyCaps,
            resolved: cap_table.ResolvedCap,
        ) !bool {
            _ = frame;
            _ = call;
            _ = inbound_caps;
            _ = resolved;
            peer.handled_count += 1;
            return false;
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
            // This fake only counts reports; the assertion below is that the
            // count stays 0, so the specific error is deliberately unused.
            // Discard through a pointer (`_ = &err`, the same idiom std uses in
            // `debug.simple_panic.unwrapError`): a plain `_ = err;` is rejected
            // because discarding a value of error-set type is a compile error.
            _ = &err;
            peer.nonfatal_count += 1;
        }
    };

    var pending = std.AutoHashMap(u32, std.ArrayList(PendingCall)).init(std.testing.allocator);
    defer pending.deinit();

    var call_builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer call_builder.deinit();
    var call = try call_builder.beginCall(41, 0xAA, 2);
    try call.setTargetImportedCap(7);
    _ = try call.initCapTableTyped(0);

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
        Hooks.sendReturnException,
        Hooks.releaseInboundCaps,
        Hooks.reportNonfatal,
        Hooks.replayExportCall,
    );

    try std.testing.expectEqual(@as(usize, 1), peer.exception_count);
    try std.testing.expectEqual(@as(u32, 41), peer.last_question_id);
    try std.testing.expectEqualStrings("promise broken", peer.last_reason);
    try std.testing.expectEqual(@as(usize, 1), peer.release_count);
    try std.testing.expectEqual(@as(usize, 0), peer.handled_count);
    try std.testing.expectEqual(@as(usize, 0), peer.nonfatal_count);
    try std.testing.expect(!pending.contains(99));
}
