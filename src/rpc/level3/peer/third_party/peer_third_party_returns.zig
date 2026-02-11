const std = @import("std");
const protocol = @import("../../../level0/protocol.zig");

pub fn hasPendingReturn(
    pending_returns: *const std.AutoHashMap(u32, []u8),
    answer_id: u32,
) bool {
    return pending_returns.contains(answer_id);
}

pub fn hasPendingReturnForPeer(comptime PeerType: type, peer: *PeerType, answer_id: u32) bool {
    return hasPendingReturn(&peer.pending_third_party_returns, answer_id);
}

pub fn hasPendingReturnForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32) bool {
    return struct {
        fn call(peer: *PeerType, answer_id: u32) bool {
            return hasPendingReturnForPeer(PeerType, peer, answer_id);
        }
    }.call;
}

pub fn bufferPendingReturn(
    allocator: std.mem.Allocator,
    pending_returns: *std.AutoHashMap(u32, []u8),
    answer_id: u32,
    frame: []const u8,
) !void {
    const copy = try allocator.alloc(u8, frame.len);
    errdefer allocator.free(copy);
    std.mem.copyForwards(u8, copy, frame);
    try pending_returns.put(answer_id, copy);
}

pub fn bufferPendingReturnForPeer(comptime PeerType: type, peer: *PeerType, answer_id: u32, frame: []const u8) !void {
    try bufferPendingReturn(peer.allocator, &peer.pending_third_party_returns, answer_id, frame);
}

pub fn bufferPendingReturnForPeerFn(comptime PeerType: type) *const fn (*PeerType, u32, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, answer_id: u32, frame: []const u8) anyerror!void {
            try bufferPendingReturnForPeer(PeerType, peer, answer_id, frame);
        }
    }.call;
}

pub fn takePendingReturnFrame(
    pending_returns: *std.AutoHashMap(u32, []u8),
    answer_id: u32,
) ?[]u8 {
    if (pending_returns.fetchRemove(answer_id)) |pending| {
        return pending.value;
    }
    return null;
}

pub fn handlePendingReturnFrame(
    comptime PeerType: type,
    allocator: std.mem.Allocator,
    peer: *PeerType,
    frame: []const u8,
    on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) !void {
    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    if (decoded.tag != .return_) return error.UnexpectedMessage;
    try on_return(peer, frame, try decoded.asReturn());
}

pub fn handlePendingReturnFrameForPeer(
    comptime PeerType: type,
    peer: *PeerType,
    frame: []const u8,
    on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) !void {
    try handlePendingReturnFrame(PeerType, peer.allocator, peer, frame, on_return);
}

pub fn handlePendingReturnFrameForPeerFn(
    comptime PeerType: type,
    comptime on_return: *const fn (*PeerType, []const u8, protocol.Return) anyerror!void,
) *const fn (*PeerType, []const u8) anyerror!void {
    return struct {
        fn call(peer: *PeerType, frame: []const u8) anyerror!void {
            try handlePendingReturnFrameForPeer(PeerType, peer, frame, on_return);
        }
    }.call;
}

test "peer_third_party_returns buffer/has/take lifecycle clones frame bytes" {
    var pending_returns = std.AutoHashMap(u32, []u8).init(std.testing.allocator);
    defer {
        var it = pending_returns.valueIterator();
        while (it.next()) |frame| {
            std.testing.allocator.free(frame.*);
        }
        pending_returns.deinit();
    }

    var source = [_]u8{ 1, 2, 3, 4 };
    try bufferPendingReturn(
        std.testing.allocator,
        &pending_returns,
        33,
        source[0..],
    );
    source[0] = 9;

    try std.testing.expect(hasPendingReturn(&pending_returns, 33));
    const stored = takePendingReturnFrame(&pending_returns, 33) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(stored);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 1, 2, 3, 4 }, stored);
    try std.testing.expect(!hasPendingReturn(&pending_returns, 33));
}

test "peer_third_party_returns handlePendingReturnFrame rejects non-return frames" {
    const FakePeer = struct {};
    const Hooks = struct {
        fn onReturn(peer: *FakePeer, frame: []const u8, ret: protocol.Return) !void {
            _ = peer;
            _ = frame;
            _ = ret;
            return error.TestUnexpectedResult;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    _ = try builder.beginCall(1, 0xAB, 2);
    const call_frame = try builder.finish();
    defer std.testing.allocator.free(call_frame);

    var peer = FakePeer{};
    try std.testing.expectError(
        error.UnexpectedMessage,
        handlePendingReturnFrame(
            FakePeer,
            std.testing.allocator,
            &peer,
            call_frame,
            Hooks.onReturn,
        ),
    );
}

test "peer_third_party_returns handlePendingReturnFrame decodes and forwards return" {
    const FakePeer = struct {
        called: bool = false,
        seen_answer_id: u32 = 0,
        seen_tag: protocol.ReturnTag = .exception,
    };
    const Hooks = struct {
        fn onReturn(peer: *FakePeer, frame: []const u8, ret: protocol.Return) !void {
            _ = frame;
            peer.called = true;
            peer.seen_answer_id = ret.answer_id;
            peer.seen_tag = ret.tag;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    _ = try builder.beginReturn(42, .canceled);
    const return_frame = try builder.finish();
    defer std.testing.allocator.free(return_frame);

    var peer = FakePeer{};
    try handlePendingReturnFrame(
        FakePeer,
        std.testing.allocator,
        &peer,
        return_frame,
        Hooks.onReturn,
    );

    try std.testing.expect(peer.called);
    try std.testing.expectEqual(@as(u32, 42), peer.seen_answer_id);
    try std.testing.expectEqual(protocol.ReturnTag.canceled, peer.seen_tag);
}

test "peer_third_party_returns peer helper factories use peer-owned pending return map" {
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        pending_third_party_returns: std.AutoHashMap(u32, []u8),

        fn init(allocator: std.mem.Allocator) @This() {
            return .{
                .allocator = allocator,
                .pending_third_party_returns = std.AutoHashMap(u32, []u8).init(allocator),
            };
        }

        fn deinit(self: *@This()) void {
            var it = self.pending_third_party_returns.valueIterator();
            while (it.next()) |frame| self.allocator.free(frame.*);
            self.pending_third_party_returns.deinit();
        }
    };

    var peer = FakePeer.init(std.testing.allocator);
    defer peer.deinit();

    const has_pending = hasPendingReturnForPeerFn(FakePeer);
    const buffer_pending = bufferPendingReturnForPeerFn(FakePeer);

    try std.testing.expect(!has_pending(&peer, 91));

    var source = [_]u8{ 9, 1, 2 };
    try buffer_pending(&peer, 91, source[0..]);
    source[0] = 7;

    try std.testing.expect(has_pending(&peer, 91));
    const stored = takePendingReturnFrame(&peer.pending_third_party_returns, 91) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(stored);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 9, 1, 2 }, stored);
    try std.testing.expect(!has_pending(&peer, 91));
}

test "peer_third_party_returns handlePendingReturnFrameForPeerFn decodes and dispatches via peer allocator" {
    const FakePeer = struct {
        allocator: std.mem.Allocator,
        called: bool = false,
        seen_answer_id: u32 = 0,

        fn onReturn(self: *@This(), frame: []const u8, ret: protocol.Return) !void {
            _ = frame;
            self.called = true;
            self.seen_answer_id = ret.answer_id;
        }
    };

    var builder = protocol.MessageBuilder.init(std.testing.allocator);
    defer builder.deinit();
    _ = try builder.beginReturn(123, .canceled);
    const return_frame = try builder.finish();
    defer std.testing.allocator.free(return_frame);

    var peer = FakePeer{
        .allocator = std.testing.allocator,
    };
    const handle_pending = handlePendingReturnFrameForPeerFn(FakePeer, FakePeer.onReturn);
    try handle_pending(&peer, return_frame);

    try std.testing.expect(peer.called);
    try std.testing.expectEqual(@as(u32, 123), peer.seen_answer_id);
}
