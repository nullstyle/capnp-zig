const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const rpc = capnpc.rpc;
const protocol = rpc.wire.protocol;
const Peer = rpc.peer.Peer;

const CompoundMessage = struct {
    bytes: []const u8,
    packed_bytes: []const u8,
};

fn makeCompoundMessage(allocator: std.mem.Allocator) !CompoundMessage {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(2, 3);
    root.writeU64(0, 0xfedc_ba98_7654_3210);
    root.writeU32(8, 0xfeed_face);
    root.writeBool(12, 0, true);
    root.writeBool(12, 7, true);
    try root.writeText(0, "hardening-smoke");
    try root.writeData(1, &.{ 0, 0xff, 0x7e, 0x81, 0, 0x42 });

    var words = try root.writeU32List(2, 8);
    for (0..words.len()) |i| {
        const idx: u32 = @intCast(i);
        try words.set(idx, 0xa5a5_0000 ^ (idx *% 0x1f1f_0101));
    }

    return .{
        .bytes = try builder.toBytes(),
        .packed_bytes = try builder.toPackedBytes(),
    };
}

fn exerciseMessageDecode(allocator: std.mem.Allocator, bytes: []const u8) !void {
    var decoded = try message.Message.init(allocator, bytes, .{
        .segment_count_limit = 4,
        .total_segment_words_limit = 64,
        .traversal_limit_words = 128,
        .inline_composite_element_limit = 32,
        .nesting_limit = 8,
    });
    defer decoded.deinit();

    const root = try decoded.getRootStruct();
    try std.testing.expectEqual(@as(u64, 0xfedc_ba98_7654_3210), root.readU64(0));
    try std.testing.expectEqual(@as(u32, 0xfeed_face), root.readU32(8));
    try std.testing.expect(root.readBool(12, 0));
    try std.testing.expect(root.readBool(12, 7));
    try std.testing.expectEqualStrings("hardening-smoke", try root.readText(0));
    try std.testing.expectEqualSlices(u8, &.{ 0, 0xff, 0x7e, 0x81, 0, 0x42 }, try root.readData(1));

    const words = try root.readU32List(2);
    try std.testing.expectEqual(@as(u32, 8), words.len());
    try std.testing.expectEqual(@as(u32, 0xa5a5_0000), try words.get(0));
}

fn exercisePackedDecode(allocator: std.mem.Allocator, packed_bytes: []const u8) !void {
    var decoded = try message.Message.initPacked(allocator, packed_bytes, .{
        .segment_count_limit = 4,
        .total_segment_words_limit = 64,
        .traversal_limit_words = 128,
        .inline_composite_element_limit = 32,
        .nesting_limit = 8,
    });
    defer decoded.deinit();

    const root = try decoded.getRootStruct();
    try std.testing.expectEqualStrings("hardening-smoke", try root.readText(0));
    try std.testing.expectEqual(@as(u32, 0xfeed_face), root.readU32(8));
}

fn exerciseHostileMessageDecode(allocator: std.mem.Allocator) !void {
    const hostile_cases = [_][]const u8{
        "",
        &.{ 0xff, 0xff, 0xff, 0xff },
        &.{ 0, 0, 0, 0, 1, 0, 0, 0 },
        &.{ 1, 0, 0, 0, 1, 0, 0, 0 },
        &.{ 0, 0, 0, 0, 1, 0, 0, 0, 0xff, 0xff, 0xff, 0xff },
    };

    for (hostile_cases) |case| {
        var decoded = message.Message.init(allocator, case, .{
            .segment_count_limit = 8,
            .total_segment_words_limit = 16,
            .traversal_limit_words = 16,
            .inline_composite_element_limit = 16,
            .nesting_limit = 4,
        }) catch continue;
        decoded.deinit();
    }

    const hostile_packed_cases = [_][]const u8{
        &.{0},
        &.{ 0xff, 0 },
        &.{ 0x01, 0xaa },
        &.{ 0x00, 0xff, 0xff },
        &.{ 0xff, 1, 2, 3 },
    };

    for (hostile_packed_cases) |case| {
        var decoded = message.Message.initPacked(allocator, case, .{
            .segment_count_limit = 8,
            .total_segment_words_limit = 16,
            .traversal_limit_words = 16,
            .inline_composite_element_limit = 16,
            .nesting_limit = 4,
        }) catch continue;
        decoded.deinit();
    }
}

fn exerciseRpcFramer(allocator: std.mem.Allocator, frame: []const u8) !void {
    var framer = rpc.wire.framing.Framer.initWithOptions(allocator, .{ .max_buffered_bytes = 4096 });
    defer framer.deinit();

    try framer.push(frame[0..3]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(frame[3..]);
    const popped = (try framer.popFrame()).?;
    defer allocator.free(popped);
    try std.testing.expectEqualSlices(u8, frame, popped);

    const hostile_streams = [_][]const u8{
        &.{ 0xff, 0xff, 0xff, 0xff },
        &.{ 0, 2, 0, 0, 0xff, 0xff, 0xff, 0xff },
        &.{ 0, 0, 0, 0, 0xff, 0xff, 0xff, 0x7f },
    };

    for (hostile_streams) |stream| {
        var hostile = rpc.wire.framing.Framer.initWithOptions(allocator, .{ .max_buffered_bytes = 64 });
        defer hostile.deinit();
        hostile.push(stream) catch continue;
        _ = hostile.popFrame() catch {
            hostile.reset();
            continue;
        };
    }
}

fn exerciseRpcProtocol(allocator: std.mem.Allocator) ![]const u8 {
    var builder = protocol.MessageBuilder.init(allocator);
    defer builder.deinit();
    try builder.buildAbort("fuzz-smoke-abort");
    const frame = try builder.finish();

    var decoded = try protocol.DecodedMessage.init(allocator, frame);
    defer decoded.deinit();
    const abort = try decoded.asAbort();
    try std.testing.expectEqualStrings("fuzz-smoke-abort", abort.exception.reason);

    var invalid_tag_builder = message.MessageBuilder.init(allocator);
    defer invalid_tag_builder.deinit();
    var root = try invalid_tag_builder.allocateStruct(1, 1);
    root.writeUnionDiscriminant(0, 0xffff);
    const invalid_tag = try invalid_tag_builder.toBytes();
    defer allocator.free(invalid_tag);
    try std.testing.expectError(error.InvalidMessageTag, protocol.DecodedMessage.init(allocator, invalid_tag));

    return frame;
}

fn exerciseQuicLengthFramer(allocator: std.mem.Allocator, frame: []const u8) !void {
    var bytes = std.ArrayList(u8).empty;
    defer bytes.deinit(allocator);

    try bytes.resize(allocator, 4);
    std.mem.writeInt(u32, bytes.items[0..4], @intCast(frame.len), .little);
    try bytes.appendSlice(allocator, frame);
    const second_prefix = bytes.items.len;
    try bytes.resize(allocator, second_prefix + 4);
    std.mem.writeInt(u32, bytes.items[second_prefix..][0..4], 3, .little);
    try bytes.appendSlice(allocator, "xyz");

    var framer = rpc.transport.quic.LengthDelimitedFramer.initWithOptions(allocator, .{
        .max_message_bytes = 1024,
        .max_buffered_bytes = 2048,
    });
    defer framer.deinit();

    try framer.push(bytes.items[0..2]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes.items[2 .. frame.len + 6]);

    const first = (try framer.popFrame()).?;
    defer allocator.free(first);
    try std.testing.expectEqualSlices(u8, frame, first);

    try framer.push(bytes.items[frame.len + 6 ..]);
    const second = (try framer.popFrame()).?;
    defer allocator.free(second);
    try std.testing.expectEqualStrings("xyz", second);

    var hostile = rpc.transport.quic.LengthDelimitedFramer.initWithOptions(allocator, .{
        .max_message_bytes = 8,
        .max_buffered_bytes = 12,
    });
    defer hostile.deinit();

    var oversized: [4]u8 = undefined;
    std.mem.writeInt(u32, &oversized, 9, .little);
    try hostile.push(&oversized);
    try std.testing.expectError(error.FrameTooLarge, hostile.popFrame());
    hostile.reset();

    var empty: [4]u8 = @splat(0);
    try hostile.push(&empty);
    try std.testing.expectError(error.InvalidFrame, hostile.popFrame());
}

fn exercisePeerState(allocator: std.mem.Allocator, abort_frame: []const u8) !void {
    var peer = Peer.initDetached(allocator);
    defer peer.deinit();

    const DummyTransport = struct {
        fn send(_: *anyopaque, _: []const u8) anyerror!void {}
        fn start(_: *anyopaque, _: *Peer) void {}
        fn close(_: *anyopaque) void {}
        fn isClosing(_: *anyopaque) bool {
            return false;
        }
    };

    var ctx: u8 = 0;
    peer.attachTransport(&ctx, DummyTransport.start, DummyTransport.send, DummyTransport.close, DummyTransport.isClosing);
    try std.testing.expect(peer.hasAttachedTransport());
    peer.detachTransport();
    try std.testing.expect(!peer.hasAttachedTransport());

    try std.testing.expectError(error.RemoteAbort, peer.handleFrame(abort_frame));
    try std.testing.expectEqual(protocol.MessageTag.abort, peer.getLastInboundTag().?);
    try std.testing.expectEqualStrings("fuzz-smoke-abort", peer.getLastRemoteAbortReason().?);

    peer.handleFrame(&.{ 0, 0, 0, 0 }) catch {};
}

test "hardening fuzz smoke covers decode framer protocol quic and peer paths" {
    const allocator = std.testing.allocator;

    const compound = try makeCompoundMessage(allocator);
    defer allocator.free(compound.bytes);
    defer allocator.free(compound.packed_bytes);

    try exerciseMessageDecode(allocator, compound.bytes);
    try exercisePackedDecode(allocator, compound.packed_bytes);
    try exerciseHostileMessageDecode(allocator);
    try exerciseRpcFramer(allocator, compound.bytes);

    const abort_frame = try exerciseRpcProtocol(allocator);
    defer allocator.free(abort_frame);

    try exerciseQuicLengthFramer(allocator, abort_frame);
    try exercisePeerState(allocator, abort_frame);
}
