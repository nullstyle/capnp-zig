const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const Framer = capnpc.rpc.framing.Framer;

fn buildMessage(allocator: std.mem.Allocator, value: u32) ![]const u8 {
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    var root = try builder.allocateStruct(1, 0);
    root.writeU32(0, value);

    return try builder.toBytes();
}

test "Framer yields complete frames from partial input" {
    const allocator = std.testing.allocator;

    const bytes = try buildMessage(allocator, 1234);
    defer allocator.free(bytes);

    var framer = Framer.init(allocator);
    defer framer.deinit();

    try framer.push(bytes[0..5]);
    try std.testing.expectEqual(@as(?[]u8, null), try framer.popFrame());

    try framer.push(bytes[5..]);
    const frame = (try framer.popFrame()) orelse return error.MissingFrame;
    defer allocator.free(frame);

    try std.testing.expectEqualSlices(u8, bytes, frame);
}

test "Framer handles multiple frames in a buffer" {
    const allocator = std.testing.allocator;

    const first = try buildMessage(allocator, 1);
    const second = try buildMessage(allocator, 2);
    defer allocator.free(first);
    defer allocator.free(second);

    var framer = Framer.init(allocator);
    defer framer.deinit();

    var combined = try allocator.alloc(u8, first.len + second.len);
    defer allocator.free(combined);
    std.mem.copyForwards(u8, combined[0..first.len], first);
    std.mem.copyForwards(u8, combined[first.len..], second);

    try framer.push(combined);
    const frame1 = (try framer.popFrame()) orelse return error.MissingFrame;
    defer allocator.free(frame1);
    const frame2 = (try framer.popFrame()) orelse return error.MissingFrame;
    defer allocator.free(frame2);

    try std.testing.expectEqualSlices(u8, first, frame1);
    try std.testing.expectEqualSlices(u8, second, frame2);
    try std.testing.expectEqual(@as(?[]u8, null), try framer.popFrame());
}

test "Framer rejects malformed frame header overflow" {
    const allocator = std.testing.allocator;

    var framer = Framer.init(allocator);
    defer framer.deinit();

    // segment_count_minus_one = max u32 overflows on +1
    const bad_header = [_]u8{ 0xff, 0xff, 0xff, 0xff };
    try framer.push(&bad_header);
    try std.testing.expectError(error.InvalidFrame, framer.popFrame());
}

test "Framer rejects oversized frame claims" {
    const allocator = std.testing.allocator;

    var framer = Framer.init(allocator);
    defer framer.deinit();

    const oversized_words: u32 = @as(u32, @intCast(Framer.max_frame_words + 1));
    var header: [8]u8 = undefined;
    std.mem.writeInt(u32, header[0..4], 0, .little); // 1 segment
    std.mem.writeInt(u32, header[4..8], oversized_words, .little);

    try framer.push(&header);
    try std.testing.expectError(error.FrameTooLarge, framer.popFrame());
}

test "Framer fuzz malformed streams does not crash" {
    const allocator = std.testing.allocator;

    var framer = Framer.init(allocator);
    defer framer.deinit();

    var prng = std.Random.DefaultPrng.init(0x8A31_D4E2_551A_0F7B);
    const random = prng.random();

    var i: usize = 0;
    while (i < 512) : (i += 1) {
        const chunk_len = random.uintLessThan(usize, 80);
        const chunk = try allocator.alloc(u8, chunk_len);
        defer allocator.free(chunk);
        random.bytes(chunk);

        framer.push(chunk) catch |err| {
            try std.testing.expect(err == error.InvalidFrame);
            framer.deinit();
            framer = Framer.init(allocator);
            continue;
        };

        var drained: usize = 0;
        while (drained < 8) : (drained += 1) {
            const maybe_frame = framer.popFrame() catch |err| {
                try std.testing.expect(err == error.InvalidFrame);
                framer.deinit();
                framer = Framer.init(allocator);
                break;
            };

            if (maybe_frame) |frame| {
                defer allocator.free(frame);
                var msg = message.Message.init(allocator, frame) catch continue;
                msg.deinit();
            } else {
                break;
            }
        }
    }
}
