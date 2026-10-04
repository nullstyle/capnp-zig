const std = @import("std");
const capnpc = @import("capnpc-zig");

const message = capnpc.message;
const Framer = capnpc.rpc.wire.framing.Framer;

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

test "Framer rejects buffered byte budget before append" {
    var framer = Framer.initWithOptions(std.testing.allocator, .{
        .max_buffered_bytes = 4,
    });
    defer framer.deinit();

    try framer.push(&[_]u8{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 4), framer.bufferedBytes());

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{5}));
    try std.testing.expectEqual(@as(usize, 4), framer.bufferedBytes());
}

test "Framer budget rejection does not allocate" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var framer = Framer.initWithOptions(failing.allocator(), .{
        .max_buffered_bytes = 0,
    });
    defer framer.deinit();

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{1}));
    try std.testing.expectEqual(@as(usize, 0), framer.bufferedBytes());
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
                var msg = message.Message.init(allocator, frame, .{}) catch continue;
                msg.deinit();
            } else {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Read-cursor coverage. The framers hand frames out from a consumed prefix
// that is reclaimed lazily (src/rpc/wire/read_cursor.zig) instead of shifting
// the unread tail down after every frame. These tests pin what must not
// change: every frame decodes intact whatever the read boundaries, the
// buffered-byte budget counts unread bytes only, `reset` discards the prefix
// too, and the consumed prefix is reclaimed rather than accumulated.
// ---------------------------------------------------------------------------

const quic = capnpc.rpc.transport.quic;

/// Concatenated frames, where each one ends, and the payload `popFrame`
/// should return for each.
const FrameStream = struct {
    bytes: []u8,
    ends: []usize,
    expected: [][]u8,

    fn deinit(self: FrameStream, allocator: std.mem.Allocator) void {
        for (self.expected) |payload| allocator.free(payload);
        allocator.free(self.expected);
        allocator.free(self.ends);
        allocator.free(self.bytes);
    }

    fn maxFrameLen(self: FrameStream) usize {
        var max: usize = 0;
        var start: usize = 0;
        for (self.ends) |end| {
            max = @max(max, end - start);
            start = end;
        }
        return max;
    }
};

const StreamKind = enum { capnp, length_delimited, native_control };

/// `count` frames with payloads of varied sizes (1..300 bytes), so frame
/// boundaries land at every alignment relative to the read chunks.
fn buildFrameStream(allocator: std.mem.Allocator, kind: StreamKind, count: usize) !FrameStream {
    var bytes: std.ArrayList(u8) = .empty;
    errdefer bytes.deinit(allocator);
    const ends = try allocator.alloc(usize, count);
    errdefer allocator.free(ends);
    const expected = try allocator.alloc([]u8, count);
    var built: usize = 0;
    errdefer {
        for (expected[0..built]) |payload| allocator.free(payload);
        allocator.free(expected);
    }

    var payload_buf: [512]u8 = undefined;
    for (0..count) |i| {
        // Never empty: the QUIC framers reject zero-length frames.
        const payload = payload_buf[0 .. 1 + (i * 37) % 300];
        for (payload, 0..) |*byte, j| byte.* = @truncate(i *% 31 +% j);

        switch (kind) {
            .capnp => {
                var builder = message.MessageBuilder.init(allocator);
                defer builder.deinit();
                var root = try builder.allocateStruct(1, 1);
                root.writeU32(0, @intCast(i));
                try root.writeData(0, payload);
                const frame = try builder.toBytes();
                defer allocator.free(frame);
                try bytes.appendSlice(allocator, frame);
                // The capnp framer returns the whole frame, header included.
                expected[i] = try allocator.dupe(u8, frame);
            },
            .length_delimited => {
                var prefix: [4]u8 = undefined;
                std.mem.writeInt(u32, &prefix, @intCast(payload.len), .little);
                try bytes.appendSlice(allocator, &prefix);
                try bytes.appendSlice(allocator, payload);
                expected[i] = try allocator.dupe(u8, payload);
            },
            .native_control => {
                const frame = try quic.native.encodeInlineRpc(allocator, i, payload, 4096);
                defer allocator.free(frame);
                try bytes.appendSlice(allocator, frame);
                expected[i] = try allocator.dupe(u8, payload);
            },
        }
        built += 1;
        ends[i] = bytes.items.len;
    }

    return .{ .bytes = try bytes.toOwnedSlice(allocator), .ends = ends, .expected = expected };
}

fn FramerFor(comptime kind: StreamKind) type {
    return switch (kind) {
        .capnp => Framer,
        .length_delimited => quic.LengthDelimitedFramer,
        .native_control => quic.NativeControlFramer,
    };
}

fn initFramer(comptime kind: StreamKind, allocator: std.mem.Allocator, max_buffered_bytes: ?usize) FramerFor(kind) {
    return switch (kind) {
        .capnp => if (max_buffered_bytes) |max|
            Framer.initWithOptions(allocator, .{ .max_buffered_bytes = max })
        else
            Framer.init(allocator),
        .length_delimited => quic.LengthDelimitedFramer.initWithOptions(allocator, .{
            .max_message_bytes = 4096,
            .max_buffered_bytes = max_buffered_bytes,
        }),
        .native_control => quic.NativeControlFramer.init(allocator, .{
            .max_control_frame_bytes = 4096,
            .max_rpc_frame_bytes = 4096,
            .max_buffered_bytes = max_buffered_bytes,
        }),
    };
}

/// Pop one frame and return its payload; the caller frees it.
fn popPayload(comptime kind: StreamKind, framer: *FramerFor(kind), allocator: std.mem.Allocator) !?[]u8 {
    switch (kind) {
        .capnp, .length_delimited => return try framer.popFrame(),
        .native_control => {
            const frame = (try framer.popFrame()) orelse return null;
            switch (frame) {
                .inline_rpc => |inline_rpc| return inline_rpc.frame,
                else => {
                    frame.deinit(allocator);
                    return error.UnexpectedControlFrame;
                },
            }
        },
    }
}

/// Unread bytes, through each framer's own accessor where it has one.
fn unreadBytes(comptime kind: StreamKind, framer: *const FramerFor(kind)) usize {
    return switch (kind) {
        .capnp => framer.bufferedBytes(),
        // `freeBytes` is the budget minus the unread bytes.
        .native_control => framer.max_buffered_bytes - framer.freeBytes(),
        .length_delimited => framer.buffer.items.len - framer.consumed,
    };
}

/// Push `stream` in chunks of `chunk_len` (PRNG-chosen sizes when null),
/// draining every complete frame after each push.
fn expectStreamDecodes(
    comptime kind: StreamKind,
    stream: FrameStream,
    chunk_len: ?usize,
    seed: u64,
) !void {
    const allocator = std.testing.allocator;
    var framer = initFramer(kind, allocator, null);
    defer framer.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var pushed: usize = 0;
    var popped_bytes: usize = 0;
    var next: usize = 0;
    var peak_capacity: usize = 0;
    var max_chunk: usize = 0;
    while (pushed < stream.bytes.len) {
        const want = chunk_len orelse 1 + random.uintLessThan(usize, 700);
        const n = @min(want, stream.bytes.len - pushed);
        max_chunk = @max(max_chunk, n);
        try framer.push(stream.bytes[pushed..][0..n]);
        pushed += n;
        peak_capacity = @max(peak_capacity, framer.buffer.capacity);

        while (try popPayload(kind, &framer, allocator)) |payload| {
            defer allocator.free(payload);
            try std.testing.expect(next < stream.expected.len);
            try std.testing.expectEqualSlices(u8, stream.expected[next], payload);
            const start = if (next == 0) 0 else stream.ends[next - 1];
            popped_bytes += stream.ends[next] - start;
            next += 1;
        }
        try std.testing.expectEqual(pushed - popped_bytes, unreadBytes(kind, &framer));
    }
    try std.testing.expectEqual(stream.expected.len, next);
    try std.testing.expectEqual(@as(usize, 0), unreadBytes(kind, &framer));

    // The consumed prefix is reclaimed, never accumulated: the allocation
    // stays within a small multiple of (one frame + one read), not the
    // stream. Without reclamation, reads that rarely end on a frame boundary
    // grow the buffer toward the whole stream.
    const bound = 4 * (stream.maxFrameLen() + max_chunk);
    try std.testing.expect(stream.bytes.len > bound);
    try std.testing.expect(peak_capacity <= bound);
}

fn expectSplitReadsDecode(comptime kind: StreamKind) !void {
    const allocator = std.testing.allocator;
    const stream = try buildFrameStream(allocator, kind, 400);
    defer stream.deinit(allocator);

    // Byte at a time, odd sizes that straddle every header, sizes larger
    // than most frames, and random sizes.
    for ([_]usize{ 1, 3, 7, 64, 333, 1000 }) |chunk| {
        try expectStreamDecodes(kind, stream, chunk, 0);
    }
    for ([_]u64{ 1, 2, 3 }) |seed| {
        try expectStreamDecodes(kind, stream, null, seed);
    }
}

test "Framer decodes frames split across many reads" {
    try expectSplitReadsDecode(.capnp);
}

test "LengthDelimitedFramer decodes frames split across many reads" {
    try expectSplitReadsDecode(.length_delimited);
}

test "NativeControlFramer decodes frames split across many reads" {
    try expectSplitReadsDecode(.native_control);
}

/// The budget bounds UNREAD bytes. With a short frame consumed ahead of a
/// longer unread one, the consumed prefix is still in the buffer (the cursor
/// is not yet past half) and must not count against the budget.
fn expectBudgetCountsUnreadOnly(comptime kind: StreamKind) !void {
    const allocator = std.testing.allocator;
    const stream = try buildFrameStream(allocator, kind, 12);
    defer stream.deinit(allocator);
    // Frame 0 has the shortest payload (1 byte); frame 9 is longer.
    const short = stream.bytes[0..stream.ends[0]];
    const long = stream.bytes[stream.ends[8]..stream.ends[9]];
    try std.testing.expect(short.len < long.len);

    var framer = initFramer(kind, allocator, short.len + long.len);
    defer framer.deinit();

    try framer.push(short);
    try framer.push(long);
    const first = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    allocator.free(first);
    try std.testing.expectEqual(long.len, unreadBytes(kind, &framer));

    // Exactly fills the budget again: allowed.
    try framer.push(short);
    try std.testing.expectEqual(long.len + short.len, unreadBytes(kind, &framer));
    // One byte more is not, and leaves the framer untouched.
    try std.testing.expectError(error.FrameTooLarge, framer.push(short[0..1]));
    try std.testing.expectEqual(long.len + short.len, unreadBytes(kind, &framer));

    const second = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    defer allocator.free(second);
    try std.testing.expectEqualSlices(u8, stream.expected[9], second);
    const third = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    defer allocator.free(third);
    try std.testing.expectEqualSlices(u8, stream.expected[0], third);
}

test "Framer budget counts unread bytes, not the consumed prefix" {
    try expectBudgetCountsUnreadOnly(.capnp);
    try expectBudgetCountsUnreadOnly(.length_delimited);
    try expectBudgetCountsUnreadOnly(.native_control);
}

/// `reset` after a partial drain discards the consumed prefix as well: the
/// next frame decodes from the start of what is pushed next.
fn expectResetDiscardsConsumedPrefix(comptime kind: StreamKind) !void {
    const allocator = std.testing.allocator;
    const stream = try buildFrameStream(allocator, kind, 12);
    defer stream.deinit(allocator);

    var framer = initFramer(kind, allocator, null);
    defer framer.deinit();

    // Frames 0..2 plus half of frame 3, then pop two of them.
    try framer.push(stream.bytes[0 .. stream.ends[2] + (stream.ends[3] - stream.ends[2]) / 2]);
    for (0..2) |_| {
        const payload = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
        allocator.free(payload);
    }
    framer.reset();
    try std.testing.expectEqual(@as(usize, 0), unreadBytes(kind, &framer));

    try framer.push(stream.bytes[stream.ends[5]..stream.ends[6]]);
    const payload = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    defer allocator.free(payload);
    try std.testing.expectEqualSlices(u8, stream.expected[6], payload);
    try std.testing.expectEqual(@as(usize, 0), unreadBytes(kind, &framer));
}

/// An append that only fits by reusing the consumed prefix must reuse it
/// rather than grow the allocation: the framer's memory stays bounded by its
/// unread bytes plus one read, as it was before the cursor existed.
fn expectPrefixReusedBeforeGrowth(comptime kind: StreamKind) !void {
    const allocator = std.testing.allocator;
    const stream = try buildFrameStream(allocator, kind, 40);
    defer stream.deinit(allocator);
    const short = stream.bytes[0..stream.ends[0]];
    const long = stream.bytes[stream.ends[0]..stream.ends[1]];
    try std.testing.expect(short.len < long.len);

    var framer = initFramer(kind, allocator, null);
    defer framer.deinit();

    try framer.push(stream.bytes[0..stream.ends[1]]);
    const capacity = framer.buffer.capacity;
    // Consume the short frame; the cursor is not past half, so the prefix
    // stays in place.
    const first = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    allocator.free(first);

    // Exactly fills the allocation once the prefix is reclaimed, and
    // overflows it if it is not.
    const fill = capacity - long.len;
    try std.testing.expect(stream.bytes.len - stream.ends[1] >= fill);
    try framer.push(stream.bytes[stream.ends[1]..][0..fill]);
    try std.testing.expectEqual(capacity, framer.buffer.capacity);

    // And the stream still decodes from where it left off.
    var next: usize = 1;
    while (try popPayload(kind, &framer, allocator)) |payload| {
        defer allocator.free(payload);
        try std.testing.expectEqualSlices(u8, stream.expected[next], payload);
        next += 1;
    }
    try std.testing.expect(next > 1);
}

test "Framer reuses the consumed prefix before growing" {
    try expectPrefixReusedBeforeGrowth(.capnp);
    try expectPrefixReusedBeforeGrowth(.length_delimited);
    try expectPrefixReusedBeforeGrowth(.native_control);
}

test "Framer reset discards the consumed prefix" {
    try expectResetDiscardsConsumedPrefix(.capnp);
    try expectResetDiscardsConsumedPrefix(.length_delimited);
    try expectResetDiscardsConsumedPrefix(.native_control);
}

/// The documented field contract (see `Framer.buffer`): while a consumed
/// prefix is still in the buffer, `buffer.items[consumed..]` is exactly the
/// unread bytes, which is what `buffer.items` alone held before the cursor.
/// Consumers that read leftover bytes from the field are told to use it.
fn expectUnreadBytesAreCursorTail(comptime kind: StreamKind) !void {
    const allocator = std.testing.allocator;
    const stream = try buildFrameStream(allocator, kind, 12);
    defer stream.deinit(allocator);

    var framer = initFramer(kind, allocator, null);
    defer framer.deinit();

    // Frames 0..2 plus half of frame 3, then pop frame 0. It is the
    // shortest, so the cursor is not past half and the prefix stays.
    const pushed = stream.ends[2] + (stream.ends[3] - stream.ends[2]) / 2;
    try framer.push(stream.bytes[0..pushed]);
    const first = (try popPayload(kind, &framer, allocator)) orelse return error.MissingFrame;
    allocator.free(first);

    try std.testing.expectEqual(stream.ends[0], framer.consumed);
    try std.testing.expectEqualSlices(u8, stream.bytes[stream.ends[0]..pushed], framer.buffer.items[framer.consumed..]);
    try std.testing.expectEqual(pushed - stream.ends[0], unreadBytes(kind, &framer));
}

test "Framer unread bytes are buffer.items[consumed..]" {
    try expectUnreadBytesAreCursorTail(.capnp);
    try expectUnreadBytesAreCursorTail(.length_delimited);
    try expectUnreadBytesAreCursorTail(.native_control);
}
