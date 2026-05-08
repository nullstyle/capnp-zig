const std = @import("std");

pub const length_prefix_bytes: usize = 4;

/// A u32 little-endian length-delimited message framer used inside the QUIC
/// baseline stream. The payload itself is still one complete Cap'n Proto RPC
/// message encoded by the existing peer/protocol layer.
pub const LengthDelimitedFramer = struct {
    pub const Options = struct {
        max_message_bytes: usize,
        max_buffered_bytes: ?usize = null,
    };

    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    expected_len: ?usize = null,
    max_message_bytes: usize,
    max_buffered_bytes: usize,

    pub fn init(allocator: std.mem.Allocator, max_message_bytes: usize) LengthDelimitedFramer {
        return initWithOptions(allocator, .{ .max_message_bytes = max_message_bytes });
    }

    pub fn initWithOptions(allocator: std.mem.Allocator, options: Options) LengthDelimitedFramer {
        return .{
            .allocator = allocator,
            .buffer = .empty,
            .max_message_bytes = options.max_message_bytes,
            .max_buffered_bytes = options.max_buffered_bytes orelse length_prefix_bytes + options.max_message_bytes,
        };
    }

    pub fn deinit(self: *LengthDelimitedFramer) void {
        self.buffer.deinit(self.allocator);
        self.expected_len = null;
    }

    pub fn push(self: *LengthDelimitedFramer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.ensureAppendBudget(data.len);
        try self.buffer.appendSlice(self.allocator, data);
    }

    pub fn reset(self: *LengthDelimitedFramer) void {
        self.buffer.items.len = 0;
        self.expected_len = null;
    }

    pub fn popFrame(self: *LengthDelimitedFramer) !?[]u8 {
        try self.updateExpected();
        const len = self.expected_len orelse return null;
        const total = length_prefix_bytes + len;
        if (self.buffer.items.len < total) return null;

        const frame = try self.allocator.alloc(u8, len);
        std.mem.copyForwards(u8, frame, self.buffer.items[length_prefix_bytes..total]);

        const remaining = self.buffer.items.len - total;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..remaining], self.buffer.items[total..]);
        }
        self.buffer.items.len = remaining;
        self.expected_len = null;
        return frame;
    }

    fn updateExpected(self: *LengthDelimitedFramer) !void {
        if (self.expected_len != null) return;
        if (self.buffer.items.len < length_prefix_bytes) return;

        const raw_len = std.mem.readInt(u32, self.buffer.items[0..length_prefix_bytes], .little);
        if (raw_len == 0) return error.InvalidFrame;
        const len: usize = @intCast(raw_len);
        if (len > self.max_message_bytes) return error.FrameTooLarge;
        self.expected_len = len;
    }

    fn ensureAppendBudget(self: *const LengthDelimitedFramer, data_len: usize) !void {
        const next = std.math.add(usize, self.buffer.items.len, data_len) catch return error.FrameTooLarge;
        if (next > self.max_buffered_bytes) return error.FrameTooLarge;
    }
};

test "LengthDelimitedFramer assembles fragmented and coalesced frames" {
    var framer = LengthDelimitedFramer.init(std.testing.allocator, 1024);
    defer framer.deinit();

    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");

    try framer.push(bytes[0..2]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes[2..9]);

    const first = (try framer.popFrame()).?;
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("abc", first);
    try std.testing.expect(try framer.popFrame() == null);

    try framer.push(bytes[9..]);
    const second = (try framer.popFrame()).?;
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings("hello", second);
    try std.testing.expect(try framer.popFrame() == null);
}

test "LengthDelimitedFramer rejects oversized frames" {
    var framer = LengthDelimitedFramer.init(std.testing.allocator, 2);
    defer framer.deinit();

    var bytes: [4]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    try framer.push(&bytes);
    try std.testing.expectError(error.FrameTooLarge, framer.popFrame());
}

test "LengthDelimitedFramer rejects buffered byte budget before append" {
    var framer = LengthDelimitedFramer.initWithOptions(std.testing.allocator, .{
        .max_message_bytes = 1024,
        .max_buffered_bytes = 4,
    });
    defer framer.deinit();

    try framer.push(&[_]u8{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 4), framer.buffer.items.len);

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{5}));
    try std.testing.expectEqual(@as(usize, 4), framer.buffer.items.len);
}

test "LengthDelimitedFramer budget rejection does not allocate" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var framer = LengthDelimitedFramer.initWithOptions(failing.allocator(), .{
        .max_message_bytes = 1024,
        .max_buffered_bytes = 0,
    });
    defer framer.deinit();

    try std.testing.expectError(error.FrameTooLarge, framer.push(&[_]u8{1}));
    try std.testing.expectEqual(@as(usize, 0), framer.buffer.items.len);
}
