const std = @import("std");

pub const preface = "capnp-zig-quic-native/1\n";
pub const version: u16 = 1;
pub const length_prefix_bytes: usize = 4;
pub const common_header_bytes: usize = 4;
pub const rpc_header_bytes: usize = common_header_bytes + 8;
pub const data_rpc_payload_bytes: usize = common_header_bytes + 8 + 8 + 8;

pub const ControlFrameTag = enum(u8) {
    hello = 1,
    inline_rpc = 2,
    data_rpc = 3,
};

pub const Hello = struct {
    version: u16,
};

pub const InlineRpc = struct {
    sequence: u64,
    frame: []u8,
};

pub const DataRpc = struct {
    sequence: u64,
    stream_id: u64,
    length: usize,
};

pub const ControlFrame = union(ControlFrameTag) {
    hello: Hello,
    inline_rpc: InlineRpc,
    data_rpc: DataRpc,

    pub fn deinit(self: ControlFrame, allocator: std.mem.Allocator) void {
        switch (self) {
            .inline_rpc => |inline_frame| allocator.free(inline_frame.frame),
            else => {},
        }
    }
};

pub const ControlFramer = struct {
    pub const Options = struct {
        max_control_frame_bytes: usize,
        max_rpc_frame_bytes: usize,
        max_buffered_bytes: ?usize = null,
    };

    allocator: std.mem.Allocator,
    buffer: std.ArrayList(u8),
    expected_len: ?usize = null,
    max_control_frame_bytes: usize,
    max_rpc_frame_bytes: usize,
    max_buffered_bytes: usize,

    pub fn init(allocator: std.mem.Allocator, options: Options) ControlFramer {
        return .{
            .allocator = allocator,
            .buffer = .empty,
            .max_control_frame_bytes = options.max_control_frame_bytes,
            .max_rpc_frame_bytes = options.max_rpc_frame_bytes,
            .max_buffered_bytes = options.max_buffered_bytes orelse length_prefix_bytes + options.max_control_frame_bytes,
        };
    }

    pub fn deinit(self: *ControlFramer) void {
        self.buffer.deinit(self.allocator);
        self.expected_len = null;
    }

    pub fn push(self: *ControlFramer, data: []const u8) !void {
        if (data.len == 0) return;
        try self.ensureAppendBudget(data.len);
        try self.buffer.appendSlice(self.allocator, data);
    }

    pub fn reset(self: *ControlFramer) void {
        self.buffer.items.len = 0;
        self.expected_len = null;
    }

    pub fn popFrame(self: *ControlFramer) !?ControlFrame {
        try self.updateExpected();
        const len = self.expected_len orelse return null;
        const total = length_prefix_bytes + len;
        if (self.buffer.items.len < total) return null;

        const payload = self.buffer.items[length_prefix_bytes..total];
        const frame = try decodePayload(self.allocator, payload, self.max_rpc_frame_bytes);

        const remaining = self.buffer.items.len - total;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.buffer.items[0..remaining], self.buffer.items[total..]);
        }
        self.buffer.items.len = remaining;
        self.expected_len = null;
        return frame;
    }

    fn updateExpected(self: *ControlFramer) !void {
        if (self.expected_len != null) return;
        if (self.buffer.items.len < length_prefix_bytes) return;

        const raw_len = std.mem.readInt(u32, self.buffer.items[0..length_prefix_bytes], .little);
        if (raw_len == 0) return error.InvalidFrame;
        const len: usize = @intCast(raw_len);
        if (len > self.max_control_frame_bytes) return error.FrameTooLarge;
        self.expected_len = len;
    }

    fn ensureAppendBudget(self: *const ControlFramer, data_len: usize) !void {
        const next = std.math.add(usize, self.buffer.items.len, data_len) catch return error.FrameTooLarge;
        if (next > self.max_buffered_bytes) return error.FrameTooLarge;
    }
};

pub fn encodedHelloLen() usize {
    return length_prefix_bytes + common_header_bytes;
}

pub fn encodeHello(dest: []u8) !usize {
    if (dest.len < encodedHelloLen()) return error.NoSpaceLeft;
    std.mem.writeInt(u32, dest[0..length_prefix_bytes], common_header_bytes, .little);
    dest[4] = @intFromEnum(ControlFrameTag.hello);
    std.mem.writeInt(u16, dest[5..7], version, .little);
    dest[7] = 0;
    return encodedHelloLen();
}

pub fn encodeInlineRpc(
    allocator: std.mem.Allocator,
    sequence: u64,
    frame: []const u8,
    max_control_frame_bytes: usize,
) ![]u8 {
    if (frame.len == 0) return error.InvalidFrame;
    const payload_len = std.math.add(usize, rpc_header_bytes, frame.len) catch return error.FrameTooLarge;
    if (payload_len > max_control_frame_bytes or payload_len > std.math.maxInt(u32)) return error.FrameTooLarge;

    const out = try allocator.alloc(u8, length_prefix_bytes + payload_len);
    errdefer allocator.free(out);
    std.mem.writeInt(u32, out[0..length_prefix_bytes], @intCast(payload_len), .little);
    out[4] = @intFromEnum(ControlFrameTag.inline_rpc);
    @memset(out[5..8], 0);
    std.mem.writeInt(u64, out[8..16], sequence, .little);
    std.mem.copyForwards(u8, out[16..], frame);
    return out;
}

pub fn encodeDataRpc(
    allocator: std.mem.Allocator,
    sequence: u64,
    stream_id: u64,
    length: usize,
    max_control_frame_bytes: usize,
) ![]u8 {
    if (length == 0) return error.InvalidFrame;
    if (data_rpc_payload_bytes > max_control_frame_bytes) return error.FrameTooLarge;

    const out = try allocator.alloc(u8, length_prefix_bytes + data_rpc_payload_bytes);
    errdefer allocator.free(out);
    std.mem.writeInt(u32, out[0..length_prefix_bytes], data_rpc_payload_bytes, .little);
    out[4] = @intFromEnum(ControlFrameTag.data_rpc);
    @memset(out[5..8], 0);
    std.mem.writeInt(u64, out[8..16], sequence, .little);
    std.mem.writeInt(u64, out[16..24], stream_id, .little);
    std.mem.writeInt(u64, out[24..32], @intCast(length), .little);
    return out;
}

fn decodePayload(
    allocator: std.mem.Allocator,
    payload: []const u8,
    max_rpc_frame_bytes: usize,
) !ControlFrame {
    if (payload.len < common_header_bytes) return error.InvalidFrame;
    const tag = std.enums.fromInt(ControlFrameTag, payload[0]) orelse return error.InvalidFrame;
    return switch (tag) {
        .hello => decodeHello(payload),
        .inline_rpc => try decodeInlineRpc(allocator, payload, max_rpc_frame_bytes),
        .data_rpc => decodeDataRpc(payload, max_rpc_frame_bytes),
    };
}

fn decodeHello(payload: []const u8) !ControlFrame {
    if (payload.len != common_header_bytes) return error.InvalidFrame;
    const hello_version = std.mem.readInt(u16, payload[1..3], .little);
    if (payload[3] != 0) return error.InvalidFrame;
    if (hello_version != version) return error.InvalidFrame;
    return .{ .hello = .{ .version = hello_version } };
}

fn decodeInlineRpc(
    allocator: std.mem.Allocator,
    payload: []const u8,
    max_rpc_frame_bytes: usize,
) !ControlFrame {
    if (payload.len < rpc_header_bytes) return error.InvalidFrame;
    if (!reservedIsZero(payload[1..4])) return error.InvalidFrame;
    const frame_len = payload.len - rpc_header_bytes;
    if (frame_len == 0) return error.InvalidFrame;
    if (frame_len > max_rpc_frame_bytes) return error.FrameTooLarge;

    const frame = try allocator.alloc(u8, frame_len);
    errdefer allocator.free(frame);
    std.mem.copyForwards(u8, frame, payload[rpc_header_bytes..]);
    return .{
        .inline_rpc = .{
            .sequence = std.mem.readInt(u64, payload[4..12], .little),
            .frame = frame,
        },
    };
}

fn decodeDataRpc(payload: []const u8, max_rpc_frame_bytes: usize) !ControlFrame {
    if (payload.len != data_rpc_payload_bytes) return error.InvalidFrame;
    if (!reservedIsZero(payload[1..4])) return error.InvalidFrame;
    const raw_len = std.mem.readInt(u64, payload[20..28], .little);
    if (raw_len == 0) return error.InvalidFrame;
    if (raw_len > max_rpc_frame_bytes or raw_len > std.math.maxInt(usize)) return error.FrameTooLarge;
    return .{
        .data_rpc = .{
            .sequence = std.mem.readInt(u64, payload[4..12], .little),
            .stream_id = std.mem.readInt(u64, payload[12..20], .little),
            .length = @intCast(raw_len),
        },
    };
}

fn reservedIsZero(bytes: []const u8) bool {
    for (bytes) |byte| {
        if (byte != 0) return false;
    }
    return true;
}

test "native QUIC control framer decodes hello inline and data frames" {
    var framer = ControlFramer.init(std.testing.allocator, .{
        .max_control_frame_bytes = 1024,
        .max_rpc_frame_bytes = 1024,
    });
    defer framer.deinit();

    var hello_buf: [encodedHelloLen()]u8 = undefined;
    const hello_len = try encodeHello(&hello_buf);
    const inline_control = try encodeInlineRpc(std.testing.allocator, 7, "rpc", 1024);
    defer std.testing.allocator.free(inline_control);
    const data = try encodeDataRpc(std.testing.allocator, 8, 2, 4096, 1024);
    defer std.testing.allocator.free(data);

    try framer.push(hello_buf[0..hello_len]);
    try framer.push(inline_control[0..5]);
    var frame = (try framer.popFrame()).?;
    try std.testing.expect(frame == .hello);
    try std.testing.expectEqual(version, frame.hello.version);

    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(inline_control[5..]);
    frame = (try framer.popFrame()).?;
    defer frame.deinit(std.testing.allocator);
    try std.testing.expect(frame == .inline_rpc);
    try std.testing.expectEqual(@as(u64, 7), frame.inline_rpc.sequence);
    try std.testing.expectEqualStrings("rpc", frame.inline_rpc.frame);

    try framer.push(data);
    frame = (try framer.popFrame()).?;
    try std.testing.expect(frame == .data_rpc);
    try std.testing.expectEqual(@as(u64, 8), frame.data_rpc.sequence);
    try std.testing.expectEqual(@as(u64, 2), frame.data_rpc.stream_id);
    try std.testing.expectEqual(@as(usize, 4096), frame.data_rpc.length);
}

test "native QUIC control framer rejects malformed frames" {
    var framer = ControlFramer.init(std.testing.allocator, .{
        .max_control_frame_bytes = 8,
        .max_rpc_frame_bytes = 8,
    });
    defer framer.deinit();

    var oversized: [4]u8 = undefined;
    std.mem.writeInt(u32, oversized[0..4], 9, .little);
    try framer.push(&oversized);
    try std.testing.expectError(error.FrameTooLarge, framer.popFrame());

    framer.reset();
    var bad_reserved = try encodeInlineRpc(std.testing.allocator, 0, "a", 32);
    defer std.testing.allocator.free(bad_reserved);
    bad_reserved[5] = 1;
    try framer.push(bad_reserved);
    try std.testing.expectError(error.InvalidFrame, framer.popFrame());
}

test "native QUIC control framer rejects malformed envelope headers" {
    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 8,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var zero_len: [length_prefix_bytes]u8 = @splat(0);
        try framer.push(&zero_len);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 8,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var unknown_tag: [length_prefix_bytes + common_header_bytes]u8 = @splat(0);
        std.mem.writeInt(u32, unknown_tag[0..length_prefix_bytes], common_header_bytes, .little);
        unknown_tag[length_prefix_bytes] = 0xff;
        try framer.push(&unknown_tag);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 8,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var bad_hello: [encodedHelloLen()]u8 = undefined;
        _ = try encodeHello(&bad_hello);
        bad_hello[7] = 1;
        try framer.push(&bad_hello);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 8,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var bad_version: [encodedHelloLen()]u8 = undefined;
        _ = try encodeHello(&bad_version);
        std.mem.writeInt(u16, bad_version[5..7], version + 1, .little);
        try framer.push(&bad_version);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }
}

test "native QUIC control framer enforces rpc payload limits" {
    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 8,
            .max_buffered_bytes = 8,
        });
        defer framer.deinit();

        const too_much_buffer: [9]u8 = @splat(0);
        try std.testing.expectError(error.FrameTooLarge, framer.push(&too_much_buffer));
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var empty_inline: [length_prefix_bytes + rpc_header_bytes]u8 = @splat(0);
        std.mem.writeInt(u32, empty_inline[0..length_prefix_bytes], rpc_header_bytes, .little);
        empty_inline[length_prefix_bytes] = @intFromEnum(ControlFrameTag.inline_rpc);
        try framer.push(&empty_inline);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 4,
        });
        defer framer.deinit();

        const too_large_inline = try encodeInlineRpc(std.testing.allocator, 0, "abcde", 32);
        defer std.testing.allocator.free(too_large_inline);
        try framer.push(too_large_inline);
        try std.testing.expectError(error.FrameTooLarge, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var bad_reserved = try encodeDataRpc(std.testing.allocator, 0, 2, 8, 32);
        defer std.testing.allocator.free(bad_reserved);
        bad_reserved[5] = 1;
        try framer.push(bad_reserved);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        var zero_len_data: [length_prefix_bytes + data_rpc_payload_bytes]u8 = @splat(0);
        std.mem.writeInt(u32, zero_len_data[0..length_prefix_bytes], data_rpc_payload_bytes, .little);
        zero_len_data[length_prefix_bytes] = @intFromEnum(ControlFrameTag.data_rpc);
        try framer.push(&zero_len_data);
        try std.testing.expectError(error.InvalidFrame, framer.popFrame());
    }

    {
        var framer = ControlFramer.init(std.testing.allocator, .{
            .max_control_frame_bytes = 32,
            .max_rpc_frame_bytes = 8,
        });
        defer framer.deinit();

        const too_large_data = try encodeDataRpc(std.testing.allocator, 0, 2, 9, 32);
        defer std.testing.allocator.free(too_large_data);
        try framer.push(too_large_data);
        try std.testing.expectError(error.FrameTooLarge, framer.popFrame());
    }
}
