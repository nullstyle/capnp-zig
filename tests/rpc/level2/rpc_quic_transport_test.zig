const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.quic;

test "quic transport exposes native Cap'n Proto RPC ALPN" {
    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);
    try std.testing.expectEqual(@as(u64, 0), quic.baseline_stream_id);
}

test "quic length-delimited framer handles fragmented and coalesced payloads" {
    var framer = quic.LengthDelimitedFramer.init(std.testing.allocator, 1024);
    defer framer.deinit();

    var bytes: [16]u8 = @splat(0);
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");

    try framer.push(bytes[0..5]);
    try std.testing.expect(try framer.popFrame() == null);
    try framer.push(bytes[5..]);

    const first = (try framer.popFrame()).?;
    defer std.testing.allocator.free(first);
    try std.testing.expectEqualStrings("abc", first);

    const second = (try framer.popFrame()).?;
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualStrings("hello", second);
    try std.testing.expect(try framer.popFrame() == null);
}

test "quic path address conversion round-trips IPv4" {
    const addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 10, 20, 30, 40 },
        .port = 4321,
    } };

    const path_addr = quic.ipAddressToPathAddress(addr);
    const round_trip = quic.pathAddressToIpAddress(path_addr).?;

    try std.testing.expect(round_trip == .ip4);
    try std.testing.expectEqual(addr.ip4.port, round_trip.ip4.port);
    try std.testing.expectEqualSlices(u8, &addr.ip4.bytes, &round_trip.ip4.bytes);
}
