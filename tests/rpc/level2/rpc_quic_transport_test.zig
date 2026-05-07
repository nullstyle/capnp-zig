const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.quic;

test "quic transport exposes native Cap'n Proto RPC ALPN" {
    try std.testing.expectEqualStrings("capnp-rpc/1", quic.alpn);
    try std.testing.expectEqual(@as(u64, 0), quic.baseline_stream_id);
    try std.testing.expect(quic.default_max_outbound_queue_items > 0);
    try std.testing.expect(quic.default_max_outbound_queue_bytes > quic.default_max_message_bytes);
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

test "quic client outbound queue enforces item and byte bounds" {
    const remote_addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 4433,
    } };

    var item_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 1,
        .max_outbound_queue_bytes = 1024,
    });
    defer item_limited.deinit();

    try item_limited.sendFrame("abc");
    try std.testing.expectError(error.OutboundQueueFull, item_limited.sendFrame("def"));

    var byte_limited = try quic.Connection.initClient(std.testing.allocator, std.testing.io, .{
        .remote_addr = remote_addr,
        .server_name = "localhost",
        .max_outbound_queue_items = 4,
        .max_outbound_queue_bytes = 8,
    });
    defer byte_limited.deinit();

    try byte_limited.sendFrame("abcd");
    try std.testing.expectError(error.OutboundQueueFull, byte_limited.sendFrame("e"));
}

test "quic server rejects multi-connection option until fanout exists" {
    const listen_addr: std.Io.net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 0,
    } };

    try std.testing.expectError(error.InvalidConfig, quic.Connection.initServer(std.testing.allocator, std.testing.io, .{
        .listen_addr = listen_addr,
        .tls_cert_pem = "",
        .tls_key_pem = "",
        .max_concurrent_connections = 2,
    }));
}
