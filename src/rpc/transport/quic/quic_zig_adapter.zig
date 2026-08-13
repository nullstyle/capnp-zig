const std = @import("std");
const quic_zig = @import("quic");

const Net = std.Io.net;

pub fn defaultClientBindAddress(remote_addr: Net.IpAddress) Net.IpAddress {
    return switch (remote_addr) {
        .ip4 => .{ .ip4 = .unspecified(0) },
        .ip6 => .{ .ip6 = .unspecified(0) },
    };
}

pub fn ipAddressToPathAddress(addr: Net.IpAddress) quic_zig.conn.path.Address {
    // quic-zig's Address deliberately mirrors std.Io.net.IpAddress, so the
    // boundary is a one-to-one variant map.
    return switch (addr) {
        .ip4 => |ip4| .{ .ipv4 = .{ .addr = ip4.bytes, .port = ip4.port } },
        .ip6 => |ip6| .{ .ipv6 = .{
            .addr = ip6.bytes,
            .port = ip6.port,
            .flow = ip6.flow,
        } },
    };
}

pub fn pathAddressToIpAddress(addr: quic_zig.conn.path.Address) ?Net.IpAddress {
    return switch (addr) {
        .unspecified => null,
        .ipv4 => |v4| .{ .ip4 = .{ .bytes = v4.addr, .port = v4.port } },
        .ipv6 => |v6| .{ .ip6 = .{
            .bytes = v6.addr,
            .port = v6.port,
            .flow = v6.flow,
        } },
    };
}

test "QUIC path address round-trips IPv4" {
    const addr: Net.IpAddress = .{ .ip4 = .{
        .bytes = .{ 127, 0, 0, 1 },
        .port = 7001,
    } };
    const path_addr = ipAddressToPathAddress(addr);
    const round_trip = pathAddressToIpAddress(path_addr).?;
    try std.testing.expect(round_trip == .ip4);
    try std.testing.expectEqual(addr.ip4.port, round_trip.ip4.port);
    try std.testing.expectEqualSlices(u8, &addr.ip4.bytes, &round_trip.ip4.bytes);
}
