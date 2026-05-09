const std = @import("std");
const quic_zig = @import("quic_zig");

const Net = std.Io.net;

pub fn defaultClientBindAddress(remote_addr: Net.IpAddress) Net.IpAddress {
    return switch (remote_addr) {
        .ip4 => .{ .ip4 = .unspecified(0) },
        .ip6 => .{ .ip6 = .unspecified(0) },
    };
}

pub fn ipAddressToPathAddress(addr: Net.IpAddress) quic_zig.conn.path.Address {
    var out: quic_zig.conn.path.Address = .{};
    switch (addr) {
        .ip4 => |ip4| {
            out.bytes[0] = 4;
            @memcpy(out.bytes[1..5], &ip4.bytes);
            std.mem.writeInt(u16, out.bytes[5..7], ip4.port, .big);
        },
        .ip6 => |ip6| {
            out.bytes[0] = 6;
            @memcpy(out.bytes[1..17], &ip6.bytes);
            std.mem.writeInt(u16, out.bytes[17..19], ip6.port, .big);
            out.bytes[19] = @truncate(ip6.flow >> 16);
            out.bytes[20] = @truncate(ip6.flow >> 8);
            out.bytes[21] = @truncate(ip6.flow);
        },
    }
    return out;
}

pub fn pathAddressToIpAddress(addr: quic_zig.conn.path.Address) ?Net.IpAddress {
    switch (addr.bytes[0]) {
        4 => {
            var ip4_bytes: [4]u8 = undefined;
            @memcpy(&ip4_bytes, addr.bytes[1..5]);
            const port = std.mem.readInt(u16, addr.bytes[5..7], .big);
            return .{ .ip4 = .{ .bytes = ip4_bytes, .port = port } };
        },
        6 => {
            var ip6_bytes: [16]u8 = undefined;
            @memcpy(&ip6_bytes, addr.bytes[1..17]);
            const port = std.mem.readInt(u16, addr.bytes[17..19], .big);
            const flow: u32 = (@as(u32, addr.bytes[19]) << 16) |
                (@as(u32, addr.bytes[20]) << 8) |
                @as(u32, addr.bytes[21]);
            return .{ .ip6 = .{
                .bytes = ip6_bytes,
                .port = port,
                .flow = flow,
            } };
        },
        else => return null,
    }
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
