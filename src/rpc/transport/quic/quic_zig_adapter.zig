const std = @import("std");
const quic_zig = @import("quic");

const Net = std.Io.net;

pub fn defaultClientBindAddress(remote_addr: Net.IpAddress) Net.IpAddress {
    return switch (remote_addr) {
        .ip4 => .{ .ip4 = .unspecified(0) },
        .ip6 => .{ .ip6 = .unspecified(0) },
    };
}

/// Ask the kernel for bigger buffers on a UDP socket this transport bound.
/// Best effort on purpose: the OS default still works, only with less burst
/// room, so a refused or capped request must not fail the bind. quic-zig's
/// helpers try `SO_RCVBUFFORCE` / `SO_SNDBUFFORCE` first on Linux and report
/// `error.Unsupported` on Windows.
pub fn requestUdpSocketBuffers(handle: Net.Socket.Handle, recv_bytes: ?usize, send_bytes: ?usize) void {
    if (recv_bytes) |bytes| quic_zig.transport.setRecvBufferSize(handle, bytes) catch {};
    if (send_bytes) |bytes| quic_zig.transport.setSendBufferSize(handle, bytes) catch {};
}

/// Write to a stream of `conn`, and leave half of its memory budget for
/// what the peer sends. Every stream write of the transport's engines goes
/// through here (`writeStream` for their generic `conn`).
///
/// Since quic-zig v0.33.0 `max_connection_memory` bounds the application's
/// own writes as back-pressure: a write takes what the budget leaves and
/// returns short. It takes all of it, though, and the same budget holds
/// what the peer sends, where running out is a fault: the peer's next
/// STREAM frame finds no room, and quic-zig closes the connection with
/// EXCESSIVE_LOAD. A small Finish, Release or pipelined call from an honest
/// client in the middle of a large reply did that. So the transport's own
/// writes stop once the connection holds half of its budget, the most that
/// quic-zig lets a connection window grow to for a budget of 32 MiB or less
/// (its cap is 16 MiB or half the budget, whichever is smaller). Half is a
/// floor, not a guarantee: quic-zig can charge a receive buffer up to twice
/// its unread bytes, and the budget does not lower the announced windows
/// ("Current Limits" in docs/quic-transport.md). A short count, zero when
/// nothing fits, is back-pressure as before. With no room the call still
/// reaches quic-zig, with no bytes, which is what quic-zig does itself when
/// its budget is full, so the stream errors stay the same.
pub fn streamWrite(conn: *quic_zig.Connection, stream_id: u64, data: []const u8) !usize {
    return conn.streamWrite(stream_id, data[0..@min(data.len, ownWriteRoom(conn))]);
}

/// The bytes the transport's own writes may still add to `conn`: half of
/// `max_connection_memory`, less what the connection holds (send buffers,
/// receive buffers, CRYPTO and DATAGRAM data together).
pub fn ownWriteRoom(conn: *const quic_zig.Connection) usize {
    const ceiling = conn.max_connection_memory / 2;
    return std.math.lossyCast(usize, ceiling -| conn.bytes_resident);
}

/// `streamWrite` for the engines' generic `conn`: a quic-zig connection
/// takes the write above; any other type (`EmbeddedSession`'s buffered
/// view, which calls `streamWrite` itself, or a test double) its own
/// `streamWrite`.
pub fn writeStream(conn: anytype, stream_id: u64, data: []const u8) !usize {
    if (@TypeOf(conn) == *quic_zig.Connection) return streamWrite(conn, stream_id, data);
    return conn.streamWrite(stream_id, data);
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
