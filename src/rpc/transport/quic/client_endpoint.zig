const std = @import("std");
const quic_zig = @import("quic_zig");

const quic_zig_adapter = @import("quic_zig_adapter.zig");

const Net = std.Io.net;

pub const ClientEndpoint = struct {
    socket: Net.Socket,
    transport: quic_zig.Client,
    remote_addr: Net.IpAddress,
    start_timestamp: std.Io.Timestamp,
    receive_timeout: std.Io.Duration,

    pub fn driver(self: *ClientEndpoint, io: std.Io) Driver {
        return .{
            .client = self,
            .io = io,
        };
    }

    pub fn deinit(self: *ClientEndpoint, io: std.Io) void {
        self.socket.close(io);
        self.transport.deinit();
        self.* = undefined;
    }

    pub fn getAddress(self: *const ClientEndpoint) Net.IpAddress {
        return self.socket.address;
    }

    pub fn receiveTimeout(self: *const ClientEndpoint) std.Io.Duration {
        return self.receive_timeout;
    }

    pub fn nowUs(self: *const ClientEndpoint, io: std.Io) u64 {
        const now = std.Io.Timestamp.now(io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
    }
};

pub const Driver = struct {
    client: *ClientEndpoint,
    io: std.Io,

    pub fn deinit(self: Driver) void {
        self.client.deinit(self.io);
    }

    pub fn getAddress(self: Driver) Net.IpAddress {
        return self.client.getAddress();
    }

    pub fn receiveTimeout(self: Driver) std.Io.Duration {
        return self.client.receiveTimeout();
    }

    pub fn activeSocket(self: Driver) *Net.Socket {
        return &self.client.socket;
    }

    pub fn nowUs(self: Driver) u64 {
        return self.client.nowUs(self.io);
    }

    pub fn quicConnection(self: Driver) ?*quic_zig.Connection {
        return self.client.transport.conn;
    }

    pub fn handleDatagram(
        self: Driver,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !bool {
        if (!Net.IpAddress.eql(&from, &self.client.remote_addr)) return false;
        try self.client.transport.conn.handle(
            bytes,
            quic_zig_adapter.ipAddressToPathAddress(from),
            now_us,
        );
        return true;
    }

    pub fn drainOutgoingDatagrams(
        self: Driver,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        while (try self.client.transport.conn.pollDatagram(tx_buf, now_us)) |out| {
            const dest = if (out.to) |addr|
                quic_zig_adapter.pathAddressToIpAddress(addr) orelse self.client.remote_addr
            else
                self.client.remote_addr;
            try self.client.socket.send(self.io, &dest, tx_buf[0..out.len]);
        }
    }

    pub fn reapClosed(_: Driver) bool {
        return false;
    }
};
