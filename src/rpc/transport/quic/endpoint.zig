const std = @import("std");
const quic_zig = @import("quic_zig");

const listener_mod = @import("listener.zig");
const quic_zig_adapter = @import("quic_zig_adapter.zig");
const session = @import("session.zig");

const Net = std.Io.net;

pub const Role = enum { client, server };

pub const Endpoint = union(Role) {
    client: ClientEndpoint,
    server: ServerEndpoint,

    pub fn driver(self: *Endpoint, io: std.Io) EndpointDriver {
        return .{
            .endpoint = self,
            .io = io,
        };
    }

    pub fn getAddress(self: *const Endpoint) Net.IpAddress {
        return switch (self.*) {
            .client => |*client| client.getAddress(),
            .server => |*server| server.listener.getAddress(),
        };
    }

    pub fn receiveTimeout(self: *const Endpoint) std.Io.Duration {
        return switch (self.*) {
            .client => |*client| client.receive_timeout,
            .server => |*server| server.listener.receive_timeout,
        };
    }
};

pub const Runtime = struct {
    endpoint: Endpoint,
    io: std.Io,

    pub fn init(endpoint: Endpoint, io: std.Io) Runtime {
        return .{
            .endpoint = endpoint,
            .io = io,
        };
    }

    pub fn driver(self: *Runtime) EndpointDriver {
        return self.endpoint.driver(self.io);
    }

    pub fn deinit(self: *Runtime) void {
        self.driver().deinit();
    }

    pub fn getAddress(self: *const Runtime) Net.IpAddress {
        return self.endpoint.getAddress();
    }

    pub fn activeQuicConnection(self: *Runtime) ?*quic_zig.Connection {
        return self.driver().quicConnection();
    }

    pub fn nowUs(self: *Runtime) u64 {
        return self.driver().nowUs();
    }
};

pub const ClientEndpoint = struct {
    socket: Net.Socket,
    transport: quic_zig.Client,
    remote_addr: Net.IpAddress,
    start_timestamp: std.Io.Timestamp,
    receive_timeout: std.Io.Duration,

    pub fn deinit(self: *ClientEndpoint, io: std.Io) void {
        self.socket.close(io);
        self.transport.deinit();
        self.* = undefined;
    }

    pub fn getAddress(self: *const ClientEndpoint) Net.IpAddress {
        return self.socket.address;
    }

    pub fn nowUs(self: *const ClientEndpoint, io: std.Io) u64 {
        const now = std.Io.Timestamp.now(io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
    }
};

/// Compatibility endpoint used by `Connection.initServer`.
///
/// It owns a `Listener` plus the accepted session slot currently attached to
/// the peer-facing `Connection`. A future multi-session server can keep the
/// listener as the owner and hand each accepted `Session` to its own transport
/// task without changing the existing client/peer callbacks.
pub const ServerEndpoint = struct {
    listener: listener_mod.Listener,
    session: session.AcceptedSessionDriver = .{},

    pub fn deinit(self: *ServerEndpoint) void {
        self.listener.deinit();
        self.* = undefined;
    }
};

/// Role-specific QUIC endpoint operations used by the shared connection loop.
///
/// `Connection` owns scheduling, callbacks, and Cap'n Proto framing. This driver
/// owns the small set of operations that differ between client and server
/// endpoints: socket access, endpoint-relative time, inbound datagram routing,
/// outbound datagram draining, and accepted-session reaping.
pub const EndpointDriver = struct {
    endpoint: *Endpoint,
    io: std.Io,

    pub fn deinit(self: EndpointDriver) void {
        switch (self.endpoint.*) {
            .client => |*client| client.deinit(self.io),
            .server => |*server| server.deinit(),
        }
    }

    pub fn getAddress(self: EndpointDriver) Net.IpAddress {
        return self.endpoint.getAddress();
    }

    pub fn receiveTimeout(self: EndpointDriver) std.Io.Duration {
        return self.endpoint.receiveTimeout();
    }

    pub fn activeSocket(self: EndpointDriver) *Net.Socket {
        return switch (self.endpoint.*) {
            .client => |*client| &client.socket,
            .server => |*server| &server.listener.socket,
        };
    }

    pub fn nowUs(self: EndpointDriver) u64 {
        return switch (self.endpoint.*) {
            .client => |*client| client.nowUs(self.io),
            .server => |*server| server.listener.nowUs(),
        };
    }

    pub fn quicConnection(self: EndpointDriver) ?*quic_zig.Connection {
        return switch (self.endpoint.*) {
            .client => |*client| client.transport.conn,
            .server => |*server| server.session.quicConnection(),
        };
    }

    pub fn handleDatagram(
        self: EndpointDriver,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !bool {
        switch (self.endpoint.*) {
            .client => |*client| {
                if (!Net.IpAddress.eql(&from, &client.remote_addr)) return false;
                try client.transport.conn.handle(bytes, quic_zig_adapter.ipAddressToPathAddress(from), now_us);
                return true;
            },
            .server => |*server| {
                _ = try server.listener.feedDatagram(bytes, from, now_us);
                _ = server.session.attachFirstAccepted(&server.listener.server);
                try server.listener.drainStatelessResponses();
                return true;
            },
        }
    }

    pub fn drainOutgoingDatagrams(
        self: EndpointDriver,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        switch (self.endpoint.*) {
            .client => |*client| {
                while (try client.transport.conn.pollDatagram(tx_buf, now_us)) |out| {
                    const dest = if (out.to) |addr|
                        quic_zig_adapter.pathAddressToIpAddress(addr) orelse client.remote_addr
                    else
                        client.remote_addr;
                    try client.socket.send(self.io, &dest, tx_buf[0..out.len]);
                }
            },
            .server => |*server| {
                const accepted = server.session.current() orelse return;
                try server.listener.drainAcceptedSessionDatagrams(accepted, tx_buf, now_us);
            },
        }
    }

    pub fn reapClosed(self: EndpointDriver) bool {
        return switch (self.endpoint.*) {
            .client => false,
            .server => |*server| server.session.reapClosed(&server.listener.server),
        };
    }
};
