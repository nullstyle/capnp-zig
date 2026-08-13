const std = @import("std");
const quic_zig = @import("quic");

const client_endpoint = @import("client_endpoint.zig");
const server_endpoint = @import("server_endpoint.zig");

const Net = std.Io.net;

pub const Role = enum { client, server };
pub const ClientEndpoint = client_endpoint.ClientEndpoint;
pub const ServerEndpoint = server_endpoint.ServerEndpoint;

pub const Endpoint = union(Role) {
    client: ClientEndpoint,
    /// Server compatibility endpoint. Multi-session servers should own a
    /// `Listener` directly and hand out `Session` handles instead of routing
    /// every session through this one active endpoint.
    server: ServerEndpoint,

    pub fn driver(self: *Endpoint, io: std.Io) EndpointDriver {
        return switch (self.*) {
            .client => |*client| .{ .client = client.driver(io) },
            .server => |*server| .{ .server = server.driver() },
        };
    }

    pub fn getAddress(self: *const Endpoint) Net.IpAddress {
        return switch (self.*) {
            .client => |*client| client.getAddress(),
            .server => |*server| server.getAddress(),
        };
    }

    pub fn receiveTimeout(self: *const Endpoint) std.Io.Duration {
        return switch (self.*) {
            .client => |*client| client.receiveTimeout(),
            .server => |*server| server.receiveTimeout(),
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

/// Role-specific QUIC endpoint operations used by the shared one-session loop.
///
/// `Connection` owns scheduling, callbacks, and Cap'n Proto framing. This driver
/// is now a small facade over the concrete client and server endpoint drivers,
/// keeping socket routing and accepted-session mechanics out of this module.
/// On the server side it always points at the single accepted compatibility
/// session selected by `ServerEndpoint`.
pub const EndpointDriver = union(Role) {
    client: client_endpoint.Driver,
    server: server_endpoint.Driver,

    pub fn deinit(self: EndpointDriver) void {
        switch (self) {
            .client => |driver| driver.deinit(),
            .server => |driver| driver.deinit(),
        }
    }

    pub fn getAddress(self: EndpointDriver) Net.IpAddress {
        return switch (self) {
            .client => |driver| driver.getAddress(),
            .server => |driver| driver.getAddress(),
        };
    }

    pub fn receiveTimeout(self: EndpointDriver) std.Io.Duration {
        return switch (self) {
            .client => |driver| driver.receiveTimeout(),
            .server => |driver| driver.receiveTimeout(),
        };
    }

    pub fn activeSocket(self: EndpointDriver) *Net.Socket {
        return switch (self) {
            .client => |driver| driver.activeSocket(),
            .server => |driver| driver.activeSocket(),
        };
    }

    pub fn nowUs(self: EndpointDriver) u64 {
        return switch (self) {
            .client => |driver| driver.nowUs(),
            .server => |driver| driver.nowUs(),
        };
    }

    pub fn quicConnection(self: EndpointDriver) ?*quic_zig.Connection {
        return switch (self) {
            .client => |driver| driver.quicConnection(),
            .server => |driver| driver.quicConnection(),
        };
    }

    pub fn handleDatagram(
        self: EndpointDriver,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !bool {
        return switch (self) {
            .client => |driver| try driver.handleDatagram(bytes, from, now_us),
            .server => |driver| try driver.handleDatagram(bytes, from, now_us),
        };
    }

    pub fn drainOutgoingDatagrams(
        self: EndpointDriver,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        switch (self) {
            .client => |driver| try driver.drainOutgoingDatagrams(tx_buf, now_us),
            .server => |driver| try driver.drainOutgoingDatagrams(tx_buf, now_us),
        }
    }

    pub fn reapClosed(self: EndpointDriver) bool {
        return switch (self) {
            .client => |driver| driver.reapClosed(),
            .server => |driver| driver.reapClosed(),
        };
    }
};
