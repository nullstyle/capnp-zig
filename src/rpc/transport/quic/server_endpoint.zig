const std = @import("std");
const quic_zig = @import("quic_zig");

const listener_mod = @import("listener.zig");
const session = @import("session.zig");

const Net = std.Io.net;

/// Compatibility endpoint used by `Connection.initServer`.
///
/// It owns a `Listener` plus the accepted session slot currently attached to
/// the peer-facing `Connection`. A future multi-session server can keep the
/// listener as the owner and hand each accepted `Session` to its own transport
/// task without changing the existing client/peer callbacks.
pub const ServerEndpoint = struct {
    listener: listener_mod.Listener,
    session: session.AcceptedSessionDriver = .{},

    pub fn driver(self: *ServerEndpoint) Driver {
        return .{ .server = self };
    }

    pub fn deinit(self: *ServerEndpoint) void {
        self.listener.deinit();
        self.* = undefined;
    }

    pub fn getAddress(self: *const ServerEndpoint) Net.IpAddress {
        return self.listener.getAddress();
    }

    pub fn receiveTimeout(self: *const ServerEndpoint) std.Io.Duration {
        return self.listener.receive_timeout;
    }
};

pub const Driver = struct {
    server: *ServerEndpoint,

    pub fn deinit(self: Driver) void {
        self.server.deinit();
    }

    pub fn getAddress(self: Driver) Net.IpAddress {
        return self.server.getAddress();
    }

    pub fn receiveTimeout(self: Driver) std.Io.Duration {
        return self.server.receiveTimeout();
    }

    pub fn activeSocket(self: Driver) *Net.Socket {
        return &self.server.listener.socket;
    }

    pub fn nowUs(self: Driver) u64 {
        return self.server.listener.nowUs();
    }

    pub fn quicConnection(self: Driver) ?*quic_zig.Connection {
        return self.server.session.quicConnection();
    }

    pub fn handleDatagram(
        self: Driver,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !bool {
        _ = try self.server.listener.feedDatagram(bytes, from, now_us);
        _ = self.server.session.attachFirstAccepted(&self.server.listener.server);
        try self.server.listener.drainStatelessResponses();
        return true;
    }

    pub fn drainOutgoingDatagrams(
        self: Driver,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        const accepted = self.server.session.current() orelse return;
        try self.server.listener.drainAcceptedSessionDatagrams(accepted, tx_buf, now_us);
    }

    pub fn reapClosed(self: Driver) bool {
        return self.server.session.reapClosed(&self.server.listener.server);
    }
};
