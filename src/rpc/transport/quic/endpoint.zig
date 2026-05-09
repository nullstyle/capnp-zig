const std = @import("std");
const quic_zig = @import("quic_zig");

const listener_mod = @import("listener.zig");
const session = @import("session.zig");

const Net = std.Io.net;

pub const Role = enum { client, server };

pub const Endpoint = union(Role) {
    client: ClientEndpoint,
    server: ServerEndpoint,
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
    session: session.SessionTracker = .{},

    pub fn deinit(self: *ServerEndpoint) void {
        self.listener.deinit();
        self.* = undefined;
    }
};
