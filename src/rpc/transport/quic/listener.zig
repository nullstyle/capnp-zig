const std = @import("std");
const quic_zig = @import("quic_zig");

const quic_zig_adapter = @import("quic_zig_adapter.zig");
const quic_options = @import("options.zig");
const session_mod = @import("session.zig");

const Net = std.Io.net;

pub const Session = session_mod.Session;
pub const FeedOutcome = quic_zig.Server.FeedOutcome;
pub const StatelessResponse = quic_zig.Server.StatelessResponse;

/// Server-side QUIC endpoint owner.
///
/// `Listener` owns the UDP socket and `quic_zig.Server`. Accepted QUIC sessions are
/// surfaced as borrowed `Session` handles, which is the boundary future
/// multi-client fanout can grow through. The current implementation preserves
/// the existing one-session guard in `ServerOptions`.
pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: Net.Socket,
    server: quic_zig.Server,
    start_timestamp: std.Io.Timestamp,
    receive_timeout: std.Io.Duration,
    max_concurrent_sessions: u32,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: quic_options.ServerOptions,
    ) !Listener {
        const server_config = try quic_options.serverConfigFromOptions(allocator, options);

        const socket = try Net.IpAddress.bind(&options.listen_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);

        var server = try quic_zig.Server.init(server_config);
        errdefer server.deinit();

        return .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .server = server,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .receive_timeout = options.receive_timeout,
            .max_concurrent_sessions = options.max_concurrent_connections,
        };
    }

    pub fn deinit(self: *Listener) void {
        self.socket.close(self.io);
        self.server.deinit();
        self.* = undefined;
    }

    pub fn getAddress(self: *const Listener) Net.IpAddress {
        return self.socket.address;
    }

    pub fn sessionCapacity(self: *const Listener) u32 {
        return self.max_concurrent_sessions;
    }

    pub fn sessionCount(self: *const Listener) usize {
        return self.server.connectionCount();
    }

    pub fn firstSession(self: *Listener) ?Session {
        return self.sessionAt(0);
    }

    pub fn sessionAt(self: *Listener, index: usize) ?Session {
        const slots = self.server.iterator();
        if (index >= slots.len) return null;
        return Session.fromSlot(slots[index]);
    }

    pub fn feedDatagram(
        self: *Listener,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !FeedOutcome {
        return try self.server.feed(bytes, quic_zig_adapter.ipAddressToPathAddress(from), now_us);
    }

    pub fn receiveOne(
        self: *Listener,
        rx_buf: []u8,
    ) !?FeedOutcome {
        const msg = self.socket.receiveTimeout(self.io, rx_buf, .{
            .duration = .{
                .raw = self.receive_timeout,
                .clock = .awake,
            },
        }) catch |err| switch (err) {
            error.Timeout => return null,
            else => return err,
        };
        if (msg.flags.trunc) return error.DatagramTooLarge;
        const outcome = try self.feedDatagram(msg.data, msg.from, self.nowUs());
        try self.drainStatelessResponses();
        return outcome;
    }

    pub fn drainStatelessResponses(self: *Listener) !void {
        while (self.server.drainStatelessResponse()) |response| {
            const dest = quic_zig_adapter.pathAddressToIpAddress(response.dst) orelse continue;
            try self.socket.send(self.io, &dest, response.slice());
        }
    }

    pub fn drainSessionDatagrams(
        self: *Listener,
        session: Session,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        while (try session.pollDatagram(tx_buf, now_us)) |out| {
            try self.socket.send(self.io, &out.dest, out.bytes);
        }
    }

    pub fn tick(self: *Listener, now_us: u64) !void {
        try self.server.tick(now_us);
    }

    pub fn reapClosedSessions(self: *Listener) usize {
        return self.server.reap();
    }

    pub fn nowUs(self: *const Listener) u64 {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return 0;
        return @intCast(delta);
    }
};
