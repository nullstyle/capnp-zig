const std = @import("std");
const builtin = @import("builtin");
const quic_zig = @import("quic");

const datagram_drop = @import("datagram_drop.zig");
const datagram_io = @import("datagram_io.zig");
const events = @import("../../events.zig");
const quic_zig_adapter = @import("quic_zig_adapter.zig");
const non_windows_receive = @import("non_windows_receive.zig");
const quic_options = @import("options.zig");
const session_mod = @import("session.zig");
const udp_receive_bridge = @import("udp_receive_bridge.zig");

const Net = std.Io.net;

pub const Session = session_mod.Session;
pub const AcceptedSession = session_mod.AcceptedSession;
pub const FeedOutcome = quic_zig.Server.FeedOutcome;
pub const StatelessResponse = quic_zig.Server.StatelessResponse;

/// Server-side QUIC endpoint owner.
///
/// `Listener` owns the UDP socket and `quic_zig.Server`. Accepted QUIC sessions are
/// surfaced as borrowed `Session` handles, which is the boundary future
/// multi-client fanout can grow through. The current implementation preserves
/// the existing one-session guard in `ServerOptions`.
///
/// This type deliberately has no Cap'n Proto RPC callbacks. A fanout server is
/// expected to accept/choose sessions here, then hand each selected session to a
/// per-session transport driver.
pub const Listener = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    socket: Net.Socket,
    server: quic_zig.Server,
    /// Stable storage for the cancellable Windows receive. A timed wait can
    /// return while the kernel still owns this buffer, so it must not borrow a
    /// different caller slice on each `receiveOne` invocation.
    udp_rx_buf: []u8,
    /// The monotonic (`.awake`) clock at `init`. `nowUs` advances from it.
    start_timestamp: std.Io.Timestamp,
    /// The wall clock (`.real`) at `init`, in microseconds since the Unix
    /// epoch: the value `nowUs` starts from. See `nowUs`.
    clock_origin_us: u64,
    receive_timeout: std.Io.Duration,
    max_concurrent_sessions: u32,
    udp_receive: udp_receive_bridge.Bridge = .{},
    observer: ?events.Observer,
    /// Configured transport mode, kept only to tag pre-session observer
    /// events. A dropped datagram arrives before any session exists, so the
    /// endpoint's configured mode is the most specific source available.
    event_source: events.Source,
    /// Oversized inbound datagrams dropped on this UDP endpoint. Mutated only
    /// by the loop thread that drives the receive; see `droppedDatagramCount`.
    dropped_datagrams: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        options: quic_options.ServerOptions,
    ) !Listener {
        var server_config = try quic_options.serverConfigFromOptions(allocator, options);
        // The config holds the session-ticket keys (current and previous)
        // by value. quic-zig keeps its own copies (and clears them at
        // deinit), so ours go as soon as `quic_zig.Server.init` has read
        // them, on every path.
        defer quic_options.zeroServerConfigSecrets(&server_config);

        const udp_rx_buf = try allocator.alloc(u8, options.udp_rx_buffer_size);
        errdefer allocator.free(udp_rx_buf);

        const socket = try Net.IpAddress.bind(&options.listen_addr, io, .{
            .mode = .dgram,
            .protocol = .udp,
        });
        errdefer socket.close(io);
        quic_zig_adapter.requestUdpSocketBuffers(socket.handle, options.udp_socket_recv_buffer_bytes, options.udp_socket_send_buffer_bytes);

        var server = try quic_zig.Server.init(server_config);
        errdefer server.deinit();

        return .{
            .allocator = allocator,
            .io = io,
            .socket = socket,
            .server = server,
            .udp_rx_buf = udp_rx_buf,
            .start_timestamp = std.Io.Timestamp.now(io, .awake),
            .clock_origin_us = wallClockUs(io),
            .receive_timeout = options.receive_timeout,
            .max_concurrent_sessions = options.max_concurrent_connections,
            .observer = options.observer,
            .event_source = datagram_drop.eventSource(options.mode),
        };
    }

    pub fn deinit(self: *Listener) void {
        self.udp_receive.cancel(self.io);
        self.socket.close(self.io);
        self.server.deinit();
        self.allocator.free(self.udp_rx_buf);
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

    /// Compatibility helper for callers that only expect one accepted session.
    pub fn firstSession(self: *Listener) ?Session {
        return self.sessionAt(0);
    }

    pub fn sessionAt(self: *Listener, index: usize) ?Session {
        const accepted = self.acceptedSessionAt(index) orelse return null;
        return accepted.session;
    }

    pub fn firstAcceptedSession(self: *Listener) ?AcceptedSession {
        return self.acceptedSessionAt(0);
    }

    /// Return the currently accepted listener session at `index`.
    ///
    /// Handles are borrowed from `quic_zig.Server`; callers must not retain them
    /// past listener mutation that can reap or close the backing slot.
    pub fn acceptedSessionAt(self: *Listener, index: usize) ?AcceptedSession {
        const slots = self.server.iterator();
        if (index >= slots.len) return null;
        return AcceptedSession.fromSession(index, Session.fromSlot(slots[index]));
    }

    pub fn feedDatagram(
        self: *Listener,
        bytes: []u8,
        from: Net.IpAddress,
        now_us: u64,
    ) !FeedOutcome {
        return try self.server.feed(bytes, quic_zig_adapter.ipAddressToPathAddress(from), now_us);
    }

    /// Receive and feed at most one datagram.
    ///
    /// `null` means nothing was fed to QUIC this call: a receive timeout, a
    /// wake, or an oversized datagram dropped as a per-datagram fault. The
    /// three are deliberately not distinguished in the return value — none of
    /// them is a listener fault and none of them is actionable by the caller.
    /// A drop is reported out-of-band instead, through the observer and
    /// `droppedDatagramCount`, so it cannot silently look like an idle poll.
    pub fn receiveOne(
        self: *Listener,
        rx_buf: []u8,
    ) !?FeedOutcome {
        if (comptime builtin.target.os.tag == .windows) {
            // ICMP-driven faults (PortUnreachable / ConnectionResetByPeer)
            // are per-datagram, never endpoint-fatal: an off-path packet can
            // provoke them, and a departed peer's ICMP feedback must not fail
            // the listener. Same classification as `datagram_io.receiveOne`.
            const received = self.receiveConcurrent(self.receive_timeout) catch |err| {
                if (datagram_io.isTransientPeerFault(err)) {
                    datagram_drop.reportPeerFault(err);
                    return null;
                }
                return err;
            };
            return switch (received) {
                .timeout, .wake => null,
                // The Windows receive always borrows Listener-owned storage,
                // so the ceiling that was exceeded is this buffer's, not the
                // caller's `rx_buf`.
                .truncated => blk: {
                    self.recordDroppedDatagram(self.udp_rx_buf.len);
                    break :blk null;
                },
                .datagram => |msg| blk: {
                    const outcome = try self.feedDatagram(msg.data, msg.from, self.nowUs());
                    try self.drainStatelessResponses();
                    break :blk outcome;
                },
            };
        }

        const msg = non_windows_receive.receive(&self.socket, self.io, rx_buf, self.receive_timeout) catch |err| switch (err) {
            error.Timeout => return null,
            else => {
                // Mirror of the Windows arm above.
                if (datagram_io.isTransientPeerFault(err)) {
                    datagram_drop.reportPeerFault(err);
                    return null;
                }
                return err;
            },
        };
        if (msg.flags.trunc) {
            self.recordDroppedDatagram(rx_buf.len);
            return null;
        }
        const outcome = try self.feedDatagram(msg.data, msg.from, self.nowUs());
        try self.drainStatelessResponses();
        return outcome;
    }

    /// Count and report one oversized datagram dropped on this endpoint.
    ///
    /// `Server` drives its own receive against this listener's socket and
    /// routes its truncation arms here too, so the tally is per UDP endpoint
    /// rather than per API entry point. `rx_buf_len` is the buffer the failed
    /// receive actually used.
    pub fn recordDroppedDatagram(self: *Listener, rx_buf_len: usize) void {
        self.dropped_datagrams +|= 1;
        datagram_drop.report(self.observer, self.event_source, .server, rx_buf_len);
    }

    /// Oversized datagrams dropped since `init`. Loop-thread only: the counter
    /// is a plain field written by whichever thread drives the receive.
    pub fn droppedDatagramCount(self: *const Listener) u64 {
        return self.dropped_datagrams;
    }

    fn receiveConcurrent(
        self: *Listener,
        wait_duration: std.Io.Duration,
    ) !udp_receive_bridge.Bridge.WaitResult {
        return try self.udp_receive.receive(
            self.io,
            self.socket,
            self.udp_rx_buf,
            wait_duration,
        );
    }

    pub const TestingHooks = if (builtin.is_test) struct {
        /// Exercise the Windows receive shape on every host. `caller_buffer`
        /// mirrors the public API argument but is deliberately ignored: the
        /// retained kernel operation always borrows Listener-owned storage.
        pub fn receiveConcurrent(
            listener: *Listener,
            caller_buffer: []u8,
            wait_duration: std.Io.Duration,
        ) !udp_receive_bridge.Bridge.WaitResult {
            _ = caller_buffer;
            return try listener.receiveConcurrent(wait_duration);
        }

        pub fn receiveStoragePtr(listener: *Listener) [*]u8 {
            return listener.udp_rx_buf.ptr;
        }
    } else struct {};

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

    pub fn drainAcceptedSessionDatagrams(
        self: *Listener,
        accepted: AcceptedSession,
        tx_buf: []u8,
        now_us: u64,
    ) !void {
        while (try accepted.pollDatagram(tx_buf, now_us)) |out| {
            try self.socket.send(self.io, &out.dest, out.bytes);
        }
    }

    pub fn tick(self: *Listener, now_us: u64) !void {
        try self.server.tick(now_us);
    }

    pub fn reapClosedSessions(self: *Listener) usize {
        return self.server.reap();
    }

    /// Change the key that session tickets are sealed under, with no
    /// restart and no lost ticket (quic-zig `Server.rotateSessionTicketKey`).
    /// New tickets are sealed under `new_key` at once. The key that sealed
    /// until now still opens tickets, 0-RTT included, for one ticket
    /// lifetime (`ServerOptions.session_ticket_lifetime_s`, or 2 days)
    /// counted on this listener's clock (`nowUs`); then quic-zig clears it.
    /// A second rotation before that drops it at once: two keys, not more.
    /// The key is read once; nothing keeps the pointer.
    ///
    /// Call it on the thread that feeds this listener (`receiveOne`,
    /// `feedDatagram`, `tick`): quic-zig takes no lock, and a call from
    /// another thread races the handshakes that read the keys. In a Debug
    /// build quic-zig checks the thread: the first feed, tick or rotation
    /// fixes it, and a later call from another thread asserts. So a rotation
    /// before the first datagram must run on the thread that will feed the
    /// listener. To start with an old key still open, set
    /// `ServerOptions.previous_session_ticket_key` instead.
    /// `Server.rotateSessionTicketKey` checks its own loop thread too.
    ///
    /// `error.InvalidConfig`, and nothing changes: the listener was built
    /// with no `ServerOptions.session_ticket_key`, `new_key` is all zero, or
    /// `new_key` has the same key name (its first 16 bytes) as the current
    /// key.
    pub fn rotateSessionTicketKey(self: *Listener, new_key: *const quic_options.SessionTicketKey) !void {
        try self.server.rotateSessionTicketKey(new_key.*, self.nowUs());
    }

    /// The clock for this listener's quic-zig server, in microseconds since
    /// the Unix epoch. `receiveOne` and the QUIC `Server` loop pass it to
    /// every `feed`, `tick` and `pollDatagram`, and quic-zig stamps a
    /// NEW_TOKEN's issue and expiry times with it (unless
    /// `ServerOptions.new_token_clock` gives a clock of its own).
    /// `ServerOptions.previous_session_ticket_key_until_us` is on this
    /// clock too.
    ///
    /// It starts at the wall clock read once at `init` and advances with the
    /// monotonic `.awake` clock. So it never goes backwards within a
    /// process, and a wall-clock step moves no timer. Because every process
    /// starts it at the wall clock, a restarted listener's clock continues
    /// from its predecessor's: it accepts the NEW_TOKENs that its predecessor
    /// issued, within their lifetime, and enforces that lifetime in wall
    /// time across restarts.
    ///
    /// By default quic-zig checks a NEW_TOKEN's times with no clock-skew
    /// allowance (`ServerOptions.new_token_max_clock_skew_us` adds one). A
    /// successor reads its predecessor's token as not yet valid for as long
    /// as the predecessor's clock ran ahead of the wall clock when it issued
    /// the token: after a wall-clock step backwards between the two starts,
    /// or after a long uptime on a monotonic clock that runs fast (on macOS
    /// `.awake` is not NTP-disciplined; it drifts by parts per million). Time
    /// that `.awake` does not count, such as a suspend, has the opposite
    /// effect: a token looks older and expires sooner.
    pub fn nowUs(self: *const Listener) u64 {
        const now = std.Io.Timestamp.now(self.io, .awake);
        const delta = self.start_timestamp.durationTo(now).toMicroseconds();
        if (delta <= 0) return self.clock_origin_us;
        return self.clock_origin_us +| @as(u64, @intCast(delta));
    }
};

/// The wall clock in microseconds since the Unix epoch; 0 for a clock
/// before the epoch.
fn wallClockUs(io: std.Io) u64 {
    const us = std.Io.Timestamp.now(io, .real).toMicroseconds();
    if (us <= 0) return 0;
    return @intCast(us);
}
