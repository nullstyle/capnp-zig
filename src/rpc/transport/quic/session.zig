const std = @import("std");
const quic_zig = @import("quic_zig");

const quic_zig_adapter = @import("quic_zig_adapter.zig");

const Net = std.Io.net;

/// Listener slot ordinal used by the server `Connection` compatibility path.
///
/// The current peer transport exposes only one accepted server session. Fanout
/// code should carry the listener-provided ordinal instead of assuming this
/// value.
pub const compatibility_session_ordinal: usize = 0;

/// Borrowed handle for one accepted server-side QUIC session.
///
/// The owning `Listener` or compatibility `Connection` keeps the UDP socket
/// and `quic_zig.Server` alive. A `Session` is intentionally smaller: it names one
/// accepted quic_zig slot and exposes the pieces needed to drive that vat session.
pub const Session = struct {
    slot: *quic_zig.Server.Slot,

    pub fn fromSlot(slot: *quic_zig.Server.Slot) Session {
        return .{ .slot = slot };
    }

    pub fn quicConnection(self: Session) *quic_zig.Connection {
        return self.slot.conn;
    }

    pub fn peerAddress(self: Session) ?Net.IpAddress {
        const peer_addr = self.slot.peer_addr orelse return null;
        return quic_zig_adapter.pathAddressToIpAddress(peer_addr);
    }

    pub fn isClosed(self: Session) bool {
        return self.slot.conn.isClosed();
    }

    pub fn close(self: Session) void {
        self.slot.conn.close(false, 0, "");
    }

    pub fn pollDatagram(
        self: Session,
        tx_buf: []u8,
        now_us: u64,
    ) !?OutgoingDatagram {
        while (try self.slot.conn.pollDatagram(tx_buf, now_us)) |out| {
            const target = if (out.to) |addr| addr else self.slot.peer_addr orelse continue;
            const dest = quic_zig_adapter.pathAddressToIpAddress(target) orelse continue;
            return .{
                .dest = dest,
                .bytes = tx_buf[0..out.len],
            };
        }
        return null;
    }
};

pub const OutgoingDatagram = struct {
    dest: Net.IpAddress,
    bytes: []const u8,
};

/// Accepted server-side session selected by a listener.
///
/// `AcceptedSession` keeps the borrowed `Session` handle together with the
/// listener slot ordinal it came from. The compatibility transport always uses
/// ordinal zero; the fanout `Server` refreshes ordinals as quic-zig reaps and
/// compacts slots.
pub const AcceptedSession = struct {
    ordinal: usize,
    session: Session,

    pub fn fromSession(ordinal: usize, accepted: Session) AcceptedSession {
        return .{
            .ordinal = ordinal,
            .session = accepted,
        };
    }

    pub fn quicConnection(self: AcceptedSession) *quic_zig.Connection {
        return self.session.quicConnection();
    }

    pub fn peerAddress(self: AcceptedSession) ?Net.IpAddress {
        return self.session.peerAddress();
    }

    pub fn isClosed(self: AcceptedSession) bool {
        return self.session.isClosed();
    }

    pub fn close(self: AcceptedSession) void {
        self.session.close();
    }

    pub fn pollDatagram(
        self: AcceptedSession,
        tx_buf: []u8,
        now_us: u64,
    ) !?OutgoingDatagram {
        return try self.session.pollDatagram(tx_buf, now_us);
    }
};

/// Tracks the single server-side session used by the compatibility transport.
/// Multi-session fanout uses `ServerSession` values keyed by accepted slot.
pub const SessionTracker = struct {
    slot: ?*quic_zig.Server.Slot = null,

    pub fn isAccepted(self: *const SessionTracker) bool {
        return self.slot != null;
    }

    pub fn handle(self: *const SessionTracker) ?Session {
        const slot = self.slot orelse return null;
        return Session.fromSlot(slot);
    }

    pub fn quicConnection(self: *const SessionTracker) ?*quic_zig.Connection {
        const session = self.handle() orelse return null;
        return session.quicConnection();
    }

    pub fn adoptFirstAccepted(self: *SessionTracker, server: *quic_zig.Server) bool {
        if (self.slot != null) return false;
        const slots = server.iterator();
        if (slots.len == 0) return false;
        self.slot = slots[0];
        return true;
    }

    pub fn clear(self: *SessionTracker) void {
        self.slot = null;
    }
};

/// Drives the accepted session attached to the compatibility server transport.
///
/// This is intentionally one-session-sized. The fanout `Server` owns its own
/// table of independent session drivers while this helper keeps
/// `Connection.initServer` small.
pub const AcceptedSessionDriver = struct {
    tracker: SessionTracker = .{},

    pub fn isAttached(self: *const AcceptedSessionDriver) bool {
        return self.tracker.isAccepted();
    }

    pub fn current(self: *const AcceptedSessionDriver) ?AcceptedSession {
        const accepted = self.tracker.handle() orelse return null;
        return AcceptedSession.fromSession(compatibility_session_ordinal, accepted);
    }

    pub fn quicConnection(self: *const AcceptedSessionDriver) ?*quic_zig.Connection {
        const accepted = self.current() orelse return null;
        return accepted.quicConnection();
    }

    pub fn attachFirstAccepted(self: *AcceptedSessionDriver, server: *quic_zig.Server) bool {
        return self.tracker.adoptFirstAccepted(server);
    }

    pub fn reapClosed(self: *AcceptedSessionDriver, server: *quic_zig.Server) bool {
        if (self.current()) |accepted| {
            if (!accepted.isClosed()) return false;
            const reaped = server.reap();
            if (reaped == 0) return false;
            self.clear();
            return true;
        }

        _ = server.reap();
        return false;
    }

    pub fn clear(self: *AcceptedSessionDriver) void {
        self.tracker.clear();
    }
};
