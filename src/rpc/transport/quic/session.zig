const std = @import("std");
const quic_zig = @import("quic_zig");

const quic_zig_adapter = @import("quic_zig_adapter.zig");

const Net = std.Io.net;

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

/// Tracks the single server-side session used by the current compatibility
/// transport. When fanout lands, this can become a table keyed by slot id while
/// `Connection` keeps its one-session behavior.
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
