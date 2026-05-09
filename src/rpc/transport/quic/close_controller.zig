const std = @import("std");
const quic_zig = @import("quic_zig");

const datagram_io = @import("datagram_io.zig");
const endpoint_mod = @import("endpoint.zig");
const quic_close = @import("close.zig");

const log = std.log.scoped(.rpc_quic_close);

pub const Controller = struct {
    requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    state: quic_close.State = .{},

    pub fn init(reveal_detail_on_wire: bool) Controller {
        return .{
            .state = quic_close.State.init(reveal_detail_on_wire),
        };
    }

    pub fn request(self: *Controller) void {
        _ = self.requested.swap(true, .acq_rel);
    }

    pub fn isRequested(self: *const Controller) bool {
        return self.requested.load(.acquire);
    }

    pub fn enterClosing(
        self: *Controller,
        baseline: anytype,
        native: anytype,
    ) void {
        self.request();
        self.closeEngines(baseline, native);
    }

    pub fn requestNormal(
        self: *Controller,
        baseline: anytype,
        native: anytype,
    ) void {
        self.enterClosing(baseline, native);
        self.record(.normal, null);
    }

    pub fn closeEngines(
        _: *Controller,
        baseline: anytype,
        native: anytype,
    ) void {
        baseline.close();
        native.close();
    }

    pub fn closeActive(
        self: *Controller,
        conn: ?*quic_zig.Connection,
        code: quic_close.ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        self.record(code, err);
        const active = conn orelse return;
        self.closeQuicConn(active);
    }

    pub fn flushCloseDatagram(
        self: *Controller,
        conn: ?*quic_zig.Connection,
        driver: endpoint_mod.EndpointDriver,
        udp_tx_buf: []u8,
        now_us: u64,
    ) void {
        const active = conn orelse return;
        if (!active.isClosed()) {
            self.closeQuicConn(active);
        }
        datagram_io.drainOutgoingDatagrams(driver, udp_tx_buf, now_us) catch |err| {
            log.debug("failed to flush QUIC close datagram: {}", .{err});
        };
    }

    pub fn record(
        self: *Controller,
        code: quic_close.ApplicationCloseCode,
        err: ?anyerror,
    ) void {
        self.state.record(code, err);
    }

    pub fn status(self: *const Controller) ?quic_close.Status {
        return self.state.status();
    }

    pub fn reason(self: *const Controller) []const u8 {
        return self.state.reason();
    }

    pub fn setRevealDetailOnWire(self: *Controller, reveal: bool) void {
        self.state.reveal_detail_on_wire = reveal;
    }

    fn closeQuicConn(self: *Controller, conn: *quic_zig.Connection) void {
        self.record(.normal, null);
        const status_value = self.status() orelse return;
        conn.close(false, status_value.codeValue(), self.reason());
    }
};
