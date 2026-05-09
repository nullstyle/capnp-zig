const std = @import("std");

const events = @import("../../events.zig");
const quic_close = @import("close.zig");
const termination = @import("termination.zig");

const log = std.log.scoped(.rpc_quic_transport);

/// Centralizes terminal close policy for a QUIC RPC connection.
///
/// `Connection` owns the mutable state and public API; this helper owns how
/// frame, callback, and internal errors map onto close codes, inbound teardown,
/// callback notification, and scheduler wakeups.
pub fn State(comptime Connection: type) type {
    return struct {
        const TerminationPolicy = termination.Policy;

        pub fn close(conn: *Connection) void {
            enterClosing(conn);
            conn.close_controller.closeActive(
                conn.activeQuicConnection(),
                .normal,
                null,
            );
            conn.wake();
        }

        pub fn requestClose(conn: *Connection) void {
            events.emitConnection(conn.observer, eventSource(conn.mode), eventRole(conn.role), .closing);
            conn.close_controller.requestNormal(&conn.baseline, &conn.native);
            conn.wake();
        }

        pub fn isClosing(conn: *const Connection) bool {
            return conn.close_controller.isRequested();
        }

        pub fn status(conn: *const Connection) ?quic_close.Status {
            return conn.close_controller.status();
        }

        pub fn frameError(conn: *Connection, err: anyerror) void {
            log.debug("fatal QUIC frame error, closing connection: {}", .{err});
            apply(conn, termination.policy(.frame_error, err), err);
        }

        pub fn callbackError(conn: *Connection, err: anyerror) void {
            log.debug("QUIC message callback failed, closing connection: {}", .{err});
            apply(conn, termination.policy(.callback_error, err), err);
        }

        pub fn internalError(conn: *Connection, err: anyerror) void {
            apply(conn, termination.policy(.internal_error, err), err);
        }

        pub fn enterClosing(conn: *Connection) void {
            conn.close_controller.enterClosing(&conn.baseline, &conn.native);
        }

        pub fn reason(conn: *const Connection) []const u8 {
            return conn.close_controller.reason();
        }

        pub fn revealDetailOnWire(conn: *Connection, reveal: bool) void {
            conn.close_controller.setRevealDetailOnWire(reveal);
        }

        fn apply(
            conn: *Connection,
            policy: TerminationPolicy,
            err: anyerror,
        ) void {
            conn.close_controller.closeActive(
                conn.activeQuicConnection(),
                policy.code,
                err,
            );
            events.emitProtocolError(conn.observer, eventSource(conn.mode), eventRole(conn.role), err, null);
            enterClosing(conn);
            if (policy.reset_inbound) resetInbound(conn);
            if (policy.clear_message_callback) {
                conn.callback_lifecycle.clearMessageCallback();
            }
            const on_error = if (policy.clear_error_callback)
                conn.callback_lifecycle.takeErrorCallback()
            else
                conn.callback_lifecycle.errorCallback();
            conn.wake();
            if (policy.notify_error_callback) {
                if (on_error) |cb| conn.callback_lifecycle.invokeError(conn, cb, err);
            }
        }

        fn resetInbound(conn: *Connection) void {
            conn.baseline.resetInbound();
            conn.native.resetInbound(conn.allocator);
        }

        fn eventSource(mode: anytype) events.Source {
            return switch (mode) {
                .baseline => .quic_baseline,
                .native => .quic_native,
            };
        }

        fn eventRole(role: anytype) events.Role {
            return switch (role) {
                .client => .client,
                .server => .server,
            };
        }
    };
}
