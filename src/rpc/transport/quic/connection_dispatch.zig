const std = @import("std");

const events = @import("../../events.zig");
const connection_termination = @import("connection_termination.zig");
const mode_router = @import("mode_router.zig");

/// Send and inbound dispatch policy for a QUIC RPC connection.
///
/// Engines handle QUIC stream mechanics. This helper owns the last hop between
/// those engines and `Connection`: outbound frame validation/queueing and
/// inbound callback invocation/error translation.
pub fn State(comptime Connection: type) type {
    return struct {
        const Termination = connection_termination.State(Connection);

        pub fn sendFrame(conn: *Connection, frame: []const u8) !void {
            if (Termination.isClosing(conn)) return error.BrokenPipe;
            if (frame.len == 0 or frame.len > conn.max_message_bytes) {
                events.emitResourceRejection(
                    conn.observer,
                    eventSource(conn.mode),
                    eventRole(conn.role),
                    .frame_bytes,
                    frame.len,
                    conn.max_message_bytes,
                    error.FrameTooLarge,
                );
                return error.FrameTooLarge;
            }
            if (frame.len > std.math.maxInt(u32)) {
                events.emitResourceRejection(
                    conn.observer,
                    eventSource(conn.mode),
                    eventRole(conn.role),
                    .frame_bytes,
                    frame.len,
                    std.math.maxInt(u32),
                    error.FrameTooLarge,
                );
                return error.FrameTooLarge;
            }

            mode_router.fromConnection(conn).enqueue(conn.allocator, frame) catch |err| {
                switch (err) {
                    error.OutboundQueueFull => events.emitBackpressure(
                        conn.observer,
                        eventSource(conn.mode),
                        eventRole(conn.role),
                        .outbound_queue_bytes,
                        frame.len,
                        null,
                        err,
                    ),
                    else => {},
                }
                return err;
            };
            events.emitFrame(conn.observer, eventSource(conn.mode), eventRole(conn.role), .enqueued, frame.len);
            conn.wake();
        }

        pub fn dispatchRpcFrame(conn: *Connection, frame: []const u8) !void {
            if (!conn.callback_lifecycle.callbacksReady()) return;
            events.emitFrame(conn.observer, eventSource(conn.mode), eventRole(conn.role), .received, frame.len);
            const on_message = conn.callback_lifecycle.messageCallback() orelse return;
            conn.callback_lifecycle.invokeMessage(conn, on_message, frame) catch |err| {
                Termination.callbackError(conn, err);
                return;
            };
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
