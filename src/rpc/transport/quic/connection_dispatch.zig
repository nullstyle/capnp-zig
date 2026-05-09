const std = @import("std");

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
                return error.FrameTooLarge;
            }
            if (frame.len > std.math.maxInt(u32)) return error.FrameTooLarge;

            try selectedMode(conn).enqueue(conn.allocator, frame);
            conn.wake();
        }

        pub fn dispatchRpcFrame(conn: *Connection, frame: []const u8) !void {
            if (!conn.callback_lifecycle.callbacksReady()) return;
            const on_message = conn.callback_lifecycle.messageCallback() orelse return;
            conn.callback_lifecycle.invokeMessage(conn, on_message, frame) catch |err| {
                Termination.callbackError(conn, err);
                return;
            };
        }

        fn selectedMode(conn: *Connection) mode_router.Router {
            return .{
                .mode = conn.mode,
                .baseline = &conn.baseline,
                .native = &conn.native,
            };
        }
    };
}
