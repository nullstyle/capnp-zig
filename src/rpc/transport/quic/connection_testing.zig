const std = @import("std");

const length_framer = @import("length_framer.zig");
const native_framer = @import("native_framer.zig");
const quic_close = @import("close.zig");
const quic_options = @import("options.zig");

/// Test-only accessors for QUIC connection internals.
///
/// The public transport module exposes this only from `quic.testing` when
/// `builtin.is_test` is true. Production connection code keeps only a narrow
/// `TestingHooks` bridge for private owner and callback lifecycle operations.
pub fn Access(comptime Connection: type) type {
    return struct {
        const Hooks = Connection.TestingHooks;

        pub const ApplicationCloseCode = quic_close.ApplicationCloseCode;
        pub const CloseCallback = Hooks.CloseCallback;
        pub const ErrorCallback = Hooks.ErrorCallback;
        pub const MessageCallback = Hooks.MessageCallback;
        pub const NativeOptions = quic_options.NativeOptions;

        pub fn wakeRequested(conn: *const Connection) bool {
            return Hooks.wakeRequested(conn);
        }

        pub fn consumeWakeRequested(conn: *Connection) bool {
            return conn.wake_state.consumeRequested();
        }

        pub fn processNativeControlFrames(conn: *Connection) !void {
            try processNativeControlFramesAt(conn, 0);
        }

        pub fn processNativeControlFramesAt(conn: *Connection, now_us: u64) !void {
            const active = Hooks.activeQuicConn(conn) orelse return error.ConnectionNotReady;
            try conn.native.processControlFrames(Hooks.nativeOwner(conn), active, now_us);
        }

        pub fn startNativePendingData(
            conn: *Connection,
            data: native_framer.DataRpc,
        ) !void {
            try startNativePendingDataAt(conn, data, 0);
        }

        pub fn startNativePendingDataAt(
            conn: *Connection,
            data: native_framer.DataRpc,
            now_us: u64,
        ) !void {
            try conn.native.startPendingData(Hooks.nativeOwner(conn), data, now_us);
        }

        pub fn resetNativeInbound(
            conn: *Connection,
            allocator: std.mem.Allocator,
            max_control_frame_bytes: usize,
            max_rpc_frame_bytes: usize,
        ) void {
            conn.native.inbound.deinit();
            conn.native.inbound = native_framer.ControlFramer.init(allocator, .{
                .max_control_frame_bytes = max_control_frame_bytes,
                .max_rpc_frame_bytes = max_rpc_frame_bytes,
            });
        }

        pub fn dispatchBaselineFrames(conn: *Connection) !void {
            try conn.baseline.dispatchAvailableFrames(Hooks.baselineOwner(conn));
        }

        pub fn resetBaselineInbound(
            conn: *Connection,
            allocator: std.mem.Allocator,
            max_message_bytes: usize,
        ) void {
            conn.baseline.inbound.deinit();
            conn.baseline.inbound = length_framer.LengthDelimitedFramer.init(
                allocator,
                max_message_bytes,
            );
        }

        pub fn terminateFrameError(conn: *Connection, err: anyerror) void {
            Hooks.terminateFrameError(conn, err);
        }

        pub fn closeReason(conn: *const Connection) []const u8 {
            return Hooks.closeReason(conn);
        }

        pub fn revealCloseDetailOnWire(conn: *Connection, reveal: bool) void {
            Hooks.revealCloseDetailOnWire(conn, reveal);
        }

        pub fn setCallbacks(
            conn: *Connection,
            ctx: ?*anyopaque,
            on_message: ?MessageCallback,
            on_error: ?ErrorCallback,
            on_close: ?CloseCallback,
        ) void {
            Hooks.setCallbacks(conn, ctx, on_message, on_error, on_close);
        }

        pub fn closeCallback(conn: *const Connection) ?CloseCallback {
            return Hooks.closeCallback(conn);
        }

        pub fn deinitRequested(conn: *const Connection) bool {
            return Hooks.deinitRequested(conn);
        }

        pub fn invokeCloseCallback(
            conn: *Connection,
            cb: *const fn (conn: *Connection) void,
        ) void {
            Hooks.invokeCloseCallback(conn, cb);
        }
    };
}
