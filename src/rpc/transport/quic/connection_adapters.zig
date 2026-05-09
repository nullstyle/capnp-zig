const connection_loop = @import("connection_loop.zig");
const engine_owner_mod = @import("engine_owner.zig");
const endpoint_mod = @import("endpoint.zig");
const mode_router = @import("mode_router.zig");

/// Builds the owner views used by QUIC loop and stream engines.
///
/// `Connection` keeps the real behavior. This module owns the repetitive
/// `*anyopaque` projection layer so the main connection file can stay focused
/// on public API, callback lifecycle, and termination policy.
pub fn State(comptime Connection: type) type {
    return struct {
        const Self = @This();

        pub fn engineOwner(conn: *Connection) engine_owner_mod.Owner {
            return .{
                .ptr = conn,
                .allocator = conn.allocator,
                .role = conn.role,
                .max_message_bytes = conn.max_message_bytes,
                .stream_read_buf = conn.stream_read_buf,
                .is_closing = engineIsClosing,
                .callbacks_ready = engineCallbacksReady,
                .dispatch_rpc_frame = engineDispatchRpcFrame,
                .terminate_frame_error = engineTerminateFrameError,
                .deinit_requested = engineDeinitRequested,
            };
        }

        pub fn loopOwner(conn: *Connection) connection_loop.Owner {
            return .{
                .ptr = conn,
                .io = conn.endpoint.io,
                .role = conn.role,
                .udp_rx_buf = conn.udp_rx_buf,
                .udp_tx_buf = conn.udp_tx_buf,
                .wake = &conn.wake_state,
                .driver = loopDriver,
                .selected_mode = loopSelectedMode,
                .selected_outbound_empty = loopSelectedOutboundEmpty,
                .engine_owner = loopEngineOwner,
                .close_requested = loopCloseRequested,
                .request_close = loopRequestClose,
                .terminate_internal_error = loopTerminateInternalError,
                .flush_close_datagram = loopFlushCloseDatagram,
                .close_engines = loopCloseEngines,
                .invoke_close_callback = loopInvokeCloseCallback,
                .complete_deferred_deinit = loopCompleteDeferredDeinit,
            };
        }

        fn castConnection(ptr: *anyopaque) *Connection {
            return @ptrCast(@alignCast(ptr));
        }

        fn endpointDriver(conn: *Connection) endpoint_mod.EndpointDriver {
            return conn.endpoint.driver();
        }

        fn engineIsClosing(ptr: *anyopaque) bool {
            const conn = castConnection(ptr);
            return conn.close_controller.isRequested();
        }

        fn engineCallbacksReady(ptr: *anyopaque) bool {
            const conn = castConnection(ptr);
            return conn.callback_lifecycle.callbacksReady();
        }

        fn engineDispatchRpcFrame(ptr: *anyopaque, frame: []const u8) !void {
            const conn = castConnection(ptr);
            try Connection.AdapterAccess.dispatchRpcFrame(conn, frame);
        }

        fn engineTerminateFrameError(ptr: *anyopaque, err: anyerror) void {
            const conn = castConnection(ptr);
            Connection.AdapterAccess.terminateFrameError(conn, err);
        }

        fn engineDeinitRequested(ptr: *anyopaque) bool {
            const conn = castConnection(ptr);
            return conn.callback_lifecycle.deinitRequested();
        }

        fn loopDriver(ptr: *anyopaque) endpoint_mod.EndpointDriver {
            const conn = castConnection(ptr);
            return endpointDriver(conn);
        }

        fn loopSelectedMode(ptr: *anyopaque) mode_router.Router {
            const conn = castConnection(ptr);
            return mode_router.fromConnection(conn);
        }

        fn loopSelectedOutboundEmpty(ptr: *anyopaque) bool {
            const conn = castConnection(ptr);
            return mode_router.fromConnection(conn).outboundEmpty();
        }

        fn loopEngineOwner(ptr: *anyopaque) engine_owner_mod.Owner {
            const conn = castConnection(ptr);
            return Self.engineOwner(conn);
        }

        fn loopCloseRequested(ptr: *anyopaque) bool {
            const conn = castConnection(ptr);
            return conn.close_controller.isRequested();
        }

        fn loopRequestClose(ptr: *anyopaque) void {
            const conn = castConnection(ptr);
            conn.close_controller.request();
        }

        fn loopTerminateInternalError(ptr: *anyopaque, err: anyerror) void {
            const conn = castConnection(ptr);
            Connection.AdapterAccess.terminateInternalError(conn, err);
        }

        fn loopFlushCloseDatagram(ptr: *anyopaque) void {
            const conn = castConnection(ptr);
            conn.close_controller.flushCloseDatagram(
                conn.endpoint.activeQuicConnection(),
                endpointDriver(conn),
                conn.udp_tx_buf,
                conn.endpoint.nowUs(),
            );
        }

        fn loopCloseEngines(ptr: *anyopaque) void {
            const conn = castConnection(ptr);
            conn.close_controller.closeEngines(&conn.baseline, &conn.native);
        }

        fn loopInvokeCloseCallback(ptr: *anyopaque) void {
            const conn = castConnection(ptr);
            if (conn.callback_lifecycle.closeCallback()) |cb| {
                conn.callback_lifecycle.invokeClose(conn, cb);
            }
        }

        fn loopCompleteDeferredDeinit(ptr: *anyopaque) void {
            const conn = castConnection(ptr);
            if (conn.callback_lifecycle.shouldCompleteDeferredDeinit()) {
                Connection.AdapterAccess.deinitNowUnchecked(conn);
            }
        }
    };
}
