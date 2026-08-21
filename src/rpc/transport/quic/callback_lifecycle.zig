pub fn State(comptime Connection: type) type {
    return struct {
        pub const MessageCallback = *const fn (conn: *Connection, frame: []const u8) anyerror!void;
        pub const ErrorCallback = *const fn (conn: *Connection, callback_err: anyerror) void;
        pub const CloseCallback = *const fn (conn: *Connection) void;
        pub const TickCallback = *const fn (conn: *Connection) void;
        pub const DeinitDecision = enum {
            already_deinitialized,
            defer_until_callback_exits,
            deinit_now,
        };

        depth: usize = 0,
        deinit_requested: bool = false,
        deinitialized: bool = false,
        ctx: ?*anyopaque = null,
        on_message: ?MessageCallback = null,
        on_error: ?ErrorCallback = null,
        on_close: ?CloseCallback = null,

        const Self = @This();

        pub fn start(
            self: *Self,
            ctx: *anyopaque,
            on_message: MessageCallback,
            on_error: ErrorCallback,
            on_close: CloseCallback,
        ) void {
            self.setCallbacks(ctx, on_message, on_error, on_close);
        }

        pub fn setCallbacks(
            self: *Self,
            ctx: ?*anyopaque,
            on_message: ?MessageCallback,
            on_error: ?ErrorCallback,
            on_close: ?CloseCallback,
        ) void {
            self.ctx = ctx;
            self.on_message = on_message;
            self.on_error = on_error;
            self.on_close = on_close;
        }

        pub fn clearCallbacks(self: *Self) void {
            self.setCallbacks(null, null, null, null);
        }

        pub fn context(self: *const Self) ?*anyopaque {
            return self.ctx;
        }

        pub fn callbacksReady(self: *const Self) bool {
            return self.on_message != null and self.on_error != null;
        }

        pub fn messageCallback(self: *const Self) ?MessageCallback {
            return self.on_message;
        }

        pub fn errorCallback(self: *const Self) ?ErrorCallback {
            return self.on_error;
        }

        pub fn closeCallback(self: *const Self) ?CloseCallback {
            return self.on_close;
        }

        pub fn clearMessageCallback(self: *Self) void {
            self.on_message = null;
        }

        pub fn takeErrorCallback(self: *Self) ?ErrorCallback {
            const cb = self.on_error;
            self.on_error = null;
            return cb;
        }

        pub fn decideDeinit(self: *Self) DeinitDecision {
            if (self.deinitialized) return .already_deinitialized;
            if (self.depth != 0) {
                self.deinit_requested = true;
                return .defer_until_callback_exits;
            }
            return .deinit_now;
        }

        pub fn beginDeinit(self: *Self) bool {
            if (self.deinitialized) return false;
            self.deinitialized = true;
            self.deinit_requested = false;
            return true;
        }

        pub fn deinitRequested(self: *const Self) bool {
            return self.deinit_requested;
        }

        pub fn shouldCompleteDeferredDeinit(self: *const Self) bool {
            return self.deinit_requested and self.depth == 0;
        }

        pub fn invokeError(
            self: *Self,
            conn: *Connection,
            cb: ErrorCallback,
            err: anyerror,
        ) void {
            self.depth += 1;
            defer self.depth -= 1;
            cb(conn, err);
        }

        pub fn invokeMessage(
            self: *Self,
            conn: *Connection,
            cb: MessageCallback,
            frame: []const u8,
        ) !void {
            self.depth += 1;
            defer self.depth -= 1;
            try cb(conn, frame);
        }

        pub fn invokeClose(
            self: *Self,
            conn: *Connection,
            cb: CloseCallback,
        ) void {
            self.depth += 1;
            defer self.depth -= 1;
            cb(conn);
        }

        /// Invoke the periodic tick callback (the peer's deadline sweep)
        /// under the same depth accounting as every other callback, so a
        /// `deinit()` from inside it is deferred rather than freeing state
        /// out from under the loop — the TCP transport's
        /// `invokeTickWakeCallback` contract.
        pub fn invokeTick(
            self: *Self,
            conn: *Connection,
            cb: TickCallback,
        ) void {
            self.depth += 1;
            defer self.depth -= 1;
            cb(conn);
        }
    };
}
