pub fn State(comptime Connection: type) type {
    return struct {
        pub const MessageCallback = *const fn (conn: *Connection, frame: []const u8) anyerror!void;
        pub const ErrorCallback = *const fn (conn: *Connection, callback_err: anyerror) void;
        pub const CloseCallback = *const fn (conn: *Connection) void;
        pub const DeinitDecision = enum {
            already_deinitialized,
            defer_until_callback_exits,
            deinit_now,
        };

        depth: usize = 0,
        deinit_requested: bool = false,
        deinitialized: bool = false,

        const Self = @This();

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
    };
}
