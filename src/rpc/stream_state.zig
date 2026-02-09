const std = @import("std");

/// Tracks in-flight streaming calls, caches the first error, and provides
/// drain notification.  Used by generated `StreamClient` types to implement
/// Cap'n Proto `-> stream` flow control on the client side.
pub const StreamState = struct {
    in_flight: u32 = 0,
    stream_error: ?anyerror = null,
    on_drain: ?DrainCallback = null,
    on_drain_ctx: ?*anyopaque = null,

    pub const DrainCallback = *const fn (ctx: *anyopaque, err: ?anyerror) void;

    /// Record that a new streaming call has been sent.
    pub fn noteCallSent(self: *StreamState) void {
        self.in_flight += 1;
    }

    /// Called by the Return handler for each completed streaming call.
    /// If any call fails (exception), the error is cached and all subsequent
    /// calls on the same stream will fail immediately.
    pub fn handleReturn(self: *StreamState, is_exception: bool) void {
        if (is_exception and self.stream_error == null)
            self.stream_error = error.StreamingCallFailed;
        self.in_flight -= 1;
        if (self.in_flight == 0) {
            if (self.on_drain) |cb| {
                const ctx = self.on_drain_ctx;
                self.on_drain = null;
                self.on_drain_ctx = null;
                cb(ctx.?, self.stream_error);
            }
        }
    }

    /// Register a callback for when all in-flight calls complete.
    /// Fires immediately if nothing is in-flight.
    pub fn waitStreaming(self: *StreamState, ctx: *anyopaque, callback: DrainCallback) void {
        if (self.in_flight == 0) {
            callback(ctx, self.stream_error);
        } else {
            self.on_drain = callback;
            self.on_drain_ctx = ctx;
        }
    }

    /// Returns true if any streaming call has failed.
    pub fn hasFailed(self: *const StreamState) bool {
        return self.stream_error != null;
    }
};

test "StreamState: basic lifecycle" {
    var state = StreamState{};

    // No calls yet — not failed
    try std.testing.expect(!state.hasFailed());
    try std.testing.expectEqual(@as(u32, 0), state.in_flight);

    // Send two calls
    state.noteCallSent();
    state.noteCallSent();
    try std.testing.expectEqual(@as(u32, 2), state.in_flight);

    // First returns OK
    state.handleReturn(false);
    try std.testing.expectEqual(@as(u32, 1), state.in_flight);
    try std.testing.expect(!state.hasFailed());

    // Second returns OK
    state.handleReturn(false);
    try std.testing.expectEqual(@as(u32, 0), state.in_flight);
    try std.testing.expect(!state.hasFailed());
}

test "StreamState: error caching" {
    var state = StreamState{};

    state.noteCallSent();
    state.noteCallSent();

    // First call fails
    state.handleReturn(true);
    try std.testing.expect(state.hasFailed());
    try std.testing.expectEqual(error.StreamingCallFailed, state.stream_error.?);

    // Second call succeeds — error stays cached
    state.handleReturn(false);
    try std.testing.expect(state.hasFailed());
}

test "StreamState: drain callback fires when in-flight hits zero" {
    var state = StreamState{};

    const Ctx = struct {
        called: bool = false,
        err: ?anyerror = null,
    };
    var ctx = Ctx{};

    state.noteCallSent();
    state.waitStreaming(@ptrCast(&ctx), struct {
        fn cb(ptr: *anyopaque, err: ?anyerror) void {
            const c: *Ctx = @ptrCast(@alignCast(ptr));
            c.called = true;
            c.err = err;
        }
    }.cb);

    // Not called yet
    try std.testing.expect(!ctx.called);

    // Complete the call
    state.handleReturn(false);
    try std.testing.expect(ctx.called);
    try std.testing.expectEqual(@as(?anyerror, null), ctx.err);
}

test "StreamState: drain callback fires immediately if nothing in-flight" {
    var state = StreamState{};

    const Ctx = struct {
        called: bool = false,
        err: ?anyerror = null,
    };
    var ctx = Ctx{};

    state.waitStreaming(@ptrCast(&ctx), struct {
        fn cb(ptr: *anyopaque, err: ?anyerror) void {
            const c: *Ctx = @ptrCast(@alignCast(ptr));
            c.called = true;
            c.err = err;
        }
    }.cb);

    try std.testing.expect(ctx.called);
    try std.testing.expectEqual(@as(?anyerror, null), ctx.err);
}

test "StreamState: drain callback reports cached error" {
    var state = StreamState{};

    const Ctx = struct {
        called: bool = false,
        err: ?anyerror = null,
    };
    var ctx = Ctx{};

    state.noteCallSent();
    state.handleReturn(true); // fails

    state.waitStreaming(@ptrCast(&ctx), struct {
        fn cb(ptr: *anyopaque, err: ?anyerror) void {
            const c: *Ctx = @ptrCast(@alignCast(ptr));
            c.called = true;
            c.err = err;
        }
    }.cb);

    try std.testing.expect(ctx.called);
    try std.testing.expectEqual(error.StreamingCallFailed, ctx.err.?);
}
