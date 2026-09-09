const std = @import("std");

/// Tracks in-flight streaming calls, caches the first error, and provides
/// drain notification.  Used by generated `StreamClient` types to implement
/// Cap'n Proto `-> stream` flow control on the client side.
pub const StreamState = struct {
    in_flight: u32 = 0,
    /// A zero value means "unlimited".
    max_in_flight: u32 = 0,
    /// Encoded Cap'n Proto frame bytes, including segment framing.
    in_flight_bytes: usize = 0,
    /// A zero value means unlimited. A call larger than a nonzero window is
    /// rejected even when the stream is idle; it is never admitted alone.
    max_in_flight_bytes: usize = 0,
    /// Size of the most recent byte-admission rejection, for readiness retry.
    last_rejected_bytes: usize = 0,
    stream_error: ?anyerror = null,
    on_drain: ?DrainCallback = null,
    on_drain_ctx: ?*anyopaque = null,

    on_ready: ?DrainCallback = null,
    on_ready_ctx: ?*anyopaque = null,
    ready_bytes: usize = 0,

    /// One reservation per generated context. Settle before destroying its
    /// owner or invoking callbacks; exact frame bytes are reserved before send.
    pub const Reservation = struct {
        stream: *StreamState,
        bytes: usize = 0,
        active: bool = true,

        pub fn reserveBytes(self: *Reservation, encoded_bytes: usize) !void {
            std.debug.assert(self.active and self.bytes == 0);
            errdefer self.stream.last_rejected_bytes = encoded_bytes;
            if (self.stream.stream_error) |err| return err;
            const limit = self.stream.max_in_flight_bytes;
            if (limit != 0 and encoded_bytes > limit) return error.StreamCallTooLarge;
            const total = std.math.add(usize, self.stream.in_flight_bytes, encoded_bytes) catch return error.StreamByteLimitExceeded;
            if (limit != 0 and total > limit) return error.StreamByteLimitExceeded;
            self.bytes = encoded_bytes;
            self.stream.in_flight_bytes = total;
        }

        pub fn settle(self: *Reservation, err: ?anyerror) void {
            if (!self.active) return;
            self.active = false;
            const stream = self.stream;
            std.debug.assert(stream.in_flight != 0);
            std.debug.assert(stream.in_flight_bytes >= self.bytes);
            stream.in_flight -= 1;
            stream.in_flight_bytes -= self.bytes;
            if (err != null and stream.stream_error == null) stream.stream_error = err;
            stream.notify();
        }
    };

    pub const DrainCallback = *const fn (ctx: *anyopaque, err: ?anyerror) void;

    /// Record that a new streaming call has been sent.
    pub fn noteCallSent(self: *StreamState) !void {
        if (self.max_in_flight != 0 and self.in_flight >= self.max_in_flight) {
            return error.StreamInFlightLimitExceeded;
        }
        self.in_flight = std.math.add(u32, self.in_flight, 1) catch return error.StreamInFlightLimitExceeded;
    }

    /// Called by the Return handler for each completed streaming call.
    /// If any call fails (exception), the error is cached and all subsequent
    /// calls on the same stream will fail immediately.
    pub fn handleReturn(self: *StreamState, is_exception: bool) void {
        if (is_exception and self.stream_error == null)
            self.stream_error = error.StreamingCallFailed;
        self.in_flight = self.in_flight -| 1;
        self.notify();
    }

    fn notify(self: *StreamState) void {
        const ready = if (self.on_ready != null and (self.stream_error != null or self.canSend(self.ready_bytes))) self.on_ready else null;
        const ready_ctx = self.on_ready_ctx;
        const drained = if (self.in_flight == 0) self.on_drain else null;
        const drain_ctx = self.on_drain_ctx;
        const err = self.stream_error;
        if (ready != null) {
            self.on_ready = null;
            self.on_ready_ctx = null;
        }
        if (drained != null) {
            self.on_drain = null;
            self.on_drain_ctx = null;
        }
        // Clear and snapshot before callbacks: callbacks may send another call,
        // register the next waiter, or tear down the owning peer.
        if (ready) |callback| callback(ready_ctx.?, err);
        if (drained) |callback| callback(drain_ctx.?, err);
    }

    pub fn canSend(self: *const StreamState, encoded_bytes: usize) bool {
        if (self.hasFailed()) return false;
        if (self.max_in_flight != 0 and self.in_flight >= self.max_in_flight) return false;
        if (self.in_flight == std.math.maxInt(u32)) return false;
        const total = std.math.add(usize, self.in_flight_bytes, encoded_bytes) catch return false;
        return self.max_in_flight_bytes == 0 or total <= self.max_in_flight_bytes;
    }

    /// One-shot readiness notification for a call of the indicated encoded
    /// size. Readiness is an opportunity to retry; no capacity is reserved by
    /// the callback registration. Only one readiness waiter may be pending.
    pub fn whenReady(self: *StreamState, encoded_bytes: usize, ctx: *anyopaque, callback: DrainCallback) void {
        if (self.stream_error) |err| return callback(ctx, err);
        if (self.max_in_flight_bytes != 0 and encoded_bytes > self.max_in_flight_bytes) return callback(ctx, error.StreamCallTooLarge);
        if (self.canSend(encoded_bytes)) return callback(ctx, null);
        if (self.on_ready != null) return callback(ctx, error.StreamReadyAlreadyPending);
        self.on_ready = callback;
        self.on_ready_ctx = ctx;
        self.ready_bytes = encoded_bytes;
    }

    /// Register a callback for when all in-flight calls complete.
    /// Fires immediately if nothing is in-flight.
    pub fn waitStreaming(self: *StreamState, ctx: *anyopaque, callback: DrainCallback) void {
        if (self.in_flight == 0) {
            callback(ctx, self.stream_error);
        } else {
            if (self.on_drain != null) {
                callback(ctx, error.StreamDrainAlreadyPending);
                return;
            }
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
    try state.noteCallSent();
    try state.noteCallSent();
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

    try state.noteCallSent();
    try state.noteCallSent();

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

    try state.noteCallSent();
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

    try state.noteCallSent();
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

test "StreamState: second waiter gets explicit error without replacing first waiter" {
    var state = StreamState{};

    const Ctx = struct {
        called: bool = false,
        err: ?anyerror = null,
    };
    var first = Ctx{};
    var second = Ctx{};

    try state.noteCallSent();
    state.waitStreaming(@ptrCast(&first), struct {
        fn cb(ptr: *anyopaque, err: ?anyerror) void {
            const c: *Ctx = @ptrCast(@alignCast(ptr));
            c.called = true;
            c.err = err;
        }
    }.cb);
    state.waitStreaming(@ptrCast(&second), struct {
        fn cb(ptr: *anyopaque, err: ?anyerror) void {
            const c: *Ctx = @ptrCast(@alignCast(ptr));
            c.called = true;
            c.err = err;
        }
    }.cb);

    try std.testing.expect(second.called);
    try std.testing.expectEqual(error.StreamDrainAlreadyPending, second.err.?);
    try std.testing.expect(!first.called);

    state.handleReturn(false);
    try std.testing.expect(first.called);
    try std.testing.expectEqual(@as(?anyerror, null), first.err);
}

test "StreamState: max in-flight limit is enforced" {
    var state = StreamState{ .max_in_flight = 2 };

    try state.noteCallSent();
    try state.noteCallSent();
    try std.testing.expectEqual(@as(u32, 2), state.in_flight);

    try std.testing.expectError(error.StreamInFlightLimitExceeded, state.noteCallSent());
    try std.testing.expectEqual(@as(u32, 2), state.in_flight);

    state.handleReturn(false);
    try state.noteCallSent();
    try std.testing.expectEqual(@as(u32, 2), state.in_flight);
}

test "StreamState: in-flight overflow returns a typed error" {
    var state = StreamState{ .in_flight = std.math.maxInt(u32) };

    try std.testing.expectError(error.StreamInFlightLimitExceeded, state.noteCallSent());
    try std.testing.expectEqual(std.math.maxInt(u32), state.in_flight);
}
