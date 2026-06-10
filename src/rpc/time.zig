//! Injectable monotonic time source for RPC deadlines and idle tracking.
//!
//! The RPC runtime never reads the system clock directly. Components that
//! need time (call deadlines, idle reaping, shutdown drain bounds) read a
//! `Clock` that the embedder injects. Transport glue injects an
//! `std.Io`-backed clock automatically when a peer is attached to a TCP
//! connection; tests inject a `TestClock` and advance it deterministically.
//! Freestanding targets (WASM) have no default clock, so deadline features
//! stay inert there unless the host provides one.

const std = @import("std");

/// Monotonic clock handle. `now()` returns nanoseconds from an arbitrary
/// fixed origin; values are only compared and subtracted, never interpreted
/// as wall-clock time.
pub const Clock = struct {
    ctx: *anyopaque,
    now_fn: *const fn (ctx: *anyopaque) i64,

    pub fn now(self: Clock) i64 {
        return self.now_fn(self.ctx);
    }

    /// A clock backed by `std.Io`'s monotonic (`awake`) clock.
    ///
    /// `io` must point at storage that outlives the returned clock (for
    /// example a `Connection.io` field, not a stack temporary).
    pub fn fromIo(io: *const std.Io) Clock {
        return .{
            .ctx = @ptrCast(@constCast(io)),
            .now_fn = struct {
                fn call(ctx: *anyopaque) i64 {
                    const io_ptr: *const std.Io = @ptrCast(@alignCast(ctx));
                    const ts = std.Io.Clock.awake.now(io_ptr.*);
                    // Monotonic nanoseconds since an arbitrary origin (boot);
                    // i64 holds ~292 years of nanoseconds.
                    return @intCast(ts.nanoseconds);
                }
            }.call,
        };
    }
};

/// Deterministic clock for tests: starts at zero and only moves when
/// explicitly advanced.
pub const TestClock = struct {
    now_ns: i64 = 0,

    pub fn clock(self: *TestClock) Clock {
        return .{
            .ctx = self,
            .now_fn = struct {
                fn call(ctx: *anyopaque) i64 {
                    const tc: *TestClock = @ptrCast(@alignCast(ctx));
                    return tc.now_ns;
                }
            }.call,
        };
    }

    pub fn advanceNs(self: *TestClock, ns: i64) void {
        self.now_ns += ns;
    }

    pub fn advanceMs(self: *TestClock, ms: i64) void {
        self.now_ns += ms * std.time.ns_per_ms;
    }
};

test "TestClock advances deterministically" {
    var tc = TestClock{};
    const c = tc.clock();
    try std.testing.expectEqual(@as(i64, 0), c.now());
    tc.advanceMs(5);
    try std.testing.expectEqual(@as(i64, 5 * std.time.ns_per_ms), c.now());
    tc.advanceNs(1);
    try std.testing.expectEqual(@as(i64, 5 * std.time.ns_per_ms + 1), c.now());
}

test "Clock.fromIo reads the io monotonic clock" {
    const io = std.testing.io;
    const c = Clock.fromIo(&io);
    const a = c.now();
    const b = c.now();
    try std.testing.expect(b >= a);
}
