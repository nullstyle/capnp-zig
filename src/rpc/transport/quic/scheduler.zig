const std = @import("std");

pub const fallback_no_wake_wait = std.Io.Duration.fromMilliseconds(5);

pub const StepMode = enum {
    /// Wait for inbound UDP, a wake signal, or the next QUIC timer deadline.
    wait,
    /// Service only work that is ready now.
    poll,
};

pub const StepResult = struct {
    received_datagram: bool = false,
    wake_drained: bool = false,
    waited_for: std.Io.Duration = std.Io.Duration.zero,
    next_deadline_us: ?u64 = null,
    closed: bool = false,
};

pub const ReceiveWaitInput = struct {
    mode: StepMode,
    receive_timeout: std.Io.Duration,
    now_us: u64,
    next_deadline_us: ?u64 = null,
    immediate_work: bool = false,
    wake_supported: bool = false,
};

pub fn receiveWaitDuration(input: ReceiveWaitInput) std.Io.Duration {
    if (input.mode == .poll or input.immediate_work) return std.Io.Duration.zero;

    var wait = input.receive_timeout;
    if (input.next_deadline_us) |deadline_us| {
        wait = minDuration(wait, durationUntil(input.now_us, deadline_us));
    }

    if (!input.wake_supported) {
        wait = minDuration(wait, fallback_no_wake_wait);
    }
    return wait;
}

pub fn durationUntil(now_us: u64, deadline_us: u64) std.Io.Duration {
    if (deadline_us <= now_us) return std.Io.Duration.zero;
    const delta = deadline_us - now_us;
    const bounded = @min(delta, @as(u64, @intCast(std.math.maxInt(i64))));
    return std.Io.Duration.fromMicroseconds(@intCast(bounded));
}

pub fn minDuration(a: std.Io.Duration, b: std.Io.Duration) std.Io.Duration {
    return if (a.toMicroseconds() <= b.toMicroseconds()) a else b;
}

pub fn durationToPollTimeoutMs(duration: std.Io.Duration) i32 {
    const us = duration.toMicroseconds();
    if (us <= 0) return 0;
    const ms = duration.toMilliseconds();
    if (ms <= 0) return 1;
    return @intCast(@min(ms, std.math.maxInt(i32)));
}

test "QUIC scheduler uses zero wait for poll mode and immediate work" {
    const long = std.Io.Duration.fromMilliseconds(500);

    try std.testing.expectEqual(@as(i64, 0), receiveWaitDuration(.{
        .mode = .poll,
        .receive_timeout = long,
        .now_us = 0,
        .wake_supported = true,
    }).toMicroseconds());

    try std.testing.expectEqual(@as(i64, 0), receiveWaitDuration(.{
        .mode = .wait,
        .receive_timeout = long,
        .now_us = 0,
        .immediate_work = true,
        .wake_supported = true,
    }).toMicroseconds());
}

test "QUIC scheduler bounds waits by timer deadline" {
    const wait = receiveWaitDuration(.{
        .mode = .wait,
        .receive_timeout = std.Io.Duration.fromMilliseconds(500),
        .now_us = 1_000,
        .next_deadline_us = 3_500,
        .wake_supported = true,
    });

    try std.testing.expectEqual(@as(i64, 2_500), wait.toMicroseconds());
}

test "QUIC scheduler caps no-wake blocking waits" {
    const wait = receiveWaitDuration(.{
        .mode = .wait,
        .receive_timeout = std.Io.Duration.fromMilliseconds(500),
        .now_us = 0,
        .wake_supported = false,
    });

    try std.testing.expectEqual(fallback_no_wake_wait.toMicroseconds(), wait.toMicroseconds());
}

test "QUIC scheduler poll timeout rounds sub-millisecond waits up" {
    try std.testing.expectEqual(@as(i32, 0), durationToPollTimeoutMs(std.Io.Duration.zero));
    try std.testing.expectEqual(@as(i32, 1), durationToPollTimeoutMs(std.Io.Duration.fromMicroseconds(1)));
    try std.testing.expectEqual(@as(i32, 7), durationToPollTimeoutMs(std.Io.Duration.fromMilliseconds(7)));
}
