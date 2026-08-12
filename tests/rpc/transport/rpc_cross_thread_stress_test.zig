const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const Connection = tcp.Connection;
const quic_wake = capnpc.rpc.transport.quic.wake;

// Cross-thread interleaving hammers for the two sanctioned any-thread doors
// (`wake()` / `request()`) racing owner-thread teardown. These are the
// interleavings the wake mutexes exist for: pre-fix, the QUIC handle hammer
// reads `fds` just before `deinit` closes them and writes to a closed —
// possibly kernel-recycled — descriptor. The suites are deterministic in
// shape (bounded reps, joined threads) but probabilistic in interleaving;
// under the TSan lane a reintroduced race is reported on the first overlap.

const quic_handle_reps = 200;
const quic_waker_threads = 4;

test "quic wake Handle: cross-thread request() storm vs owner deinit()" {
    const Waker = struct {
        fn run(handle: *quic_wake.Handle, stop: *std.atomic.Value(bool)) void {
            while (!stop.load(.acquire)) {
                handle.request();
                std.atomic.spinLoopHint();
            }
        }
    };

    var rep: usize = 0;
    while (rep < quic_handle_reps) : (rep += 1) {
        var handle = quic_wake.Handle.init();
        var stop = std.atomic.Value(bool).init(false);

        var threads: [quic_waker_threads]std.Thread = undefined;
        var spawned: usize = 0;
        for (&threads) |*t| {
            t.* = std.Thread.spawn(.{}, Waker.run, .{ &handle, &stop }) catch break;
            spawned += 1;
        }

        // Keep the fds write path hot: the owner drains the requested flag so
        // wakers re-enter the descriptor write instead of short-circuiting on
        // the dedupe swap, then tears down mid-storm.
        var spin: usize = 0;
        while (spin < 32) : (spin += 1) {
            _ = handle.consumeRequested();
            std.Thread.yield() catch {};
        }
        handle.deinit();

        // Post-deinit requests must be safe no-ops (fds cleared under the
        // handle mutex); let the storm run a moment against the dead handle.
        std.Thread.yield() catch {};
        stop.store(true, .release);
        for (threads[0..spawned]) |t| t.join();

        // After deinit the handle reports unsupported and requests stay inert.
        try std.testing.expect(!handle.isSupported());
    }
}

test "tcp Connection: adopt-before-run lets run-thread callbacks call checked methods" {
    // A bare Connection constructed on this thread but run on another must
    // adopt affinity before run() (as the session wrappers do); then an
    // affinity-checked method called from a callback on the run thread (here
    // isClosing, from on_tick) does not panic. Without the adopt it would,
    // because the owner is still the constructing thread.
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try tcp.createLoopbackSocketPair(io);
    defer tcp.closeFd(io, fds[1]);

    checked_from_run_ok.store(false, .release);

    const Cbs = struct {
        fn onMessage(_: *Connection, _: []const u8) anyerror!void {}
        fn onError(_: *Connection, _: anyerror) void {}
        fn onClose(_: *Connection) void {}
        fn onTick(conn: *Connection) void {
            // isClosing() asserts thread affinity; reaching here without a
            // panic proves this thread owns the connection.
            _ = conn.isClosing();
            checked_from_run_ok.store(true, .release);
            conn.requestClose();
        }
        fn run(conn: *Connection) void {
            // Adopt before entering the loop, the sanctioned handoff.
            conn.adoptOwnerThread();
            conn.run();
        }
    };

    var conn = try Connection.init(allocator, io, fds[0], .{ .tick_interval_ms = 5 });
    var dummy: u8 = 0;
    conn.start(&dummy, Cbs.onMessage, Cbs.onError, Cbs.onClose);
    conn.on_tick = Cbs.onTick;

    // Run on a DIFFERENT thread; the first tick fires the checked callback.
    const t = try std.Thread.spawn(.{}, Cbs.run, .{&conn});
    t.join();
    // The run thread owns the connection now; re-adopt here (quiescent after
    // join) for the owner-thread-only deinit.
    conn.adoptOwnerThread();
    conn.deinit();

    try std.testing.expect(checked_from_run_ok.load(.acquire));
}

var checked_from_run_ok = std.atomic.Value(bool).init(false);

const tcp_wake_reps = 30;
const tcp_waker_threads = 3;
const tcp_wakes_per_thread = 200;

fn onWakeNoop(_: *Connection) void {}

test "tcp Connection: cross-thread wake() storm vs owner deinit()" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const Waker = struct {
        fn run(conn: *Connection) void {
            var i: usize = 0;
            while (i < tcp_wakes_per_thread) : (i += 1) {
                conn.wake();
            }
        }
    };

    var rep: usize = 0;
    while (rep < tcp_wake_reps) : (rep += 1) {
        const fds = try tcp.createLoopbackSocketPair(io);
        defer tcp.closeFd(io, fds[1]);

        var conn = try Connection.init(allocator, io, fds[0], .{});
        try conn.enableWake(onWakeNoop);

        var threads: [tcp_waker_threads]std.Thread = undefined;
        var spawned: usize = 0;
        for (&threads) |*t| {
            t.* = std.Thread.spawn(.{}, Waker.run, .{&conn}) catch break;
            spawned += 1;
        }

        // Tear down on the owner thread while wakers are mid-storm. wake()
        // after deinit must observe the cleared wake_fds under wake_mu and
        // return without touching a recycled descriptor.
        std.Thread.yield() catch {};
        conn.deinit();

        for (threads[0..spawned]) |t| t.join();
    }
}
