const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const Connection = capnpc.rpc.transport.tcp.Connection;
const tcp = capnpc.rpc.transport.tcp;

/// Portable connected pair: loopback TCP via std.Io, so these suites run on
/// every platform (POSIX socketpair does not exist on Windows).
fn createSocketPair(io: std.Io) ![2]tcp.SocketFd {
    return tcp.createLoopbackSocketPair(io);
}

fn closeFd(io: std.Io, socket: tcp.SocketFd) void {
    tcp.closeFd(io, socket);
}

// These regression tests guard the connection teardown seam. They exercise
// the exact ownership patterns that previously caused a use-after-free:
// destroying a heap-allocated Connection as part of its own lifecycle, and
// calling deinit() from a tick/wake callback while the run loop (and, on
// Windows, an in-flight concurrent read) is still live.

var destroy_count: usize = 0;

test "on_destroy frees the heap connection as run()'s last action (no use-after-free)" {
    destroy_count = 0;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    const Cbs = struct {
        fn onMessage(_: *Connection, _: []const u8) anyerror!void {}
        fn onError(_: *Connection, _: anyerror) void {}
        fn onClose(conn: *Connection) void {
            // Mirror the real owner pattern: request deinit from on_close.
            // This is deferred (we are inside a callback), and crucially it
            // must NOT free the connection's memory — run() still touches
            // `self` after on_close returns.
            conn.deinit();
        }
        fn onDestroy(conn: *Connection) void {
            // The only callback from which freeing the object is safe.
            destroy_count += 1;
            conn.allocator.destroy(conn);
        }
    };

    const conn = try allocator.create(Connection);
    conn.* = try Connection.init(allocator, io, fds[0], .{
        .tick_interval_ms = 10,
        .idle_timeout_ms = 40,
    });
    var dummy: u8 = 0;
    conn.start(&dummy, Cbs.onMessage, Cbs.onError, Cbs.onClose);
    conn.on_destroy = Cbs.onDestroy;

    // The peer end (fds[1]) stays silent: the loop exits via idle reaping,
    // then on_close (deferred deinit) and finally on_destroy (free) run. If
    // run() dereferenced `self` after on_destroy freed it, the testing
    // allocator or ReleaseSafe safety checks would trip here.
    conn.run();

    // If on_destroy had not run, the testing allocator would report the
    // Connection as leaked at test teardown; a double free would be caught
    // immediately. Exactly one free is expected.
    try std.testing.expectEqual(@as(usize, 1), destroy_count);
}

var tick_saw_deferred = false;

test "deinit() from on_tick is deferred until the run loop exits" {
    tick_saw_deferred = false;
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const fds = try createSocketPair(io);
    defer closeFd(io, fds[1]);

    const Cbs = struct {
        fn onMessage(_: *Connection, _: []const u8) anyerror!void {}
        fn onError(_: *Connection, _: anyerror) void {}
        fn onClose(_: *Connection) void {}
        fn onTick(conn: *Connection) void {
            conn.deinit();
            // Because we are inside a callback, deinit() is deferred: the
            // connection (and, on Windows, the read_buf of any in-flight
            // concurrent read) must not have been torn down yet.
            tick_saw_deferred = !conn.deinitialized;
        }
    };

    var conn = try Connection.init(allocator, io, fds[0], .{ .tick_interval_ms = 10 });
    var dummy: u8 = 0;
    conn.start(&dummy, Cbs.onMessage, Cbs.onError, Cbs.onClose);
    conn.on_tick = Cbs.onTick;

    // Silent peer: the first tick fires, its callback requests deinit, the
    // loop observes deinit_requested and exits, and the deferred deinit is
    // completed by run().
    conn.run();

    try std.testing.expect(tick_saw_deferred);
    // After run() returns the deferred deinit has completed exactly once;
    // no explicit conn.deinit() here (it would be a redundant second call,
    // which deinitNow guards against, but we assert the state directly).
    try std.testing.expect(conn.deinitialized);
}
