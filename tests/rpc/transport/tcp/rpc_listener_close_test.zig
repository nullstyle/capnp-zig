//! `Listener.close` under a parked accept (sprint 2026-10-15, item P1-1).
//!
//! `Listener.close` is Stable and says that it unblocks any thread blocked
//! in `accept()`. Each test here parks a thread in one accept entry point
//! (`Listener.accept`, `Listener.acceptFd`, `ServerSession.accept`), calls
//! `close` from the test thread, and expects the parked thread to return a
//! closed-listener error within 2 s.
//!
//! On Windows, `close` closes the AFD handle under the parked wait. std
//! 0.17.0's `netAcceptWindows` treats the resulting `STATUS_CANCELLED` as
//! `unreachable`, so this binary is expected to abort there until P1-1 is
//! fixed. It is its own test root so that an abort here cannot hide the
//! results of the other transport suites.
//!
//! Every wait is bounded. A thread that close does not wake is released by
//! a dial when the listener is still open; otherwise it is detached, and its
//! state is leaked on purpose, so that the test fails with a message and
//! never hangs the suite.

const std = @import("std");
const builtin = @import("builtin");
const capnpc = @import("capnpc-zig");

const tcp = capnpc.rpc.transport.tcp;
const testing = std.testing;

/// How long a parked accept may take to return after `Listener.close`.
const wake_bound_ms: i64 = 2000;
/// How long the acceptor thread gets to enter accept before `close`. A
/// close before that is a different path (`close_requested` is checked
/// before the accept), which this suite does not test.
const park_settle_ms: u64 = 300;
/// How long the acceptor thread may take to start at all.
const start_bound_ms: i64 = 5000;

const Entry = enum {
    listener_accept,
    listener_accept_fd,
    server_session_accept,
};

/// The error a parked accept returns when `Listener.close` wakes it on this
/// OS, or null where the only accepted result is `error.ListenerClosed`.
///
/// Linux: `close` shuts the socket down first, and accept(2) then fails
/// with EINVAL. macOS: the close of the fd wakes accept(2) with
/// ECONNABORTED. Windows: the P1-1 fix is to return `error.ListenerClosed`
/// for the cancelled AFD wait.
fn osWakeError() ?anyerror {
    return switch (builtin.target.os.tag) {
        .linux => error.SocketNotListening,
        .windows => null,
        else => error.ConnectionAborted,
    };
}

/// True when `err` is what a parked accept may return after `close`:
/// `error.ListenerClosed`, or this OS's wake error (`osWakeError`).
fn isClosedWake(err: anyerror) bool {
    if (err == error.ListenerClosed) return true;
    const os_err = osWakeError() orelse return false;
    return err == os_err;
}

/// The acceptor thread's state, on the page allocator. When `close` does not
/// wake the thread and nothing else can, the test detaches the thread and
/// leaks this, so the thread never writes into a freed stack frame.
const Acceptor = struct {
    listener: tcp.Listener,
    entry: Entry,
    started: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    result: ?anyerror = null,

    fn run(self: *Acceptor) void {
        self.started.store(true, .release);
        self.result = self.acceptOnce();
        self.done.store(true, .release);
    }

    /// Accept once through `entry`. Returns the accept's error, or
    /// `error.UnexpectedConnection` after it frees an accepted connection.
    fn acceptOnce(self: *Acceptor) anyerror {
        switch (self.entry) {
            .listener_accept => {
                const conn = self.listener.accept() catch |err| return err;
                conn.deinit();
                self.listener.allocator.destroy(conn);
            },
            .listener_accept_fd => {
                const fd = self.listener.acceptFd() catch |err| return err;
                tcp.closeFd(self.listener.io, fd);
            },
            .server_session_accept => {
                const session = tcp.ServerSession.accept(self.listener.allocator, &self.listener, .{}) catch |err| return err;
                session.deinit();
            },
        }
        return error.UnexpectedConnection;
    }
};

fn nowNs(io: std.Io) i64 {
    return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
}

fn msSince(io: std.Io, start_ns: i64) i64 {
    return @divTrunc(nowNs(io) - start_ns, std.time.ns_per_ms);
}

fn sleepMs(io: std.Io, ms: u64) void {
    const duration: std.Io.Clock.Duration = .{
        .raw = .{ .nanoseconds = @as(i96, @intCast(ms)) * std.time.ns_per_ms },
        .clock = .awake,
    };
    duration.sleep(io) catch {};
}

/// Wait until `flag` is set or `bound_ms` passes. Returns the flag.
fn waitFlag(io: std.Io, flag: *const std.atomic.Value(bool), bound_ms: i64) bool {
    const start = nowNs(io);
    while (!flag.load(.acquire)) {
        if (msSince(io, start) >= bound_ms) return false;
        sleepMs(io, 5);
    }
    return true;
}

/// Release a thread that is still parked in accept: dial the listener, so
/// the accept returns a connection. This works only while the listener is
/// still open. Returns true when the thread finished.
fn releaseParked(io: std.Io, state: *Acceptor) bool {
    const address = state.listener.getAddress();
    const client = std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream }) catch |err| {
        std.debug.print("could not dial the listener to release the parked thread: {s}\n", .{@errorName(err)});
        return false;
    };
    defer tcp.closeFd(io, .{ .handle = client.socket.handle });
    return waitFlag(io, &state.done, wake_bound_ms);
}

/// Park a thread in `entry`, call `Listener.close` from this thread, and
/// expect the parked thread to return a closed-listener error within
/// `wake_bound_ms`.
fn expectCloseWakesParkedAccept(entry: Entry) !void {
    const io = testing.io;
    const page = std.heap.page_allocator;

    const state = try page.create(Acceptor);
    var leak_state = false;
    defer if (!leak_state) page.destroy(state);

    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    state.* = .{
        .listener = try tcp.Listener.init(testing.allocator, io, address, .{}),
        .entry = entry,
    };
    // `close` is idempotent. A leaked state keeps its listener's handle
    // closed too: only a thread that close did not wake is leaked.
    defer if (!leak_state) state.listener.close();

    const thread = try std.Thread.spawn(.{}, Acceptor.run, .{state});

    if (!waitFlag(io, &state.started, start_bound_ms)) {
        // Not in accept yet: close makes it return at its first check.
        state.listener.close();
        thread.join();
        std.debug.print("{t}: the acceptor thread did not start within {d} ms\n", .{ entry, start_bound_ms });
        return error.AcceptorNotStarted;
    }
    sleepMs(io, park_settle_ms);
    if (state.done.load(.acquire)) {
        thread.join();
        std.debug.print("{t}: accept returned before close: {s}\n", .{ entry, @errorName(state.result orelse error.NoAcceptResult) });
        return error.AcceptReturnedEarly;
    }

    state.listener.close();
    if (!waitFlag(io, &state.done, wake_bound_ms)) {
        std.debug.print("{t}: the parked accept did not return within {d} ms of Listener.close\n", .{ entry, wake_bound_ms });
        if (releaseParked(io, state)) {
            thread.join();
        } else {
            std.debug.print("{t}: the parked thread is still blocked; detaching it and leaking its state\n", .{entry});
            thread.detach();
            leak_state = true;
        }
        return error.AcceptNotWoken;
    }
    thread.join();

    const result = state.result orelse return error.NoAcceptResult;
    if (!isClosedWake(result)) {
        std.debug.print("{t}: the parked accept returned {s} after Listener.close (expected ListenerClosed", .{ entry, @errorName(result) });
        if (osWakeError()) |os_err| std.debug.print(" or {s}", .{@errorName(os_err)});
        std.debug.print(")\n", .{});
        return error.UnexpectedAcceptResult;
    }
}

test "Listener.close wakes a thread parked in Listener.accept" {
    try expectCloseWakesParkedAccept(.listener_accept);
}

test "Listener.close wakes a thread parked in Listener.acceptFd" {
    try expectCloseWakesParkedAccept(.listener_accept_fd);
}

test "Listener.close wakes a thread parked in ServerSession.accept" {
    try expectCloseWakesParkedAccept(.server_session_accept);
}
