//! `Listener.close` under a parked accept (sprint 2026-10-15, item P1-1).
//!
//! `Listener.close` is Stable and says that a thread blocked in `accept()`
//! returns. Each test in the first part parks a thread in one accept entry
//! point (`Listener.accept`, `Listener.acceptFd`, `ServerSession.accept`),
//! calls `close` from another thread, and expects the parked thread to
//! return a closed-listener error within 2 s, and `close` to return.
//!
//! On Windows, `close` used to close the AFD handle under the parked wait,
//! and std 0.17.0's `netAcceptWindows` treated the resulting
//! `STATUS_CANCELLED` as `unreachable`: Windows CI run 38918354154 aborted in
//! all three tests, in Debug and in ReleaseSafe. The transport now accepts
//! through its own AFD accept there (src/rpc/transport/tcp/windows_accept.zig),
//! and `close` cancels the wait and waits for the accept to return before it
//! closes the handle. So `close` itself can block now, and the tests bound it
//! too. The second part tests that accept through an Io seam (Windows only).
//!
//! The file is its own test root, so that an abort here cannot hide the
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

/// How long a parked accept may take to return after `Listener.close`, and
/// `close` after that.
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
/// ECONNABORTED. Windows: the transport's own accept returns
/// `error.ListenerClosed` for the wait that `close` cancels.
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
/// leaks this, so the thread never writes into a freed stack frame. The
/// thread that calls `close` uses it too.
const Acceptor = struct {
    listener: tcp.Listener,
    entry: Entry,
    started: std.atomic.Value(bool) = .init(false),
    done: std.atomic.Value(bool) = .init(false),
    result: ?anyerror = null,
    /// The accept took a connection (`result` is then
    /// `error.UnexpectedConnection`).
    accepted: bool = false,
    closed: std.atomic.Value(bool) = .init(false),
    /// Awake-clock time `close` returned at.
    closed_ns: i64 = 0,

    fn run(self: *Acceptor) void {
        self.started.store(true, .release);
        self.result = self.acceptOnce();
        self.done.store(true, .release);
    }

    /// `Listener.close`, on its own thread: on Windows it waits for the
    /// parked accept to return.
    fn closeListener(self: *Acceptor) void {
        self.listener.close();
        self.closed_ns = nowNs(testing.io);
        self.closed.store(true, .release);
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
        self.accepted = true;
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
/// still open (on Windows, `close` keeps it open until the accept returns).
/// Returns true when the thread finished.
fn releaseParked(io: std.Io, state: *Acceptor) bool {
    const address = state.listener.getAddress();
    const client = std.Io.net.IpAddress.connect(&address, io, .{ .mode = .stream }) catch |err| {
        std.debug.print("could not dial the listener to release the parked thread: {s}\n", .{@errorName(err)});
        return false;
    };
    defer tcp.closeFd(io, .{ .handle = client.socket.handle });
    return waitFlag(io, &state.done, wake_bound_ms);
}

/// Join the acceptor and the closer, each once its flag is set within
/// `wake_bound_ms`; detach the ones that are still running. Returns true
/// when both were joined, and false when the caller must leak the state.
fn joinBounded(io: std.Io, state: *Acceptor, acceptor: std.Thread, closer: std.Thread) bool {
    var joined = true;
    if (state.done.load(.acquire) or releaseParked(io, state)) {
        acceptor.join();
    } else {
        std.debug.print("{t}: the parked thread is still blocked; detaching it and leaking its state\n", .{state.entry});
        acceptor.detach();
        joined = false;
    }
    if (waitFlag(io, &state.closed, wake_bound_ms)) {
        closer.join();
    } else {
        std.debug.print("{t}: Listener.close is still blocked; detaching its thread and leaking the state\n", .{state.entry});
        closer.detach();
        joined = false;
    }
    return joined;
}

/// Park a thread in `entry`, call `Listener.close` from another thread, and
/// expect the parked thread to return a closed-listener error within
/// `wake_bound_ms`, and `close` to return within `wake_bound_ms` after that.
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

    const closer = try std.Thread.spawn(.{}, Acceptor.closeListener, .{state});
    if (!waitFlag(io, &state.done, wake_bound_ms)) {
        std.debug.print("{t}: the parked accept did not return within {d} ms of Listener.close\n", .{ entry, wake_bound_ms });
        leak_state = !joinBounded(io, state, thread, closer);
        return error.AcceptNotWoken;
    }
    if (!waitFlag(io, &state.closed, wake_bound_ms)) {
        std.debug.print("{t}: Listener.close did not return within {d} ms of the accept's return\n", .{ entry, wake_bound_ms });
        leak_state = !joinBounded(io, state, thread, closer);
        return error.CloseNotReturned;
    }
    thread.join();
    closer.join();

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

// ---------------------------------------------------------------------------
// Windows: the transport's own accept, through an Io seam
// ---------------------------------------------------------------------------

/// kernel32's `CancelIoEx`, as `windows_accept.stopWaits` calls it: with no
/// OVERLAPPED it cancels every pending request on the handle, whichever
/// thread posted it.
extern "kernel32" fn CancelIoEx(hFile: std.os.windows.HANDLE, lpOverlapped: ?*anyopaque) callconv(.winapi) std.os.windows.BOOL;

/// An Io whose listen waits (`IOCTL_AFD_WAIT_FOR_LISTEN`, which the
/// transport's Windows accept posts through `Io.operate`) are counted, and
/// can be cancelled by Windows or held back on purpose. Every other
/// operation is `std.testing.io`'s. Windows only.
const ListenWaits = struct {
    var vtable: std.Io.VTable = undefined;
    /// Listen waits asked for, from 1.
    var posts: std.atomic.Value(u32) = .init(0);
    /// Waits 1 to `cancelled` are cancelled by Windows: a helper thread
    /// cancels every request on the handle (`CancelIoEx`, as `close` does)
    /// until the wait returns. The listener's `close_requested` is not set.
    var cancelled: u32 = 0;
    /// The first wait is posted only this long after the accept asked for
    /// it, as if its thread were descheduled between its `close_requested`
    /// check and its post.
    var hold_first_ms: u64 = 0;
    /// Set once the first wait returned; then `first_returned_ns` holds the
    /// awake-clock time it returned at. A plain i64 published by this flag,
    /// as `Acceptor.closed_ns` is: 32-bit targets have no 64-bit atomics.
    var first_returned: std.atomic.Value(bool) = .init(false);
    var first_returned_ns: i64 = 0;

    /// `std.testing.io` with this `operate`.
    fn wrap(cancelled_waits: u32, hold_ms: u64) std.Io {
        posts.store(0, .release);
        cancelled = cancelled_waits;
        hold_first_ms = hold_ms;
        first_returned.store(false, .release);
        first_returned_ns = 0;
        vtable = testing.io.vtable.*;
        vtable.operate = operate;
        return .{ .userdata = testing.io.userdata, .vtable = &vtable };
    }

    fn isListenWait(operation: std.Io.Operation) bool {
        const wait_code: u32 = @bitCast(std.os.windows.IOCTL.AFD.WAIT_FOR_LISTEN);
        return switch (operation) {
            .device_io_control => |o| @as(u32, @bitCast(o.code)) == wait_code,
            else => false,
        };
    }

    fn operate(userdata: ?*anyopaque, operation: std.Io.Operation) std.Io.Cancelable!std.Io.Operation.Result {
        const real = testing.io.vtable.operate;
        if (!isListenWait(operation)) return real(userdata, operation);
        const n = posts.fetchAdd(1, .acq_rel) + 1;
        if (n == 1 and hold_first_ms > 0) sleepMs(testing.io, hold_first_ms);
        var returned: std.atomic.Value(bool) = .init(false);
        const canceller: ?std.Thread = if (n <= cancelled)
            std.Thread.spawn(.{}, cancelUntil, .{ operation.device_io_control.file.handle, &returned }) catch @panic("cannot spawn the canceller thread")
        else
            null;
        const result = real(userdata, operation);
        returned.store(true, .release);
        if (canceller) |t| t.join();
        if (n == 1) {
            first_returned_ns = nowNs(testing.io);
            first_returned.store(true, .release);
        }
        return result;
    }

    fn cancelUntil(handle: std.os.windows.HANDLE, returned: *const std.atomic.Value(bool)) void {
        while (!returned.load(.acquire)) {
            _ = CancelIoEx(handle, null);
            sleepMs(testing.io, 5);
        }
    }
};

/// A listener on `io` with a thread in `Listener.acceptFd`, on the page
/// allocator (leaked when a thread cannot be joined; see `joinBounded`).
fn startSeamAcceptor(io: std.Io) !*Acceptor {
    const state = try std.heap.page_allocator.create(Acceptor);
    errdefer std.heap.page_allocator.destroy(state);
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    state.* = .{
        .listener = try tcp.Listener.init(testing.allocator, io, address, .{}),
        .entry = .listener_accept_fd,
    };
    return state;
}

/// Wait until `ListenWaits.posts` reaches `count` or `bound_ms` passes.
fn waitPosts(count: u32, bound_ms: i64) bool {
    const start = nowNs(testing.io);
    while (ListenWaits.posts.load(.acquire) < count) {
        if (msSince(testing.io, start) >= bound_ms) return false;
        sleepMs(testing.io, 5);
    }
    return true;
}

test "Windows: an accept posts again a listen wait that Windows cancelled while the listener is open" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    const io = ListenWaits.wrap(2, 0);
    const state = try startSeamAcceptor(io);
    var leak_state = false;
    defer if (!leak_state) std.heap.page_allocator.destroy(state);
    defer if (!leak_state) state.listener.close();

    const thread = try std.Thread.spawn(.{}, Acceptor.run, .{state});
    // Waits 1 and 2 end STATUS_CANCELLED while the listener is open, so the
    // accept posts wait 3; a dial then gives it a connection. std's own
    // accept treats the first cancelled wait as unreachable.
    const third = waitPosts(3, start_bound_ms);
    if (!releaseParked(testing.io, state)) {
        std.debug.print("the accept did not take the dialed connection (waits posted: {d})\n", .{ListenWaits.posts.load(.acquire)});
        const closer = try std.Thread.spawn(.{}, Acceptor.closeListener, .{state});
        leak_state = !joinBounded(testing.io, state, thread, closer);
        return error.AcceptNotReleased;
    }
    thread.join();
    try testing.expect(third);
    try testing.expect(state.accepted);
    // At least 3: a cancel that Windows makes on its own adds a post.
    try testing.expect(ListenWaits.posts.load(.acquire) >= 3);
}

test "Windows: Listener.close ends a parked accept with ListenerClosed and closes only after the wait returned" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    const io = ListenWaits.wrap(0, 0);
    const state = try startSeamAcceptor(io);
    var leak_state = false;
    defer if (!leak_state) std.heap.page_allocator.destroy(state);
    defer if (!leak_state) state.listener.close();

    const thread = try std.Thread.spawn(.{}, Acceptor.run, .{state});
    const posted = waitPosts(1, start_bound_ms);
    sleepMs(testing.io, park_settle_ms);
    const closer = try std.Thread.spawn(.{}, Acceptor.closeListener, .{state});
    if (!waitFlag(testing.io, &state.done, wake_bound_ms) or !waitFlag(testing.io, &state.closed, wake_bound_ms)) {
        std.debug.print("the parked accept or Listener.close did not return\n", .{});
        leak_state = !joinBounded(testing.io, state, thread, closer);
        return error.CloseDidNotWake;
    }
    thread.join();
    closer.join();
    try testing.expect(posted);
    try testing.expectEqual(@as(?anyerror, error.ListenerClosed), state.result);
    // The cancelled wait was not posted again, and the handle was closed
    // only once it had returned.
    try testing.expectEqual(@as(u32, 1), ListenWaits.posts.load(.acquire));
    try testing.expect(ListenWaits.first_returned.load(.acquire));
    const returned_ns = ListenWaits.first_returned_ns;
    try testing.expect(state.closed_ns >= returned_ns);
}

test "Windows: Listener.close waits for an accept that checked close_requested but had not posted its wait" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    // The accept's first wait is posted 300 ms after the accept read
    // `close_requested`. `close` runs in that gap. A close that closed the
    // handle at once would leave the wait to be posted on a closed handle
    // (error.Unexpected), or on whatever object Windows gave the handle
    // value to since.
    const hold_ms: u64 = 300;
    const io = ListenWaits.wrap(0, hold_ms);
    const state = try startSeamAcceptor(io);
    var leak_state = false;
    defer if (!leak_state) std.heap.page_allocator.destroy(state);
    defer if (!leak_state) state.listener.close();

    const thread = try std.Thread.spawn(.{}, Acceptor.run, .{state});
    const asked = waitPosts(1, start_bound_ms);
    const close_start_ns = nowNs(testing.io);
    const closer = try std.Thread.spawn(.{}, Acceptor.closeListener, .{state});
    if (!waitFlag(testing.io, &state.done, wake_bound_ms) or !waitFlag(testing.io, &state.closed, wake_bound_ms)) {
        std.debug.print("the accept or Listener.close did not return\n", .{});
        leak_state = !joinBounded(testing.io, state, thread, closer);
        return error.CloseDidNotWake;
    }
    thread.join();
    closer.join();
    try testing.expect(asked);
    try testing.expectEqual(@as(?anyerror, error.ListenerClosed), state.result);
    try testing.expectEqual(@as(u32, 1), ListenWaits.posts.load(.acquire));
    // `close` returned only after the held wait was posted and returned.
    try testing.expect(ListenWaits.first_returned.load(.acquire));
    const returned_ns = ListenWaits.first_returned_ns;
    try testing.expect(state.closed_ns >= returned_ns);
    try testing.expect(state.closed_ns - close_start_ns >= @as(i64, @intCast(hold_ms / 2)) * std.time.ns_per_ms);
}
