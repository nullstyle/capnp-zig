const std = @import("std");
const builtin = @import("builtin");

const Net = std.Io.net;

/// Single-in-flight UDP receive used by QUIC on Windows.
///
/// Windows `std.Io` sockets are backed by AFD handles, and the timed
/// `Socket.receiveTimeout` path is not available there.  The bridge instead
/// runs one ordinary blocking receive as an `io.concurrent` task.  The owner
/// thread waits on `cond` for completion, a timer, or an explicit wake and is
/// the only thread that ever feeds bytes into quic-zig or invokes callbacks.
/// A timer wake deliberately leaves the receive in flight; teardown is the
/// only path that cancels it.
pub const Bridge = struct {
    mu: std.Io.Mutex = .init,
    cond: std.Io.Condition = .init,
    future: ?std.Io.Future(void) = null,
    outcome: ?Outcome = null,
    wake_requested: bool = false,
    cancel_count: usize = 0,
    fail_next_start: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},

    pub const Datagram = struct {
        from: Net.IpAddress,
        /// Slice of the buffer supplied when this receive was launched.
        /// A timer or wake may return control while the task stays in flight,
        /// and a later call is allowed to supply a different buffer. Carrying
        /// the original slice prevents callers from accidentally slicing that
        /// later buffer when the retained receive completes.
        data: []u8,
        flags: Net.IncomingMessage.Flags,
    };

    pub const WaitResult = union(enum) {
        datagram: Datagram,
        /// A datagram arrived that did not fit in `rx_buf`.  Reported as its
        /// own outcome rather than as a `Datagram`, because the two platforms
        /// hand back different amounts of it and neither is worth processing:
        /// POSIX sets `MSG_TRUNC` and returns the prefix that fit, while
        /// Windows fails the receive with `STATUS_BUFFER_OVERFLOW` and
        /// discards the payload *and* the sender address.  Synthesizing a
        /// `Datagram` here would mean inventing a `from` that Windows never
        /// supplied, so callers get a fact instead: something oversized
        /// arrived and there is nothing to feed to QUIC.
        truncated,
        timeout,
        wake,
    };

    const Outcome = union(enum) {
        datagram: Datagram,
        truncated,
        fail: anyerror,
    };

    /// Wait for a datagram without ever moving QUIC state or callbacks onto
    /// the worker.  At most one receive task exists for the bridge.
    pub fn receive(
        self: *Bridge,
        io: std.Io,
        socket: Net.Socket,
        rx_buf: []u8,
        wait_duration: std.Io.Duration,
    ) !WaitResult {
        try self.ensureReceive(io, socket, rx_buf);

        var wake_requested = false;
        var outcome: ?Outcome = null;
        {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);

            if (self.outcome == null and !self.wake_requested and wait_duration.toMicroseconds() > 0) {
                const timeout: std.Io.Timeout = .{ .duration = .{
                    .raw = wait_duration,
                    .clock = .awake,
                } };
                self.cond.waitTimeout(io, &self.mu, timeout) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
            }

            if (self.wake_requested) {
                self.wake_requested = false;
                wake_requested = true;
            }
            if (self.outcome) |ready| {
                outcome = ready;
                self.outcome = null;
            }
        }

        if (outcome) |ready| {
            // Publication happens immediately before the task returns.  Await
            // it here so ownership of rx_buf is back on the loop thread before
            // the caller inspects or reuses the bytes.
            if (self.future) |*future| future.await(io);
            self.future = null;
            return switch (ready) {
                .datagram => |datagram| .{ .datagram = datagram },
                .truncated => .truncated,
                .fail => |err| return err,
            };
        }

        if (wake_requested) return .wake;
        return .timeout;
    }

    /// Wake the owner-side condition from any thread.  The in-flight receive
    /// remains valid and is neither canceled nor duplicated.
    pub fn wake(self: *Bridge, io: std.Io) void {
        self.mu.lockUncancelable(io);
        self.wake_requested = true;
        self.cond.broadcast(io);
        self.mu.unlock(io);
    }

    /// Clear the bridge predicate when the outer wake flag was consumed before
    /// entering `receive`.  This prevents one cross-thread wake from producing
    /// two empty loop iterations.
    pub fn consumeWake(self: *Bridge, io: std.Io) void {
        self.mu.lockUncancelable(io);
        self.wake_requested = false;
        self.mu.unlock(io);
    }

    /// Cancel and reap the sole receive task.  Repeated calls are no-ops.  The
    /// owner thread calls this before closing the socket or firing close
    /// callbacks, so the worker never observes torn-down transport storage.
    pub fn cancel(self: *Bridge, io: std.Io) void {
        if (self.future) |*future| {
            _ = future.cancel(io);
            self.future = null;
            self.cancel_count += 1;
        }

        self.mu.lockUncancelable(io);
        self.outcome = null;
        self.wake_requested = false;
        self.mu.unlock(io);
    }

    pub fn hasInFlight(self: *const Bridge) bool {
        return self.future != null;
    }

    pub fn cancellationCount(self: *const Bridge) usize {
        return self.cancel_count;
    }

    /// Deterministic test seam for the `io.concurrent` start transaction.
    /// Production builds carry no storage or branch for it.
    pub fn failNextStartForTesting(self: *Bridge) void {
        if (comptime !builtin.is_test) @compileError("test-only UDP receive bridge hook");
        self.fail_next_start = true;
    }

    fn ensureReceive(self: *Bridge, io: std.Io, socket: Net.Socket, rx_buf: []u8) !void {
        if (self.future != null) return;

        if (comptime builtin.is_test) {
            if (self.fail_next_start) {
                self.fail_next_start = false;
                return error.ConcurrencyUnavailable;
            }
        }

        const have_outcome = blk: {
            self.mu.lockUncancelable(io);
            defer self.mu.unlock(io);
            break :blk self.outcome != null;
        };
        if (have_outcome) return;

        self.future = try io.concurrent(receiveTask, .{ self, io, socket, rx_buf });
    }

    fn receiveTask(self: *Bridge, io: std.Io, socket: Net.Socket, rx_buf: []u8) void {
        const outcome: Outcome = blk: {
            const msg = socket.receive(io, rx_buf) catch |err| {
                // Windows is the only platform that reports an oversized
                // datagram as a *failed* receive: AFD returns
                // STATUS_BUFFER_OVERFLOW, which std maps to MessageOversize,
                // and that is the only source of this error on the Windows
                // receive path.  Left as an error it would propagate out of
                // the connection step and tear the endpoint down — so any
                // host could kill a Windows QUIC endpoint (and, on a fanout
                // server, every session on it) with one spoofed oversized
                // UDP datagram.  Normalize it to the same per-datagram fault
                // POSIX already reports through MSG_TRUNC.  Deliberately not
                // applied off Windows, where EMSGSIZE on a receive does not
                // carry this meaning.
                if (comptime builtin.target.os.tag == .windows) {
                    if (err == error.MessageOversize) break :blk .truncated;
                }
                break :blk .{ .fail = err };
            };
            if (msg.flags.trunc) break :blk .truncated;
            break :blk .{ .datagram = .{
                .from = msg.from,
                .data = msg.data,
                .flags = msg.flags,
            } };
        };

        self.mu.lockUncancelable(io);
        self.outcome = outcome;
        self.cond.broadcast(io);
        self.mu.unlock(io);
    }
};
