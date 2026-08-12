const std = @import("std");
const builtin = @import("builtin");

const scheduler = @import("scheduler.zig");
const wake_lock = @import("../wake_lock.zig");

pub const PollResult = struct {
    socket_ready: bool = false,
    wake_drained: bool = false,
};

pub const Handle = struct {
    requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// POSIX-only run-loop wake pipe. [0] is read by the run loop; [1] is
    /// written by cross-thread wake requests.
    fds: ?[2]std.posix.fd_t = null,
    /// Serializes `request()` (called from any thread) against the fd close in
    /// `deinit()` (owner thread). Without it a wake racing teardown reads
    /// `fds` just before deinit closes them and writes to a closed — possibly
    /// kernel-recycled — descriptor. Uses `std.atomic.Mutex` (mirroring the
    /// TCP connection's `wake_mu`) because `request()` may run outside any io
    /// task; the critical sections are tiny so a spin is fine. Loop-thread
    /// paths (`waitForSocket`/`drain`/`consumeRequested`) stay unlocked: only
    /// the owner thread closes the fds, and it cannot be polling and in
    /// `deinit()` at the same time.
    mu: wake_lock.Lock = .{},

    pub fn init() Handle {
        return .{
            .fds = createFds(),
        };
    }

    fn lock(self: *Handle) void {
        // `false`: the Handle has no owner-configurable checks field of its
        // own, so the wake-lock diagnostics here are Debug-only. That is the
        // mode CI runs the threaded suites in, which is where this matters.
        self.mu.acquire("quic.wake.Handle.mu", false);
    }

    fn unlock(self: *Handle) void {
        self.mu.release(false);
    }

    pub fn deinit(self: *Handle) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        // Close under the lock so a `request()` mid-write cannot land on a
        // kernel-recycled descriptor.
        self.lock();
        defer self.unlock();
        const fds = self.fds orelse return;
        self.fds = null;
        _ = std.posix.system.close(fds[0]);
        _ = std.posix.system.close(fds[1]);
    }

    pub fn request(self: *Handle) void {
        if (self.requested.swap(true, .acq_rel)) return;
        self.lock();
        defer self.unlock();
        const fds = self.fds orelse return;
        wake_lock.writeByte(fds[1]);
    }

    pub fn isRequested(self: *const Handle) bool {
        return self.requested.load(.acquire);
    }

    pub fn consumeRequested(self: *Handle) bool {
        if (!self.requested.swap(false, .acq_rel)) return false;
        self.drain();
        return true;
    }

    pub fn isSupported(self: *const Handle) bool {
        return self.fds != null;
    }

    pub fn waitForSocket(
        self: *Handle,
        socket_fd: std.posix.fd_t,
        wait_duration: std.Io.Duration,
    ) !?PollResult {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return null;
        const fds = self.fds orelse return null;

        var poll_fds = [2]std.posix.pollfd{
            .{ .fd = socket_fd, .events = std.posix.POLL.IN, .revents = 0 },
            .{ .fd = fds[0], .events = std.posix.POLL.IN, .revents = 0 },
        };
        const timeout_ms = scheduler.durationToPollTimeoutMs(wait_duration);
        while (true) {
            const rc = std.posix.system.poll(@ptrCast(&poll_fds), poll_fds.len, timeout_ms);
            // Third instance of the signedness hazard in this file: sign-first
            // classified a FAILED poll as "descriptors are ready" on Linux and
            // fell through to read `revents` the kernel never populated.
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => continue,
                else => return error.PollFailed,
            }
            if (rc == 0) return PollResult{};
            break;
        }

        var result = PollResult{};
        if (poll_fds[1].revents & (std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR) != 0) {
            self.drain();
            _ = self.requested.swap(false, .acq_rel);
            result.wake_drained = true;
        }
        if (poll_fds[0].revents & std.posix.POLL.NVAL != 0) return error.BrokenPipe;
        result.socket_ready =
            poll_fds[0].revents & (std.posix.POLL.IN | std.posix.POLL.HUP | std.posix.POLL.ERR) != 0;
        return result;
    }

    fn drain(self: *Handle) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const fds = self.fds orelse return;
        var buf: [64]u8 = undefined;
        while (true) {
            const rc = std.posix.system.read(fds[0], &buf, buf.len);
            // Classify via errno BEFORE treating `rc` as a byte count, the same
            // way `tcp.connection.pollRetryIntr` does. On Linux
            // `std.posix.system.read` is the raw syscall and returns `usize`,
            // so a failure arrives as -errno reinterpreted as a huge POSITIVE
            // value: testing `rc > 0` first classifies EVERY error as "bytes
            // read" and loops forever. On macOS/BSD the libc shim returns
            // `isize`, where -1 fails that test and the errno switch runs --
            // which is why the sign-first version hung only on Linux, and hung
            // reliably, since EAGAIN is the ordinary way a drain finishes.
            switch (std.posix.errno(rc)) {
                .SUCCESS => {},
                .INTR => continue,
                // AGAIN (socket empty) and anything else: nothing left to drain.
                else => return,
            }
            if (rc == 0) return; // EOF
        }
    }
};

fn createFds() ?[2]std.posix.fd_t {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return null;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return null;
    }
    wake_lock.setNonBlocking(fds[0]);
    wake_lock.setNonBlocking(fds[1]);
    return fds;
}
