const std = @import("std");
const builtin = @import("builtin");

const scheduler = @import("scheduler.zig");

pub const PollResult = struct {
    socket_ready: bool = false,
    wake_drained: bool = false,
};

pub const Handle = struct {
    requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    /// POSIX-only run-loop wake pipe. [0] is read by the run loop; [1] is
    /// written by cross-thread wake requests.
    fds: ?[2]std.posix.fd_t = null,

    pub fn init() Handle {
        return .{
            .fds = createFds(),
        };
    }

    pub fn deinit(self: *Handle) void {
        if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
        const fds = self.fds orelse return;
        self.fds = null;
        _ = std.posix.system.close(fds[0]);
        _ = std.posix.system.close(fds[1]);
    }

    pub fn request(self: *Handle) void {
        if (self.requested.swap(true, .acq_rel)) return;
        const fds = self.fds orelse return;
        writeByte(fds[1]);
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
            if (rc > 0) break;
            if (rc == 0) return PollResult{};
            if (std.posix.errno(rc) == .INTR) continue;
            return error.PollFailed;
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
            if (rc > 0) continue;
            if (rc == 0) return;
            switch (std.posix.errno(rc)) {
                .INTR => continue,
                .AGAIN => return,
                else => return,
            }
        }
    }
};

fn createFds() ?[2]std.posix.fd_t {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return null;
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds) != 0) {
        return null;
    }
    setNonBlocking(fds[0]);
    setNonBlocking(fds[1]);
    return fds;
}

fn setNonBlocking(fd: std.posix.fd_t) void {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
    const rc = std.posix.system.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
    if (std.posix.errno(rc) != .SUCCESS) return;
    var flags: usize = @intCast(rc);
    flags |= @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
    _ = std.posix.system.fcntl(fd, std.posix.F.SETFL, flags);
}

fn writeByte(fd: std.posix.fd_t) void {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
    const byte = [_]u8{1};
    while (true) {
        const rc = std.posix.system.write(fd, &byte, byte.len);
        if (rc > 0) return;
        if (rc == 0) return;
        switch (std.posix.errno(rc)) {
            .INTR => continue,
            .AGAIN => return,
            else => return,
        }
    }
}
