//! The process-wide thread that closes file descriptors a peer attached.
//!
//! Experimental. Linux and Darwin only (`supported`); on every other target
//! the calls are stubs and nothing is ever queued.
//!
//! ## Why a separate thread
//!
//! Closing an fd can block the thread that closes it. A peer on an AF_UNIX
//! socket can attach any open file, for example a TCP socket with
//! `SO_LINGER {1, N}` and unsent data. The final close of that socket blocks
//! the closing thread for up to N seconds, on Linux and on macOS (Darwin needs
//! `SO_LINGER_SEC` and a send queue that really stays stuck). A tty, a FUSE
//! file or an NFS file can block a close for longer.
//!
//! The kernel also does such final closes for fds still riding on unread
//! messages: inside the `close` of the AF_UNIX socket that holds them, and
//! on Darwin already inside `shutdown(SHUT_RD)` of that socket (measured:
//! a 2 s linger blocked `shutdown` for 2000 ms on macOS, 0 ms on Linux).
//!
//! So the Unix transport never closes a received fd on its reader thread or
//! on the Peer thread. It hands every received fd here, and at `deinit` its
//! own socket; on Darwin its socket-level shutdown comes here too. One thread
//! per process does that work, in hand-off order. It starts on first use and
//! never exits. `Connection.deinit` never waits for a pending close.
//!
//! ## The queue bound
//!
//! Every fd in the queue still holds a slot in the process fd table until the
//! thread closes it. A stuck close (one of the files above) stops the thread,
//! and a peer that keeps sending fds would then fill the table. So the queue
//! has a bound: `RLIMIT_NOFILE / 4` (the soft limit, read at first use, at
//! least `min_queue_limit`). A hand-off that leaves more than the bound
//! pending reports `over_limit`, and the transport closes the connection that
//! sent those fds and stops reading from it. The fds already received still
//! go to the queue: closing them anywhere else could block.
//!
//! The bound is soft by at most one read per connection (the fds of the read
//! that crossed it). Item 13 of the 2026-10-04 sprint plan replaces it with a
//! process fd budget.
//!
//! ## Allocation
//!
//! The queue grows on `std.heap.page_allocator`. A caller that must never
//! allocate at hand-off time (a reader that already holds received fds)
//! first takes a `Reservation`: capacity the queue keeps free for it. A
//! hand-off covered by a reservation cannot fail. Only an uncovered hand-off
//! whose allocation fails, or one made when the thread cannot start, does
//! its work inline on the caller's thread, and it logs a warning when it does.
//!
//! ## Synchronization
//!
//! The thread has no `std.Io` of its own: it outlives every connection and so
//! every `Io` a connection was built with. Its mutex and condition run on
//! `std.Io.Threaded.global_single_threaded`, whose futex calls go straight to
//! the OS futex and work from any thread.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const log = std.log.scoped(.rpc_fd_closer);

/// True where the closer is compiled in: Linux and Darwin.
pub const supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag.isDarwin();

/// A POSIX file descriptor number.
pub const Fd = i32;

comptime {
    if (supported and posix.fd_t != Fd) @compileError("fd_closer expects a 32-bit int fd");
}

/// The smallest queue bound the default can produce.
pub const min_queue_limit: usize = 16;

/// The default bound counts at most this many fds of RLIMIT_NOFILE. A soft
/// limit of `RLIM_INFINITY` would otherwise give a bound that never applies.
pub const max_counted_fd_limit: usize = 1 << 20;

/// The default bound when RLIMIT_NOFILE cannot be read: a quarter of the
/// macOS default soft limit (256).
pub const fallback_queue_limit: usize = 64;

/// `ensureStarted` failures: the thread could not be spawned.
pub const StartError = error{
    ThreadQuotaExceeded,
    SystemResources,
    OutOfMemory,
    LockedMemoryLimitExceeded,
    Unexpected,
    UnixSocketsUnsupported,
};

/// `reserve` failures.
pub const ReserveError = error{
    OutOfMemory,
    UnixSocketsUnsupported,
};

/// Queue capacity held free for one caller, so its next hand-off of up to
/// `slots` jobs never allocates. One thread uses a reservation at a time.
/// Give it back with `release`.
pub const Reservation = struct {
    slots: usize = 0,
};

/// The queue state right after a hand-off.
pub const Admission = struct {
    /// Jobs handed off and not yet done, including this hand-off.
    pending: usize,
    /// The queue bound in force.
    limit: usize,
    /// `pending > limit`: the closer is behind. The transport closes the
    /// connection that sent these fds and stops reading from it.
    over_limit: bool,
};

const Op = enum(u8) { close, shutdown };

const Job = struct {
    fd: Fd,
    op: Op,
};

const State = struct {
    mu: std.Io.Mutex = .init,
    work: std.Io.Condition = .init,
    /// FIFO: `jobs.items[head..]` are queued, `jobs.items[0..head]` are
    /// taken (compacted away as the thread catches up).
    jobs: std.ArrayListUnmanaged(Job) = .empty,
    head: usize = 0,
    /// Sum of all outstanding `Reservation.slots`. Invariant (under `mu`):
    /// `jobs.capacity >= jobs.items.len + reserved`.
    reserved: usize = 0,
    /// Jobs the thread took from the queue and has not finished.
    running: usize = 0,
    /// The queue bound; 0 until first use.
    limit: usize = 0,
    thread_started: bool = false,

    fn queued(self: *const State) usize {
        return self.jobs.items.len - self.head;
    }

    fn pendingLocked(self: *const State) usize {
        return self.queued() + self.running;
    }
};

var state: State = .{};
var started: std.atomic.Value(bool) = .init(false);

fn syncIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

fn lock() void {
    state.mu.lockUncancelable(syncIo());
}

fn unlock() void {
    state.mu.unlock(syncIo());
}

/// Start the closer thread if it is not running. Cheap after the first
/// success. Readers call it before `recvmsg`, so they never hold received fds
/// without a thread to hand them to.
pub fn ensureStarted() StartError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (started.load(.acquire)) return;
    lock();
    defer unlock();
    if (state.thread_started) return;
    initLimitLocked();
    const thread = try std.Thread.spawn(.{}, closerMain, .{});
    thread.detach();
    state.thread_started = true;
    started.store(true, .release);
}

/// Grow `r` to `slots` reserved queue slots. Allocates only when the queue
/// capacity is short.
pub fn reserve(r: *Reservation, slots: usize) ReserveError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (r.slots >= slots) return;
    const extra = slots - r.slots;
    lock();
    defer unlock();
    initLimitLocked();
    state.jobs.ensureTotalCapacity(std.heap.page_allocator, state.jobs.items.len + state.reserved + extra) catch
        return error.OutOfMemory;
    state.reserved += extra;
    r.slots = slots;
}

/// Give back every slot `r` still holds.
pub fn release(r: *Reservation) void {
    if (comptime !supported) return;
    if (r.slots == 0) return;
    lock();
    state.reserved -= r.slots;
    unlock();
    r.slots = 0;
}

/// Hand `fds` to the closer thread, which closes each one. The caller must
/// not touch them again.
///
/// The fds covered by `r` (up to `r.slots`) use reserved capacity and cannot
/// fail; `r.slots` drops by that many. The rest need an allocation; if it
/// fails, or the thread cannot start, those fds are closed inline on the
/// calling thread, with a warning. Never fails.
pub fn handOff(r: ?*Reservation, fds: []const Fd) Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    if (fds.len == 0) return snapshot();
    var jobs: [64]Job = undefined;
    var admission: Admission = undefined;
    var rest = fds;
    while (rest.len != 0) {
        const n = @min(rest.len, jobs.len);
        for (jobs[0..n], rest[0..n]) |*job, fd| job.* = .{ .fd = fd, .op = .close };
        admission = enqueue(r, jobs[0..n]);
        rest = rest[n..];
    }
    return admission;
}

/// Hand `socket` to the closer thread for `shutdown(SHUT_RDWR)`. It runs
/// after every job handed off before it and before every job handed off
/// after it, so a later hand-off of the same socket's close cannot overtake
/// it. Covered by `r` like `handOff`. Never fails.
pub fn handOffShutdown(r: ?*Reservation, socket: Fd) void {
    if (comptime !supported) return;
    const job = [1]Job{.{ .fd = socket, .op = .shutdown }};
    _ = enqueue(r, &job);
}

fn enqueue(r: ?*Reservation, jobs: []const Job) Admission {
    ensureStarted() catch |err| {
        log.warn("fd closer thread unavailable ({t}); running {d} job(s) on the calling thread", .{ err, jobs.len });
        runAll(jobs);
        return snapshot();
    };

    var inline_jobs: []const Job = &.{};
    var admission: Admission = undefined;
    {
        lock();
        defer unlock();
        initLimitLocked();
        var covered: usize = 0;
        if (r) |res| {
            covered = @min(res.slots, jobs.len);
            res.slots -= covered;
            state.reserved -= covered;
        }
        state.jobs.appendSliceAssumeCapacity(jobs[0..covered]);
        const rest = jobs[covered..];
        if (rest.len != 0) {
            if (state.jobs.ensureTotalCapacity(std.heap.page_allocator, state.jobs.items.len + state.reserved + rest.len)) |_| {
                state.jobs.appendSliceAssumeCapacity(rest);
            } else |_| {
                inline_jobs = rest;
            }
        }
        state.work.signal(syncIo());
        const pending_now = state.pendingLocked();
        admission = .{
            .pending = pending_now,
            .limit = state.limit,
            .over_limit = pending_now > state.limit,
        };
    }
    if (inline_jobs.len != 0) {
        log.warn("fd closer queue could not grow; running {d} job(s) on the calling thread", .{inline_jobs.len});
        runAll(inline_jobs);
    }
    return admission;
}

/// Jobs handed off and not yet done.
pub fn pending() usize {
    if (comptime !supported) return 0;
    lock();
    defer unlock();
    return state.pendingLocked();
}

/// The queue bound in force (see the module doc).
pub fn queueLimit() usize {
    if (comptime !supported) return 0;
    lock();
    defer unlock();
    initLimitLocked();
    return state.limit;
}

/// Replace the queue bound (at least 1) and return the previous one. For
/// tests, and for processes that set their own fd budget.
pub fn setQueueLimit(limit: usize) usize {
    if (comptime !supported) return 0;
    lock();
    defer unlock();
    initLimitLocked();
    const previous = state.limit;
    state.limit = @max(limit, 1);
    return previous;
}

fn snapshot() Admission {
    lock();
    defer unlock();
    initLimitLocked();
    const pending_now = state.pendingLocked();
    return .{ .pending = pending_now, .limit = state.limit, .over_limit = pending_now > state.limit };
}

fn initLimitLocked() void {
    if (state.limit != 0) return;
    state.limit = defaultQueueLimit();
}

fn defaultQueueLimit() usize {
    const limits = posix.getrlimit(.NOFILE) catch return fallback_queue_limit;
    const soft: usize = @intCast(@min(limits.cur, max_counted_fd_limit));
    return @max(soft / 4, min_queue_limit);
}

fn closerMain() void {
    var batch: [64]Job = undefined;
    while (true) {
        lock();
        while (state.queued() == 0) state.work.waitUncancelable(syncIo(), &state.mu);
        const n = @min(batch.len, state.queued());
        @memcpy(batch[0..n], state.jobs.items[state.head..][0..n]);
        state.head += n;
        compactLocked();
        state.running += n;
        unlock();

        runAll(batch[0..n]);

        lock();
        state.running -= n;
        unlock();
    }
}

/// Drop the taken prefix once it is at least half the list. Only ever
/// shrinks `jobs.items.len`, so the reservation invariant holds.
fn compactLocked() void {
    const remaining = state.queued();
    if (remaining == 0) {
        state.jobs.clearRetainingCapacity();
        state.head = 0;
        return;
    }
    if (state.head < remaining) return;
    std.mem.copyForwards(Job, state.jobs.items[0..remaining], state.jobs.items[state.head..]);
    state.jobs.shrinkRetainingCapacity(remaining);
    state.head = 0;
}

fn runAll(jobs: []const Job) void {
    for (jobs) |job| switch (job.op) {
        .close => closeOne(job.fd),
        .shutdown => shutdownOne(job.fd),
    };
}

/// One raw close. Never retried: after EINTR the fd is already released on
/// Linux, and a retry could close a number another thread just reused.
fn closeOne(fd: Fd) void {
    const rc = posix.system.close(fd);
    switch (posix.errno(rc)) {
        .SUCCESS, .INTR => {},
        // Log the number, never the tag: `posix.E` does not name every errno.
        else => |err| log.debug("closing a received fd failed: errno {d}", .{@backingInt(err)}),
    }
}

fn shutdownOne(fd: Fd) void {
    const rc = posix.system.shutdown(fd, posix.SHUT.RDWR);
    switch (posix.errno(rc)) {
        .SUCCESS => {},
        // A socket the peer already closed reports ENOTCONN; nothing to do.
        else => |err| log.debug("socket shutdown failed: errno {d}", .{@backingInt(err)}),
    }
}
