//! The process-wide threads that close file descriptors a peer attached, and
//! the AF_UNIX transport's own sockets.
//!
//! Experimental. Linux and Darwin only (`supported`); on every other target
//! the calls are stubs and nothing is ever queued.
//!
//! ## Why separate threads
//!
//! Closing an fd can block the thread that closes it. A peer on an AF_UNIX
//! socket can attach any open file, for example a TCP socket with
//! `SO_LINGER {1, N}` and unsent data. The final close of that socket blocks
//! the closing thread for up to N seconds, on Linux and on macOS (Darwin needs
//! `SO_LINGER_SEC` and a send queue that really stays stuck). Linux has no
//! cap on N, and the linger lasts as long as the far end keeps its window
//! shut; Darwin caps `SO_LINGER_SEC` near 327 s. A tty, a FUSE file or an NFS
//! file can block a close for longer.
//!
//! The kernel also does such final closes for fds still riding on unread
//! messages: inside the `close` of the AF_UNIX socket that holds them, and
//! on Darwin already inside `shutdown(SHUT_RD)` of that socket (measured:
//! a 2 s linger blocked `shutdown` for 2000 ms on macOS, 0 ms on Linux).
//!
//! So the Unix transport never closes a received fd on its reader thread or
//! on the Peer thread, and it never does a socket close or a Darwin
//! `shutdown(SHUT_RD)` that may dispose of fds there either.
//! `Connection.deinit` never waits for a pending close.
//!
//! The same holds for the copies of fds this process sends. The transport
//! queues a dup of each fd it is asked to send, and closes the dup once the
//! send is done or abandoned. By then the app may have closed its own fd and
//! the receiver its copy, so the dup's close can be the final one, and it
//! blocks if the file is one of the kinds above.
//!
//! ## Three lanes
//!
//! Each `Lane` is one thread and one FIFO queue. The threads start on first
//! use and never exit.
//!
//! - `.received` closes the fds peers attached. Any of them can block for as
//!   long as its sender likes, so this lane is bounded (below).
//! - `.socket` does the transport's own socket work: the final close of a
//!   socket that may still hold unread fds, and on Darwin the read half of
//!   `shutdown`. One of these blocks only when that very socket still holds
//!   fds whose close blocks, so a received fd that blocks never delays
//!   another connection's close or shutdown.
//! - `.sent` closes a transport's dups of the fds this process sent (or
//!   gave up sending). They are the app's own files, not a peer's, but the
//!   app may send one whose close blocks (a TCP socket handed to a worker,
//!   say), and once the app and the receiver have closed their copies the
//!   dup's close is the final one. A dup whose close blocks delays only
//!   other sent dups, never a received fd or a socket close. A transport
//!   stops counting a dup when it hands the dup here, so its own cap
//!   (`Transport.max_queued_fds`) does not bound this lane: the lane has a
//!   bound of its own (below).
//!
//! A `.socket` job still waits behind a blocked `.socket` job: a peer that
//! leaves a blocking fd unread on its own connection when the connection is
//! torn down stops this lane for as long as that close blocks. Every socket
//! close queued meanwhile holds one fd until then, and on Darwin a reader
//! blocked in a transport whose `shutdown` is queued there wakes only on its
//! own poll tick (see `Transport.shutdown`).
//!
//! ## The bounds
//!
//! Every fd in the `.received` lane still holds a slot in the process fd
//! table until the thread closes it. A stuck close stops the thread, and a
//! peer that keeps sending fds would then fill the table. So the lane has a
//! bound: `RLIMIT_NOFILE / 4` (the soft limit, read at first use, at least
//! `min_queue_limit`).
//!
//! - Before each `recvmsg`, once its socket is readable, a reader checks
//!   `admission()`. While the lane is `full()` (it holds `queueLimit()` fds
//!   or more) the reader takes nothing: the transport closes that connection
//!   with a typed cause.
//! - A hand-off that leaves more than the bound pending reports
//!   `over_limit`, and the transport closes the connection that sent those
//!   fds. The fds already received still go to the lane: closing them
//!   anywhere else could block.
//!
//! So the lane holds at most the bound plus one read (at most 254 fds: the
//! most one send carries) for each reader that passed its check before the
//! lane filled and had not handed off yet.
//!
//! The `.sent` lane has the same kind of bound (`sentLimit`, the same
//! default, read at first use), and it counts every dup alive in the
//! process: the dups transports hold for sending (their `.sent`
//! reservations) and the dups handed off and not yet closed. Without it, a
//! sent dup whose close blocks would stop the thread while every transport
//! kept sending and tearing down, each dup holding an fd-table slot, until
//! the process ran out of fds.
//!
//! - A transport reserves a slot for each dup before it makes the dup
//!   (`reserveSent`). While the lane is at or past its bound the reservation
//!   fails: the transport refuses that message (`error.FdQueueFull` and a
//!   backpressure event) and makes no dup. Sends without fds, and the
//!   connection, are not affected.
//! - Hand-offs never fail and never wait: every dup a transport holds is
//!   already counted.
//!
//! A message is admitted while the lane is below the bound, so the process
//! holds at most the bound plus 252 of these dups (one 253-fd message
//! admitted just below it). A `.sent` close that never ends stops fd sends
//! in the whole process (each one refused, never queued), but it cannot
//! fill the fd table.
//!
//! Item 13 of the 2026-10-04 sprint plan replaces both bounds with one
//! process fd budget, which counts these same dups.
//!
//! ## Allocation
//!
//! Each queue grows on `std.heap.page_allocator`. A caller that must never
//! allocate at hand-off time (a reader that already holds received fds)
//! first takes a `Reservation` in its lane: capacity the queue keeps free for
//! it. A hand-off covered by a reservation cannot fail. Only an uncovered
//! hand-off whose allocation fails, or one made when the thread cannot
//! start, does its work inline on the caller's thread, and it logs a warning
//! when it does.
//!
//! ## Synchronization
//!
//! The threads have no `std.Io` of their own: they outlive every connection
//! and so every `Io` a connection was built with. Their mutexes and
//! conditions run on `std.Io.Threaded.global_single_threaded`, whose futex
//! calls go straight to the OS futex and work from any thread.

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

/// The smallest bound the default can produce.
pub const min_queue_limit: usize = 16;

/// The default bound counts at most this many fds of RLIMIT_NOFILE. A soft
/// limit of `RLIM_INFINITY` would otherwise give a bound that never applies.
pub const max_counted_fd_limit: usize = 1 << 20;

/// The default bound when RLIMIT_NOFILE cannot be read: a quarter of the
/// macOS default soft limit (256).
pub const fallback_queue_limit: usize = 64;

/// Which thread and queue a job goes to (see "Three lanes" in the module
/// doc).
pub const Lane = enum(u8) {
    /// Fds a peer attached. Bounded (`queueLimit`).
    received,
    /// A transport's own socket: its final close, and on Darwin the read
    /// half of its shutdown.
    socket,
    /// A transport's dups of fds this process sends (`handOffSent`).
    /// Bounded, with the dups transports still hold (`sentLimit`).
    sent,
};

/// `ensureStarted` failures: a thread could not be spawned.
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

/// `reserveSent` failures. Nothing was reserved.
pub const ReserveSentError = error{
    /// The `.sent` lane already counts `sentLimit()` dups or more (see "The
    /// bounds" in the module doc). Retry once the closer has caught up.
    FdCloseQueueFull,
    OutOfMemory,
    UnixSocketsUnsupported,
};

/// Queue capacity in one lane held free for one caller, so its next
/// hand-off of up to `slots` jobs there never allocates. One thread uses a
/// reservation at a time. Give it back with `release`.
pub const Reservation = struct {
    lane: Lane = .received,
    slots: usize = 0,
};

/// The `.received` lane at one moment (see "The bound" in the module doc).
pub const Admission = struct {
    /// Fds handed off and not yet closed, including a hand-off that
    /// returned this.
    pending: usize,
    /// The bound in force.
    limit: usize,
    /// `pending > limit`: the hand-off that returned this crossed the bound.
    /// The transport closes the connection that sent those fds.
    over_limit: bool,

    /// `pending >= limit`: the lane takes no more fds. A reader that sees
    /// this before `recvmsg` reads nothing and closes its connection.
    pub fn full(self: Admission) bool {
        return self.pending >= self.limit;
    }
};

const Op = enum(u8) { close, shutdown };

const Job = struct {
    fd: Fd,
    op: Op,
};

const LaneState = struct {
    mu: std.Io.Mutex = .init,
    work: std.Io.Condition = .init,
    /// FIFO: `jobs.items[head..]` are queued, `jobs.items[0..head]` are
    /// taken (compacted away as the thread catches up).
    jobs: std.ArrayListUnmanaged(Job) = .empty,
    head: usize = 0,
    /// Sum of all outstanding `Reservation.slots` in this lane. Invariant
    /// (under `mu`): `jobs.capacity >= jobs.items.len + reserved`.
    reserved: usize = 0,
    /// Jobs the thread took from the queue and has not finished.
    running: usize = 0,
    /// Written once, under `start_mu`.
    thread_started: bool = false,

    fn queued(self: *const LaneState) usize {
        return self.jobs.items.len - self.head;
    }

    fn pendingLocked(self: *const LaneState) usize {
        return self.queued() + self.running;
    }

    fn lock(self: *LaneState) void {
        self.mu.lockUncancelable(syncIo());
    }

    fn unlock(self: *LaneState) void {
        self.mu.unlock(syncIo());
    }
};

var lanes: [std.enums.values(Lane).len]LaneState = @splat(.{});
/// The `.received` bound; 0 until first use. Guarded by that lane's `mu`.
var limit: usize = 0;
/// The `.sent` bound; 0 until first use. Guarded by that lane's `mu`.
var sent_limit: usize = 0;
var start_mu: std.Io.Mutex = .init;
var started: std.atomic.Value(bool) = .init(false);

fn syncIo() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}

fn laneState(lane: Lane) *LaneState {
    return &lanes[@backingInt(lane)];
}

/// Start every closer thread that is not running. Cheap after the first
/// success. Readers call it before `recvmsg`, so they never hold received fds
/// without a thread to hand them to.
pub fn ensureStarted() StartError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (started.load(.acquire)) return;
    start_mu.lockUncancelable(syncIo());
    defer start_mu.unlock(syncIo());
    if (started.load(.acquire)) return;
    for (std.enums.values(Lane)) |lane| {
        const s = laneState(lane);
        if (s.thread_started) continue;
        const thread = try std.Thread.spawn(.{}, closerMain, .{lane});
        thread.detach();
        s.thread_started = true;
    }
    started.store(true, .release);
}

/// Grow `r` to `slots` reserved queue slots in its lane. Allocates only
/// when the queue capacity is short.
pub fn reserve(r: *Reservation, slots: usize) ReserveError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (r.slots >= slots) return;
    const extra = slots - r.slots;
    const s = laneState(r.lane);
    s.lock();
    defer s.unlock();
    s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + extra) catch
        return error.OutOfMemory;
    s.reserved += extra;
    r.slots = slots;
}

/// `reserve` for a `.sent` reservation, under that lane's bound: grow `r`
/// to `slots` only while the lane counts fewer than `sentLimit()` dups
/// (reserved by any transport, or handed off and not yet closed). A request
/// that `r` already covers always succeeds. One admitted request may take
/// the lane past the bound; every later one then fails until the closer
/// catches up (see "The bounds" in the module doc).
pub fn reserveSent(r: *Reservation, slots: usize) ReserveSentError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    std.debug.assert(r.lane == .sent);
    if (r.slots >= slots) return;
    const extra = slots - r.slots;
    const s = laneState(.sent);
    s.lock();
    defer s.unlock();
    initSentLimitLocked();
    if (s.reserved + s.pendingLocked() >= sent_limit) return error.FdCloseQueueFull;
    s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + extra) catch
        return error.OutOfMemory;
    s.reserved += extra;
    r.slots = slots;
}

/// Give back the slots `r` holds above `slots`.
pub fn trim(r: *Reservation, slots: usize) void {
    if (comptime !supported) return;
    if (r.slots <= slots) return;
    const s = laneState(r.lane);
    s.lock();
    s.reserved -= r.slots - slots;
    s.unlock();
    r.slots = slots;
}

/// Give back every slot `r` still holds.
pub fn release(r: *Reservation) void {
    trim(r, 0);
}

/// Hand `fds` (fds a peer attached) to the `.received` lane, which closes
/// each one. The caller must not touch them again.
///
/// The fds covered by `r` (a `.received` reservation; up to `r.slots`) use
/// reserved capacity and cannot fail; `r.slots` drops by that many. The rest
/// need an allocation; if it fails, or the thread cannot start, those fds
/// are closed inline on the calling thread, with a warning. Never fails.
pub fn handOff(r: ?*Reservation, fds: []const Fd) Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    if (r) |res| std.debug.assert(res.lane == .received);
    if (fds.len == 0) return admission();
    return handOffCloses(.received, r, fds);
}

/// Hand `fds` (a transport's dups of fds it sent, or gave up sending) to the
/// `.sent` lane, which closes each one. The caller must not touch them
/// again. Covered by `r` (a `.sent` reservation) like `handOff`. Never
/// fails.
pub fn handOffSent(r: ?*Reservation, fds: []const Fd) void {
    if (comptime !supported) return;
    if (r) |res| std.debug.assert(res.lane == .sent);
    if (fds.len == 0) return;
    _ = handOffCloses(.sent, r, fds);
}

fn handOffCloses(lane: Lane, r: ?*Reservation, fds: []const Fd) Admission {
    var jobs: [64]Job = undefined;
    var after: Admission = undefined;
    var rest = fds;
    while (rest.len != 0) {
        const n = @min(rest.len, jobs.len);
        for (jobs[0..n], rest[0..n]) |*job, fd| job.* = .{ .fd = fd, .op = .close };
        after = enqueue(lane, r, jobs[0..n]);
        rest = rest[n..];
    }
    return after;
}

/// Hand a transport's own `socket` to the `.socket` lane for its final
/// close. Covered by `r` (a `.socket` reservation) like `handOff`. Never
/// fails.
pub fn handOffSocketClose(r: ?*Reservation, socket: Fd) void {
    if (comptime !supported) return;
    if (r) |res| std.debug.assert(res.lane == .socket);
    const job = [1]Job{.{ .fd = socket, .op = .close }};
    _ = enqueue(.socket, r, &job);
}

/// Hand a transport's own `socket` to the `.socket` lane for
/// `shutdown(SHUT_RDWR)`. It runs after every `.socket` job handed off
/// before it and before every one handed off after it, so a later
/// `handOffSocketClose` of the same socket cannot overtake it. Covered by `r`
/// (a `.socket` reservation) like `handOff`. Never fails.
pub fn handOffShutdown(r: ?*Reservation, socket: Fd) void {
    if (comptime !supported) return;
    if (r) |res| std.debug.assert(res.lane == .socket);
    const job = [1]Job{.{ .fd = socket, .op = .shutdown }};
    _ = enqueue(.socket, r, &job);
}

/// Queue `jobs` in `lane`. Returns the lane's state right after the append,
/// taken under the same lock, so the closer cannot drain this hand-off's
/// jobs before they are counted (`over_limit` only means something for
/// `.received`).
fn enqueue(lane: Lane, r: ?*Reservation, jobs: []const Job) Admission {
    ensureStarted() catch |err| {
        log.warn("fd closer thread unavailable ({t}); running {d} job(s) on the calling thread", .{ err, jobs.len });
        runAll(jobs);
        return admission();
    };

    const s = laneState(lane);
    var inline_jobs: []const Job = &.{};
    var after: Admission = undefined;
    {
        s.lock();
        defer s.unlock();
        var covered: usize = 0;
        if (r) |res| {
            covered = @min(res.slots, jobs.len);
            res.slots -= covered;
            s.reserved -= covered;
        }
        s.jobs.appendSliceAssumeCapacity(jobs[0..covered]);
        const rest = jobs[covered..];
        if (rest.len != 0) {
            if (s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + rest.len)) |_| {
                s.jobs.appendSliceAssumeCapacity(rest);
            } else |_| {
                inline_jobs = rest;
            }
        }
        s.work.signal(syncIo());
        const pending_now = s.pendingLocked();
        // `limit` is guarded by the `.received` lane's lock: read it only
        // under that one.
        after = switch (lane) {
            .received => blk: {
                initLimitLocked();
                break :blk .{ .pending = pending_now, .limit = limit, .over_limit = pending_now > limit };
            },
            .socket, .sent => .{ .pending = pending_now, .limit = 0, .over_limit = false },
        };
    }
    if (inline_jobs.len != 0) {
        log.warn("fd closer queue could not grow; running {d} job(s) on the calling thread", .{inline_jobs.len});
        runAll(inline_jobs);
    }
    return after;
}

/// The `.received` lane right now. A reader checks `full()` once its socket
/// is readable and before `recvmsg` (see "The bound" in the module doc).
pub fn admission() Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    const s = laneState(.received);
    s.lock();
    defer s.unlock();
    initLimitLocked();
    const pending_now = s.pendingLocked();
    return .{ .pending = pending_now, .limit = limit, .over_limit = pending_now > limit };
}

/// Jobs handed off and not yet done, in every lane.
pub fn pending() usize {
    if (comptime !supported) return 0;
    var total: usize = 0;
    for (std.enums.values(Lane)) |lane| total += pendingIn(lane);
    return total;
}

/// Jobs handed off to `lane` and not yet done.
pub fn pendingIn(lane: Lane) usize {
    if (comptime !supported) return 0;
    const s = laneState(lane);
    s.lock();
    defer s.unlock();
    return s.pendingLocked();
}

/// The `.received` bound in force (see the module doc).
pub fn queueLimit() usize {
    if (comptime !supported) return 0;
    const s = laneState(.received);
    s.lock();
    defer s.unlock();
    initLimitLocked();
    return limit;
}

/// Replace the `.received` bound (at least 1) and return the previous one.
/// For tests, and for processes that set their own fd budget.
pub fn setQueueLimit(new_limit: usize) usize {
    if (comptime !supported) return 0;
    const s = laneState(.received);
    s.lock();
    defer s.unlock();
    initLimitLocked();
    const previous = limit;
    limit = @max(new_limit, 1);
    return previous;
}

/// Under the `.received` lane's `mu`.
fn initLimitLocked() void {
    if (limit != 0) return;
    limit = defaultQueueLimit();
}

/// The `.sent` bound in force (see "The bounds" in the module doc).
pub fn sentLimit() usize {
    if (comptime !supported) return 0;
    const s = laneState(.sent);
    s.lock();
    defer s.unlock();
    initSentLimitLocked();
    return sent_limit;
}

/// Replace the `.sent` bound (at least 1) and return the previous one. For
/// tests, and for processes that set their own fd budget.
pub fn setSentLimit(new_limit: usize) usize {
    if (comptime !supported) return 0;
    const s = laneState(.sent);
    s.lock();
    defer s.unlock();
    initSentLimitLocked();
    const previous = sent_limit;
    sent_limit = @max(new_limit, 1);
    return previous;
}

/// Under the `.sent` lane's `mu`.
fn initSentLimitLocked() void {
    if (sent_limit != 0) return;
    sent_limit = defaultQueueLimit();
}

fn defaultQueueLimit() usize {
    const limits = posix.getrlimit(.NOFILE) catch return fallback_queue_limit;
    const soft: usize = @intCast(@min(limits.cur, max_counted_fd_limit));
    return @max(soft / 4, min_queue_limit);
}

fn closerMain(lane: Lane) void {
    const s = laneState(lane);
    var batch: [64]Job = undefined;
    while (true) {
        s.lock();
        while (s.queued() == 0) s.work.waitUncancelable(syncIo(), &s.mu);
        const n = @min(batch.len, s.queued());
        @memcpy(batch[0..n], s.jobs.items[s.head..][0..n]);
        s.head += n;
        compactLocked(s);
        s.running += n;
        s.unlock();

        // Count each job done as it finishes, so `pending` drops past a
        // job that returned while a later one in the batch blocks.
        for (batch[0..n]) |job| {
            runOne(job);
            s.lock();
            s.running -= 1;
            s.unlock();
        }
    }
}

/// Drop the taken prefix once it is at least half the list. Only ever
/// shrinks `jobs.items.len`, so the reservation invariant holds.
fn compactLocked(s: *LaneState) void {
    const remaining = s.queued();
    if (remaining == 0) {
        s.jobs.clearRetainingCapacity();
        s.head = 0;
        return;
    }
    if (s.head < remaining) return;
    std.mem.copyForwards(Job, s.jobs.items[0..remaining], s.jobs.items[s.head..]);
    s.jobs.shrinkRetainingCapacity(remaining);
    s.head = 0;
}

fn runAll(jobs: []const Job) void {
    for (jobs) |job| runOne(job);
}

fn runOne(job: Job) void {
    switch (job.op) {
        .close => closeOne(job.fd),
        .shutdown => shutdownOne(job.fd),
    }
}

/// One raw close. Never retried: after EINTR the fd is already released on
/// Linux, and a retry could close a number another thread just reused.
fn closeOne(fd: Fd) void {
    const rc = posix.system.close(fd);
    switch (posix.errno(rc)) {
        .SUCCESS, .INTR => {},
        // Log the number, never the tag: `posix.E` does not name every errno.
        else => |err| log.debug("closing an fd failed: errno {d}", .{@backingInt(err)}),
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
