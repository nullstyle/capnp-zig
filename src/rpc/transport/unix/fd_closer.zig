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
//!   long as its sender likes, so this lane is bounded (see the budget
//!   below).
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
//!   other sent dups, never a received fd or a socket close.
//!
//! A `.socket` job still waits behind a blocked `.socket` job: a peer that
//! leaves a blocking fd unread on its own connection when the connection is
//! torn down stops this lane for as long as that close blocks. Every socket
//! close queued meanwhile holds one fd until then, and on Darwin a reader
//! blocked in a transport whose `shutdown` is queued there wakes only on its
//! own poll tick (see `Transport.shutdown`). Darwin sends every AF_UNIX
//! close there (and the read half of every `shutdown`), Linux every close
//! with bytes unread, which a peer can force. So a listener from
//! `rpc.transport.unix.listen` takes no connection while this lane holds
//! `socketLaneBound()` jobs or more (`Listener.accept` and `acceptFd` wait):
//! a peer that stalls one close and then reconnects in a loop leaves its
//! connections in the kernel's backlog, not in this process's fd table.
//! That listener's own final close runs here too: the connections still in
//! its accept queue are released inside it, with the fds on their unread
//! messages.
//!
//! ## The process fd budget
//!
//! Every fd in the `.received` and `.sent` lanes counts against the process
//! fd budget (`fd_budget`, re-exported as `fd_io.budget`) until its thread
//! has closed it, and so do the fds the runtime keeps alive for fd passing
//! (frame fds, imports, sent dups). A stuck close stops its thread, so a
//! peer that keeps sending fds, or an app that keeps sending them, would
//! otherwise fill the fd table.
//!
//! - `.received`: an fd that arrived must go somewhere, so `handOff` always
//!   takes it, and counts it in the budget as it comes, whatever the limit.
//!   These are the lane's *arrivals*: the only fds that can take the budget
//!   past its limit. `handOffCounted` takes fds the budget already counts
//!   (a frame's, an import's). Moving them here does not change the count,
//!   so they are not arrivals, and a burst of them (a `Peer` torn down with
//!   every import, a frame's fds after dispatch) closes no connection.
//!   Before each `recvmsg`, once its socket is readable, a reader takes a
//!   read claim (`claimRead`): room for one read's worst case,
//!   `max_fds_per_recv` (254: the most one send carries). It reads nothing,
//!   and the transport closes that connection with a typed cause, while the
//!   lane holds `fd_budget.limit()` arrivals or more (`full`: a close that
//!   blocks has stopped the lane), or when no claim came back within its
//!   wait. A claim is granted only while the arrivals plus the outstanding
//!   claims leave room under the limit; the first reader always gets one.
//!   So however many readers wake at once, the lane ends at most one read
//!   past the limit (a peer used to get one read in for every reader that
//!   passed a plain check before the lane filled: measured, 16 readers put
//!   4063 fds in a lane bounded at 16). A claim lasts one non-blocking
//!   `recvmsg` and its hand-off, so the readers it holds back wait
//!   microseconds. A hand-off that leaves more than the limit of arrivals
//!   in the lane reports `over_limit`, and the transport closes the
//!   connection that sent those fds. The fds already received still go to
//!   the lane: closing them anywhere else could block. Its counted fds come
//!   on top, and the budget keeps those under its limit when it lets them
//!   in.
//! - `.sent`: a transport counts each dup before it makes it, by reserving
//!   a `.sent` slot (`reserveSent`), which succeeds only while the dups fit
//!   in the budget (`fd_budget.tryAcquire`). Otherwise the transport refuses
//!   that message (`error.FdQueueFull` and a backpressure event) and makes
//!   no dup; sends without fds, and the connection, are not affected. A
//!   reserved slot and the dup handed off under it are one unit of the
//!   budget; the closer gives it back when it has closed the dup, and
//!   `trim` gives back the unit of a slot no dup used. A `.sent` close that
//!   never ends stops fd sends in the whole process (each one refused, never
//!   queued), but it cannot fill the fd table.
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
//! The queues outlive every connection, so no caller's allocator (and no
//! `std.testing.FailingAllocator`) reaches them. `injectAllocationFailure`
//! makes one of their allocations fail instead, for fault-injection tests.
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
const budget = @import("fd_budget.zig");

/// True where the closer is compiled in: Linux and Darwin.
pub const supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag.isDarwin();

/// A POSIX file descriptor number.
pub const Fd = i32;

comptime {
    if (supported and posix.fd_t != Fd) @compileError("fd_closer expects a 32-bit int fd");
}

/// Which thread and queue a job goes to (see "Three lanes" in the module
/// doc).
pub const Lane = enum(u8) {
    /// Fds a peer attached. Counted in the process fd budget; a reader stops
    /// once this lane holds the budget's limit of fds that arrived
    /// (`handOff`, not `handOffCounted`; see `claimRead`).
    received,
    /// A transport's own socket: its final close, and on Darwin the read
    /// half of its shutdown. Not counted.
    socket,
    /// A transport's dups of fds this process sends (`handOffSent`).
    /// Counted in the process fd budget from `reserveSent` on.
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

/// `reserveSent` failures. Nothing was reserved or counted.
pub const ReserveSentError = error{
    /// The dups do not fit in the process fd budget (`fd_io.budget`; see
    /// the module doc). Retry once fds held for fd passing were closed.
    FdBudgetExceeded,
    OutOfMemory,
    UnixSocketsUnsupported,
};

/// Queue capacity in one lane held free for one caller, so its next
/// hand-off of up to `slots` jobs there never allocates. One thread uses a
/// reservation at a time. Give it back with `release`. In the `.sent` lane
/// each slot is also one unit of the process fd budget (`reserveSent`).
pub const Reservation = struct {
    lane: Lane = .received,
    slots: usize = 0,
};

/// The `.received` lane's arrivals at one moment: the fds `handOff` took
/// (counting them in the budget as they came) that are not closed yet. Fds
/// moved there already counted (`handOffCounted`) are not among them: they
/// cannot grow the count. See "The process fd budget" in the module doc.
pub const Admission = struct {
    /// Arrivals handed off and not yet closed, including a hand-off that
    /// returned this. `pendingIn(.received)` counts every fd in the lane.
    pending: usize,
    /// The process fd budget's limit in force (`fd_io.budget.limit()`).
    limit: usize,
    /// `pending > limit`: the hand-off that returned this crossed the bound.
    /// The transport closes the connection that sent those fds.
    over_limit: bool,

    /// `pending >= limit`: the lane takes no more fds. A reader whose
    /// `claimRead` finds this reads nothing and closes its connection.
    pub fn full(self: Admission) bool {
        return self.pending >= self.limit;
    }
};

const Op = enum(u8) { close, shutdown };

const Job = struct {
    fd: Fd,
    op: Op,
    /// A `.received` fd that `handOff` took: one of the lane's arrivals
    /// (`LaneState.arrivals`).
    arrival: bool = false,
};

const LaneState = struct {
    mu: std.Io.Mutex = .init,
    work: std.Io.Condition = .init,
    /// Broadcast whenever the lane gets room: a job done, or (`.received`)
    /// a read claim given back. `claimRead` and `waitSocketLane` wait on it.
    room: std.Io.Condition = .init,
    /// `.received` only: reads between `claimRead` and `endRead`, each of
    /// which may still hand off up to `max_fds_per_recv` arrivals.
    read_claims: usize = 0,
    /// FIFO: `jobs.items[head..]` are queued, `jobs.items[0..head]` are
    /// taken (compacted away as the thread catches up).
    jobs: std.ArrayListUnmanaged(Job) = .empty,
    head: usize = 0,
    /// Sum of all outstanding `Reservation.slots` in this lane. Invariant
    /// (under `mu`): `jobs.capacity >= jobs.items.len + reserved`.
    reserved: usize = 0,
    /// Jobs the thread took from the queue and has not finished.
    running: usize = 0,
    /// Jobs with `Job.arrival` set that are queued or running: what
    /// `admission` measures. A job run inline, never queued, is not counted.
    arrivals: usize = 0,
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
var start_mu: std.Io.Mutex = .init;
var started: std.atomic.Value(bool) = .init(false);
/// `injectAllocationFailure`: the queue allocations still to go before the
/// one that fails; -1 when none is armed.
var inject_countdown: std.atomic.Value(isize) = .init(-1);

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
    // A `.sent` slot is a unit of the process fd budget: `reserveSent`.
    std.debug.assert(r.lane != .sent);
    if (r.slots >= slots) return;
    const extra = slots - r.slots;
    if (injectedFailure()) return error.OutOfMemory;
    const s = laneState(r.lane);
    s.lock();
    defer s.unlock();
    s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + extra) catch
        return error.OutOfMemory;
    s.reserved += extra;
    r.slots = slots;
}

/// `reserve` for a `.sent` reservation, counted in the process fd budget:
/// grow `r` to `slots` only while the extra slots fit in the budget
/// (`fd_budget.tryAcquire`). Each slot stays one unit of the budget until
/// `trim` gives it back, or until the dup handed off under it is closed
/// (see "The process fd budget" in the module doc). A request that `r`
/// already covers always succeeds.
pub fn reserveSent(r: *Reservation, slots: usize) ReserveSentError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    std.debug.assert(r.lane == .sent);
    if (r.slots >= slots) return;
    const extra = slots - r.slots;
    if (!budget.tryAcquire(extra)) return error.FdBudgetExceeded;
    if (injectedFailure()) {
        budget.release(extra);
        return error.OutOfMemory;
    }
    const s = laneState(.sent);
    s.lock();
    defer s.unlock();
    s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + extra) catch {
        budget.release(extra);
        return error.OutOfMemory;
    };
    s.reserved += extra;
    r.slots = slots;
}

/// Give back the slots `r` holds above `slots` (and, in the `.sent` lane,
/// their units of the process fd budget).
pub fn trim(r: *Reservation, slots: usize) void {
    if (comptime !supported) return;
    if (r.slots <= slots) return;
    const freed = r.slots - slots;
    const s = laneState(r.lane);
    s.lock();
    s.reserved -= freed;
    s.unlock();
    r.slots = slots;
    if (r.lane == .sent) budget.release(freed);
}

/// Give back every slot `r` still holds.
pub fn release(r: *Reservation) void {
    trim(r, 0);
}

/// Hand `fds` (fds a peer attached) to the `.received` lane, which closes
/// each one. The caller must not touch them again. They start counting
/// against the process fd budget now, whatever its limit, and stop once
/// closed. Until then they are arrivals: `admission` measures them (see
/// "The process fd budget" in the module doc).
///
/// The fds covered by `r` (a `.received` reservation; up to `r.slots`) use
/// reserved capacity and cannot fail; `r.slots` drops by that many. The rest
/// need an allocation; if it fails, or the thread cannot start, those fds
/// are closed inline on the calling thread, with a warning. Never fails.
pub fn handOff(r: ?*Reservation, fds: []const Fd) Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    if (r) |res| std.debug.assert(res.lane == .received);
    if (fds.len == 0) return admission();
    budget.acquire(fds.len);
    return handOffCloses(.received, r, fds, true);
}

/// `handOff` for fds already counted in the process fd budget: the fds a
/// transport kept for a frame, or a `Peer` for its imports. Their units
/// pass to the closer, which gives them back once it has closed the fds.
/// They are not arrivals: moving them cannot grow the count, so they never
/// make the lane `full()`, however many come at once.
pub fn handOffCounted(r: ?*Reservation, fds: []const Fd) Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    if (r) |res| std.debug.assert(res.lane == .received);
    if (fds.len == 0) return admission();
    return handOffCloses(.received, r, fds, false);
}

/// Hand `fds` (a transport's dups of fds it sent, or gave up sending) to the
/// `.sent` lane, which closes each one. The caller must not touch them
/// again. Covered by `r` (a `.sent` reservation) like `handOff`: each
/// covered fd takes over its slot's unit of the process fd budget, and an
/// fd no slot covers starts counting now. The closer gives each unit back
/// once it has closed the fd. Never fails.
pub fn handOffSent(r: ?*Reservation, fds: []const Fd) void {
    if (comptime !supported) return;
    if (r) |res| std.debug.assert(res.lane == .sent);
    if (fds.len == 0) return;
    // A reservation is used by one thread at a time (its owner's), so its
    // slot count is stable here.
    const covered = if (r) |res| @min(res.slots, fds.len) else 0;
    budget.acquire(fds.len - covered);
    _ = handOffCloses(.sent, r, fds, false);
}

/// `fds` must not be empty.
fn handOffCloses(lane: Lane, r: ?*Reservation, fds: []const Fd, arrival: bool) Admission {
    std.debug.assert(fds.len != 0);
    std.debug.assert(!arrival or lane == .received);
    var jobs: [64]Job = undefined;
    var after: Admission = undefined;
    var rest = fds;
    while (rest.len != 0) {
        const n = @min(rest.len, jobs.len);
        for (jobs[0..n], rest[0..n]) |*job, fd| job.* = .{ .fd = fd, .op = .close, .arrival = arrival };
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
/// `.received`, and counts its arrivals).
fn enqueue(lane: Lane, r: ?*Reservation, jobs: []const Job) Admission {
    ensureStarted() catch |err| {
        log.warn("fd closer thread unavailable ({t}); running {d} job(s) on the calling thread", .{ err, jobs.len });
        // Use up the reservation as a queued hand-off would have.
        if (r) |res| takeCovered(res, jobs.len);
        runAll(lane, jobs);
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
        countArrivals(s, jobs[0..covered]);
        const rest = jobs[covered..];
        if (rest.len != 0) {
            if (injectedFailure()) {
                inline_jobs = rest;
            } else if (s.jobs.ensureTotalCapacity(std.heap.page_allocator, s.jobs.items.len + s.reserved + rest.len)) |_| {
                s.jobs.appendSliceAssumeCapacity(rest);
                countArrivals(s, rest);
            } else |_| {
                inline_jobs = rest;
            }
        }
        s.work.signal(syncIo());
        after = switch (lane) {
            .received => blk: {
                const cap = budget.limit();
                break :blk .{ .pending = s.arrivals, .limit = cap, .over_limit = s.arrivals > cap };
            },
            .socket, .sent => .{ .pending = s.pendingLocked(), .limit = 0, .over_limit = false },
        };
    }
    if (inline_jobs.len != 0) {
        log.warn("fd closer queue could not grow; running {d} job(s) on the calling thread", .{inline_jobs.len});
        runAll(lane, inline_jobs);
    }
    return after;
}

/// `queued` were just appended to `s` (under its lock): count the arrivals
/// among them.
fn countArrivals(s: *LaneState, queued: []const Job) void {
    for (queued) |job| {
        if (job.arrival) s.arrivals += 1;
    }
}

/// `jobs` jobs of a hand-off under `r` run inline (no thread): take the
/// slots they would have used from `r` and its lane, as `enqueue` does for
/// a queued hand-off. The jobs keep their budget units; `runAll` gives them
/// back.
fn takeCovered(r: *Reservation, jobs: usize) void {
    const covered = @min(r.slots, jobs);
    if (covered == 0) return;
    const s = laneState(r.lane);
    s.lock();
    s.reserved -= covered;
    s.unlock();
    r.slots -= covered;
}

/// Fault injection, for tests: make the queue allocation `nth` from now
/// fail as if out of memory (0: the next one), once; null disarms it. Every
/// `reserve`, every `reserveSent` and every hand-off that a reservation does
/// not wholly cover counts as one allocation, whether or not its queue would
/// have grown, so a test can reach each of their failure paths. Returns
/// whether an earlier injection was still armed (it is replaced).
pub fn injectAllocationFailure(nth: ?usize) bool {
    const value: isize = if (nth) |n| @intCast(n) else -1;
    return inject_countdown.swap(value, .acq_rel) >= 0;
}

/// Whether an `injectAllocationFailure` is armed and has not fired yet.
pub fn allocationFailureArmed() bool {
    return inject_countdown.load(.acquire) >= 0;
}

/// One queue allocation is about to happen: true when it is the one
/// `injectAllocationFailure` armed.
fn injectedFailure() bool {
    var current = inject_countdown.load(.monotonic);
    while (current >= 0) {
        const next: isize = if (current == 0) -1 else current - 1;
        if (inject_countdown.cmpxchgWeak(current, next, .acq_rel, .monotonic)) |actual| {
            current = actual;
            continue;
        }
        return current == 0;
    }
    return false;
}

/// The `.received` lane's arrivals right now, against the process fd
/// budget's limit (see "The process fd budget" in the module doc). Readers
/// take a claim (`claimRead`) instead; this is a snapshot, for events and
/// tests.
pub fn admission() Admission {
    if (comptime !supported) return .{ .pending = 0, .limit = 0, .over_limit = false };
    const cap = budget.limit();
    const s = laneState(.received);
    s.lock();
    const arrivals = s.arrivals;
    s.unlock();
    return .{ .pending = arrivals, .limit = cap, .over_limit = arrivals > cap };
}

/// The most fds one `recvmsg` can install: one send's fds (Linux's
/// `SCM_MAX_FD` is 253; macOS takes 254). A read claim stands for this many
/// arrivals.
pub const max_fds_per_recv: usize = 254;

/// How long `claimRead` waits at most, in one wait, before it looks at the
/// caller's cancel flag again.
const claim_wait_slice_ms: i64 = 50;

/// What `claimRead` found.
pub const ReadClaim = enum {
    /// The read may go ahead. Call `endRead` once its fds were handed off
    /// (or none came).
    granted,
    /// The lane holds the budget's limit of arrivals or more
    /// (`Admission.full`): a close that blocks has stopped it. Read nothing.
    full,
    /// The claims of other reads left no room, and none came back within
    /// the wait. A claim lasts one non-blocking `recvmsg`, so this means one
    /// of them is stuck inside the kernel (a close the kernel does inside
    /// `recvmsg` at the fd limit). Read nothing.
    timed_out,
    /// The cancel flag was set while waiting.
    canceled,
};

/// Claim room in the `.received` lane for one read, before its `recvmsg`
/// (see "The process fd budget" in the module doc).
///
/// A claim stands for `max_fds_per_recv` arrivals that the read may still
/// hand off. It is granted while the arrivals plus the outstanding claims
/// stay below the budget's limit: the first reader always gets one (unless
/// the lane is `full`), and further readers only while the limit has room
/// for their worst case too. So however many readers wake at once, the lane
/// ends at most one read (`max_fds_per_recv - 1`) past the limit. Others
/// wait, up to `max_wait_ms`, for a claim to come back or an arrival to be
/// closed; a claim lasts one non-blocking `recvmsg` and a hand-off, so the
/// wait is short. Checks `cancel` (when given) at least every 50 ms.
pub fn claimRead(cancel: ?*const std.atomic.Value(bool), max_wait_ms: u32) ReadClaim {
    if (comptime !supported) return .granted;
    const io = syncIo();
    const s = laneState(.received);
    const wait_start = std.Io.Clock.awake.now(io).nanoseconds;
    const max_wait_ns: i96 = @as(i96, max_wait_ms) * std.time.ns_per_ms;
    s.lock();
    defer s.unlock();
    while (true) {
        const cap = budget.limit();
        if (s.arrivals >= cap) return .full;
        // `read_claims` is at most the number of reader threads, so the
        // product cannot overflow.
        if (s.arrivals + s.read_claims * max_fds_per_recv < cap) {
            s.read_claims += 1;
            return .granted;
        }
        if (cancel) |flag| {
            if (flag.load(.acquire)) return .canceled;
        }
        const waited = std.Io.Clock.awake.now(io).nanoseconds - wait_start;
        if (waited >= max_wait_ns) return .timed_out;
        const slice_ns = @min(max_wait_ns - waited, @as(i96, claim_wait_slice_ms) * std.time.ns_per_ms);
        const slice: std.Io.Timeout = .{ .duration = .{ .raw = .fromNanoseconds(slice_ns), .clock = .awake } };
        // Unlocks while it waits. A timeout or a cancel just loops.
        s.room.waitTimeout(io, &s.mu, slice) catch {};
    }
}

/// Give back a claim `claimRead` granted, after the read's fds (if any)
/// were handed off, so they already count as arrivals.
pub fn endRead() void {
    if (comptime !supported) return;
    const s = laneState(.received);
    s.lock();
    defer s.unlock();
    std.debug.assert(s.read_claims != 0);
    s.read_claims -= 1;
    s.room.broadcast(syncIo());
}

/// Read claims granted and not given back (`claimRead`, `endRead`).
pub fn readClaims() usize {
    if (comptime !supported) return 0;
    const s = laneState(.received);
    s.lock();
    defer s.unlock();
    return s.read_claims;
}

/// The smallest `socketLaneBound`.
pub const min_socket_lane_bound: usize = 16;

/// The `.socket` lane jobs (queued or running) at which a listener from
/// `rpc.transport.unix.listen` stops accepting: a quarter of the process fd
/// budget's limit, at least `min_socket_lane_bound`. Each job holds one
/// socket fd until it runs, and a close there can block (see "Three lanes"
/// in the module doc); a peer that stalls one would otherwise make every
/// later AF_UNIX teardown hold its fd.
pub fn socketLaneBound() usize {
    if (comptime !supported) return 0;
    return @max(budget.limit() / 4, min_socket_lane_bound);
}

/// Whether the `.socket` lane holds `socketLaneBound()` jobs or more.
pub fn socketLaneFull() bool {
    if (comptime !supported) return false;
    return pendingIn(.socket) >= socketLaneBound();
}

/// Wait until a `.socket` job finishes, or `max_wait_ms` passes. Returns at
/// once if the lane is not `socketLaneFull`.
pub fn waitSocketLane(max_wait_ms: u32) void {
    if (comptime !supported) return;
    const io = syncIo();
    const s = laneState(.socket);
    s.lock();
    defer s.unlock();
    if (s.pendingLocked() < socketLaneBound()) return;
    const wait: std.Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(max_wait_ms), .clock = .awake } };
    s.room.waitTimeout(io, &s.mu, wait) catch {};
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
            runOne(lane, job);
            s.lock();
            s.running -= 1;
            if (job.arrival) s.arrivals -= 1;
            // Cheap without waiters: one atomic load.
            s.room.broadcast(syncIo());
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

fn runAll(lane: Lane, jobs: []const Job) void {
    for (jobs) |job| runOne(lane, job);
}

/// One job. A closed fd of the `.received` or `.sent` lane stops counting
/// against the process fd budget.
fn runOne(lane: Lane, job: Job) void {
    switch (job.op) {
        .close => {
            closeOne(job.fd);
            switch (lane) {
                .received, .sent => budget.release(1),
                .socket => {},
            }
        },
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
