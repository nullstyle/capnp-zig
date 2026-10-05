//! The process fd budget (Experimental, Linux and macOS): one count of the
//! file descriptors the RPC runtime holds because of fd passing, and one
//! limit for all of them.
//!
//! ## What counts
//!
//! Every fd that fd passing puts in this process's fd table, from the moment
//! it exists until it is closed or handed to the app:
//!
//! - fds a peer attached that a transport keeps for a frame (fd passing on),
//!   from `recvmsg` until the frame's consumer takes them or they go to the
//!   closer;
//! - fds a `Peer` keeps for its imports (`Peer.importFd`), until the import is
//!   released;
//! - a transport's dups of the fds it sends, from before the dup is made
//!   until the closer has closed it;
//! - every fd in the closer's `.received` and `.sent` lanes, until closed.
//!
//! An fd the app takes (`Transport.takeFrameFd`, `Connection.takeFrameFd`)
//! leaves the budget: it is the app's. The transport's own sockets and wake
//! fds never count.
//!
//! ## The limit
//!
//! `limit()` is `RLIMIT_NOFILE / 4` (the soft limit, read at first use; at
//! least `min_limit`, and a soft limit of `RLIM_INFINITY` counts as
//! `max_counted_fd_limit`) until `setLimit` changes it. The macOS default soft
//! limit is 256, so the default budget there is 64.
//!
//! Over it, fd passing degrades and the connections stay up:
//!
//! - A transport keeps a received fd only while it fits (`acquireUpTo`). The
//!   rest go to the closer, with a `.resource_rejection` event
//!   (`resource = .attached_fds`, `err = error.FdBudgetExceeded`); the frame
//!   still dispatches, without them.
//! - A send with fds is refused before any dup is made (`tryAcquire`): the
//!   transport returns `error.FdQueueFull`, with a `.backpressure` event
//!   (`err = error.FdBudgetExceeded`). Sends without fds still go.
//!
//! The fds a reader hands to the closer's `.received` lane as they arrive
//! (`fd_closer.handOff`) are the one thing that may pass the limit: an fd
//! that already arrived has nowhere else to go. While that lane holds
//! `limit()` of them (a close that blocks has stopped it), every AF_UNIX
//! read that finds data closes its connection instead, and read claims
//! (`fd_closer.claimRead`) keep readers that wake together from all
//! passing that check at once: the lane ends at most one read (254 fds)
//! past the limit, however many readers there are (see `fd_closer`). Fds
//! this budget already counts that move to the lane (a frame's after
//! dispatch, a `Peer`'s imports) never trip that stop: the move does not
//! change the count. So the fds counted here stay below twice the limit
//! plus 254: half of `RLIMIT_NOFILE`, plus 254, by default.
//!
//! That is well below `RLIMIT_NOFILE` only from a soft limit of about 1024
//! up (2 * 1024 / 4 + 254 = 766). At the macOS default (256) a single
//! message of 254 fds can take the fd table to its limit on its own, and
//! keep it there for as long as a close in the lane blocks. A process that
//! serves AF_UNIX peers should raise its soft `RLIMIT_NOFILE` to 1024 or
//! more before its first connection; this module never changes it.
//!
//! The count is one atomic integer; every call is safe from any thread.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

/// True where fd passing is compiled in: Linux and Darwin. Elsewhere the
/// budget counts nothing and `limit()` is 0.
pub const supported: bool = builtin.target.os.tag == .linux or builtin.target.os.tag.isDarwin();

/// The smallest limit the default can produce.
pub const min_limit: usize = 16;

/// The default counts at most this many fds of RLIMIT_NOFILE. A soft limit
/// of `RLIM_INFINITY` would otherwise give a budget that never applies.
pub const max_counted_fd_limit: usize = 1 << 20;

/// The default when RLIMIT_NOFILE cannot be read: a quarter of the macOS
/// default soft limit (256).
pub const fallback_limit: usize = 64;

var in_use: std.atomic.Value(usize) = .init(0);
/// 0 until first use.
var limit_value: std.atomic.Value(usize) = .init(0);

/// The limit in force: `defaultLimit()` at first use, or what `setLimit`
/// set since.
pub fn limit() usize {
    if (comptime !supported) return 0;
    const current = limit_value.load(.acquire);
    if (current != 0) return current;
    // First use. A racing first use computes the same value; a racing
    // `setLimit` wins.
    _ = limit_value.cmpxchgStrong(0, defaultLimit(), .acq_rel, .acquire);
    return limit_value.load(.acquire);
}

/// Replace the limit (at least 1) and return the previous one. For tests,
/// and for processes that size their own budget. Fds already counted stay
/// counted: a lower limit only refuses what comes next.
pub fn setLimit(new_limit: usize) usize {
    if (comptime !supported) return 0;
    const previous = limit();
    limit_value.store(@max(new_limit, 1), .release);
    return previous;
}

/// `RLIMIT_NOFILE / 4` right now (the soft limit; see the module doc).
pub fn defaultLimit() usize {
    if (comptime !supported) return 0;
    const limits = posix.getrlimit(.NOFILE) catch return fallback_limit;
    const soft: usize = @intCast(@min(limits.cur, max_counted_fd_limit));
    return @max(soft / 4, min_limit);
}

/// The fds counted right now.
pub fn inUse() usize {
    if (comptime !supported) return 0;
    return in_use.load(.acquire);
}

/// Count `n` more fds if all of them fit under the limit. Returns whether
/// it counted them; on false nothing changed.
pub fn tryAcquire(n: usize) bool {
    if (comptime !supported) return n == 0;
    if (n == 0) return true;
    const cap = limit();
    var current = in_use.load(.monotonic);
    while (true) {
        if (current > cap or n > cap - current) return false;
        current = in_use.cmpxchgWeak(current, current + n, .acq_rel, .monotonic) orelse return true;
    }
}

/// Count as many of `n` more fds as fit under the limit. Returns how many
/// it counted (0 to `n`).
pub fn acquireUpTo(n: usize) usize {
    if (comptime !supported) return 0;
    const cap = limit();
    var current = in_use.load(.monotonic);
    while (true) {
        const room = if (current >= cap) 0 else cap - current;
        const take = @min(n, room);
        if (take == 0) return 0;
        current = in_use.cmpxchgWeak(current, current + take, .acq_rel, .monotonic) orelse return take;
    }
}

/// Count `n` fds whatever the limit: fds that already exist and must be
/// closed (the closer's `.received` lane), or a unit taken back for an fd
/// that stays in the runtime.
pub fn acquire(n: usize) void {
    if (comptime !supported) return;
    _ = in_use.fetchAdd(n, .acq_rel);
}

/// Stop counting `n` fds: closed, or handed to the app. Releasing more than
/// is counted is a bug in the caller: Debug builds assert, and other builds
/// stop at 0 rather than wrap (a wrapped count would refuse every fd from
/// then on).
pub fn release(n: usize) void {
    if (comptime !supported) return;
    var current = in_use.load(.monotonic);
    while (true) {
        if (builtin.mode == .debug) std.debug.assert(current >= n);
        const next = current -| n;
        current = in_use.cmpxchgWeak(current, next, .acq_rel, .monotonic) orelse return;
    }
}
