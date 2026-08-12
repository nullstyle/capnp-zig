//! Shared primitives for the cross-thread wake doors: the diagnosable spin
//! lock, and the non-blocking setup their file descriptors require.
//!
//! Both transports guard their wake channel with a spin lock held across a
//! tiny critical section: the fd read+write in `wake()`/`request()`, and the
//! null+close in `deinit()`. It is a spin rather than an `Io.Mutex` because
//! those doors may be called from a thread that owns no io task.
//!
//! This module exists because the bare form said nothing when it went wrong:
//!
//!     while (!self.mu.tryLock()) std.atomic.spinLoopHint();
//!
//! A thread stuck there spins forever at 100% CPU. It cannot report that it is
//! stuck, which lock it wants, or who holds it — so the failure reaches CI as
//! a job that prints its last line and goes silent until the step cap kills
//! it, naming no test. Recovering "which lock, held by whom" from that costs a
//! container, a stack dump, and a lot of guessing.
//!
//! `Lock` keeps the same fast path and adds two things in Debug (or in release
//! when the owner opts in, matching the transports' `runtime_thread_checks`
//! convention): it records the holding thread, and it bounds the spin. Past
//! the bound it panics naming the site, the holder and the waiter — turning an
//! unbounded hang into an ordinary crash with a stack trace, at the moment and
//! place the invariant actually broke.
//!
//! ## Why an iteration bound rather than a deadline
//!
//! A wall-clock deadline would read better in the panic, but there is no
//! portable monotonic clock reachable from here: `std.time` carries only
//! constants, and `std.Io.Timestamp` needs the `Io` this lock deliberately
//! does not have. `clock_gettime` is available on Linux only. An iteration
//! bound needs nothing, works identically on every target, and is just as good
//! a stuck-signal.
//!
//! The bound is deliberately far above any legitimate contention. These
//! critical sections are a handful of syscalls, so even a heavily
//! oversubscribed machine clears them in well under a million spins; the bound
//! is two orders of magnitude beyond that. Reaching it means the holder is
//! blocked on something inside the critical section that it should not be
//! blocked on, which is a bug whether or not it would eventually resolve.

const std = @import("std");
const builtin = @import("builtin");

pub const Lock = struct {
    mu: std.atomic.Mutex = .unlocked,
    /// Thread currently holding the lock, or 0 when free. Diagnostic only —
    /// never consulted for correctness, only to name a holder in a panic.
    /// Stored as u64 rather than `std.Thread.Id` so the rendered api-snapshot
    /// stays platform-stable (`Thread.Id` is u32 on Linux/Windows, u64 on
    /// macOS).
    owner_thread_id: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Spins tolerated before the wait is declared stuck. See the module doc
    /// for why this is a count and not a duration.
    pub const stall_limit_spins: u64 = 1 << 27;

    /// Checks are on in Debug always; release builds opt in.
    pub fn diagnosticsEnabled(runtime_checks: bool) bool {
        return builtin.mode == .debug or runtime_checks;
    }

    pub fn acquire(self: *Lock, comptime site: []const u8, runtime_checks: bool) void {
        if (self.mu.tryLock()) {
            self.noteOwner(runtime_checks);
            return;
        }
        if (!diagnosticsEnabled(runtime_checks)) {
            while (!self.mu.tryLock()) std.atomic.spinLoopHint();
            return;
        }
        self.acquireDiagnosed(site);
        self.noteOwner(true);
    }

    /// Contended path with the bound. Split out so the uncontended acquire
    /// stays a single `tryLock`.
    fn acquireDiagnosed(self: *Lock, comptime site: []const u8) void {
        var spins: u64 = 0;
        while (!self.mu.tryLock()) {
            std.atomic.spinLoopHint();
            spins += 1;
            if (spins < stall_limit_spins) continue;
            const holder = self.owner_thread_id.load(.acquire);
            const waiter: u64 = std.Thread.getCurrentId();
            std.debug.panic( // stuck-wake-lock diagnostic
                "wake lock '" ++ site ++ "' still contended after {d} spins: held by thread {d}, " ++
                    "waiter is thread {d}. The holder is stuck inside the critical section; the usual " ++
                    "cause is a blocking syscall on the wake channel while the only thread that drains " ++
                    "that channel is itself waiting for this lock.",
                .{ spins, holder, waiter },
            );
        }
    }

    fn noteOwner(self: *Lock, runtime_checks: bool) void {
        if (!diagnosticsEnabled(runtime_checks)) return;
        self.owner_thread_id.store(std.Thread.getCurrentId(), .release);
    }

    pub fn release(self: *Lock, runtime_checks: bool) void {
        if (diagnosticsEnabled(runtime_checks)) {
            self.owner_thread_id.store(0, .release);
        }
        self.mu.unlock();
    }
};

/// Put a wake-channel descriptor in non-blocking mode.
///
/// Load-bearing, not hygiene. Both transports hold the wake lock across the
/// write in `wake()`/`request()`, and the only thread that drains the channel
/// is the loop thread — which also needs that lock to close the descriptors in
/// `deinit()`. If the write can block, a full wake pipe deadlocks the pair:
/// the writer sleeps in `sendmsg` holding the lock, and the drainer spins for
/// a lock it will never get.
///
/// That is not hypothetical. TCP created its socketpair without this and hung
/// exactly that way on Linux — captured with three threads: one in state S
/// inside `sendmsg` under `Connection.wake`, and two spinning in `lockWake`,
/// one of them `deinitNow`. QUIC set it and did not hang. The helper is shared
/// so the two doors cannot drift apart again.
///
/// EAGAIN from a wake write is success, not failure: it means unread wake
/// bytes are already queued, which is precisely the state a wake is trying to
/// produce.
pub fn setNonBlocking(fd: std.posix.fd_t) void {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
    const rc = std.posix.system.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
    if (std.posix.errno(rc) != .SUCCESS) return;
    var flags: usize = @intCast(rc);
    flags |= @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"));
    _ = std.posix.system.fcntl(fd, std.posix.F.SETFL, flags);
}

/// Poke a wake channel: write one byte, raw.
///
/// Deliberately NOT `io.vtable.netWrite`. `Io.Threaded.netWritePosix` treats
/// EAGAIN as `errnoBug` and PANICS on it in Debug, because it assumes a
/// blocking descriptor and manages concurrency itself. That assumption is
/// incompatible with the non-blocking channel `setNonBlocking` establishes —
/// and the channel must be non-blocking, or a full pipe deadlocks against the
/// lock. Going through the Io layer to deliver one byte also buys nothing
/// here: `wake()` may be called from a thread that owns no io task, which is
/// the same reason the lock is a spin.
///
/// Classified errno-first, not by the sign of the return value: on Linux
/// `std.posix.system.write` is the raw syscall returning `usize`, so a failure
/// arrives as -errno reinterpreted as a huge POSITIVE. Testing `rc > 0` first
/// silently reports every failed wake as delivered.
pub fn writeByte(fd: std.posix.fd_t) void {
    if (comptime builtin.target.os.tag == .freestanding or builtin.target.os.tag == .windows) return;
    const byte = [_]u8{1};
    while (true) {
        const rc = std.posix.system.write(fd, &byte, byte.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            // AGAIN means unread wake bytes are already queued, which is
            // exactly the state a wake is trying to produce: success for our
            // purposes. Anything else is unrecoverable and must not spin.
            else => return,
        }
    }
}
