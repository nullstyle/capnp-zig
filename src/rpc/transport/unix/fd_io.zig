//! Raw reads on an AF_UNIX stream socket that take the file descriptors a
//! peer attaches (SCM_RIGHTS).
//!
//! Experimental. Linux and Darwin only (`supported`); on every other target
//! the calls return `error.UnixSocketsUnsupported` and parse nothing.
//!
//! ## Why the transport reads AF_UNIX sockets this way
//!
//! Any local process that can connect to an AF_UNIX socket can attach open
//! files to the bytes it sends. A receiver that does not ask for them does not
//! make them go away:
//!
//! - macOS installs them in the receiver's fd table anyway. A plain `read`, a
//!   `recvmsg` with no control buffer, and std's `net_read` with an empty
//!   control buffer all do it, and nothing can find those fds to close them.
//!   A control buffer that is too small is no better: macOS installs every
//!   fd, and the receiver sees only the numbers that fit.
//! - Linux closes them, but inside `recvmsg`, on the reading thread. The
//!   final close of a lingering TCP socket blocks that thread for the linger
//!   time.
//!
//! So the transport reads every AF_UNIX connection with `recvWithFds` and a
//! control buffer of `max_fds_per_read` slots, and hands every fd it gets to
//! the closer (`closer`, its `.received` lane). macOS delivers at most one
//! send's fds (254) per `recvmsg`, so 512 slots never truncate there.
//!
//! ## What is left
//!
//! - EMFILE (process fd table full), both kernels: the kernel itself closes
//!   attached fds inside the `recvmsg` that hits it, on the reading thread,
//!   and a close that blocks (a lingering socket) blocks that reader for as
//!   long. Linux installs the fds that fit, sets MSG_CTRUNC and closes the
//!   rest. macOS fails the call with EMFILE (or EMSGSIZE, from XNU's
//!   free-slot check: macOS 26 with a large fd table) and closes all of that
//!   message's fds inside it (measured: 2000 ms for a 2 s linger); the
//!   transport reports
//!   the drop and retries once, and the retry returns the data. A second
//!   EMFILE closes the connection. One message carries up to 254 fds, so at
//!   the macOS default soft limit (256) a single message can reach EMFILE:
//!   raise `RLIMIT_NOFILE` well above 254 plus the fds the process uses.
//! - macOS with a control buffer smaller than one send's fds (only callers of
//!   `recvWithFds` that pass a small buffer; the transport never does): the
//!   fds that did not fit stay installed and nothing can close them. The leak
//!   is the fds sent minus the fds visible.
//! - macOS has no `MSG_CMSG_CLOEXEC`. `recvWithFds` sets FD_CLOEXEC right
//!   after `recvmsg`; a `fork` + `exec` on another thread in between inherits
//!   the fds.
//! - A received fd whose close blocks (a lingering TCP socket, a tty, a FUSE
//!   or NFS file) stops the closer's `.received` lane for as long as it
//!   blocks: on Linux without end while the socket's far end keeps its
//!   window shut, on macOS up to the linger time (near 327 s at most), and a
//!   peer can chain them. The lane keeps its bound (see `closer`): once it is
//!   full, every AF_UNIX read that finds data reads nothing and closes its
//!   connection. A peer that can stall a close can deny service on every
//!   AF_UNIX connection that receives data meanwhile, but cannot fill the
//!   fd table. TCP and QUIC connections, and AF_UNIX socket closes and
//!   shutdowns (the `.socket` lane), do not wait for it.
//! - A transport's own socket: the kernel closes the fds still in flight on
//!   it inside its final close, and on macOS inside `shutdown(SHUT_RD)`.
//!   Linux closes the socket inline when nothing can be in flight (an empty
//!   receive queue after `shutdown(SHUT_RD)`); every other such close, and on
//!   macOS every one plus the read half of every `shutdown`, runs on the
//!   closer's `.socket` lane. A peer that leaves a blocking fd unread on its
//!   own connection when that connection is torn down stops that lane for
//!   as long as the close blocks. The socket closes queued behind it each
//!   hold one fd until then: on Linux only connections torn down with unread
//!   data, on macOS every AF_UNIX connection closed meanwhile. On macOS a
//!   reader whose `shutdown` waits there notices on its 250 ms poll tick.
//! - A listening socket: the kernel closes the fds riding on connections
//!   still in its backlog (nobody accepted them) inside its final close, on
//!   both kernels (measured with a 3 s linger: 3006 ms on Linux, 3001 ms on
//!   macOS; its `shutdown` disposes of nothing). `tcp.Listener.close` closes
//!   an AF_UNIX listener's fd inline but leaves the final close, through a
//!   duplicate, to the `.socket` lane, so it never blocks, and neither does a
//!   `WorkerPool` shutdown. A peer that queues such a connection and is
//!   never accepted stops that lane, as above, once the listener closes. A
//!   caller that closes an AF_UNIX listen fd itself still does that final
//!   close on its own thread.
//! - Older Linux kernels (before 6.8, by our reading of the kernel source)
//!   run the AF_UNIX fd garbage collector inside socket close. There the
//!   final close of a peer's unreachable in-flight fds can happen inside any
//!   AF_UNIX close on the calling thread, the transport's inline one
//!   included.
//! - After `fork`, the child has no closer threads: a transport used in the
//!   child queues fds that nothing closes.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const log = std.log.scoped(.rpc_fd_io);

/// The process-wide threads that close received fds and the transport's
/// own sockets.
pub const closer = @import("fd_closer.zig");

/// True where fd reads are compiled in: Linux and Darwin.
pub const supported: bool = closer.supported;

/// A POSIX file descriptor number.
pub const Fd = closer.Fd;

/// The fd slots of the control buffer the transport reads with: twice the
/// largest count one `sendmsg` can carry (253 on Linux, 254 on macOS).
pub const max_fds_per_read: usize = 512;

/// `recvWithFds` failures. EINTR and EAGAIN never surface: an interrupted
/// call is retried, and a non-blocking socket is polled until readable.
pub const RecvError = error{
    ConnectionResetByPeer,
    ConnectionTimedOut,
    SocketUnconnected,
    SystemResources,
    /// EMFILE (and, on macOS, EMSGSIZE: XNU's free-slot check reports the
    /// same limit that way). On macOS the kernel installed nothing and has
    /// already closed the message's fds inside this failed call; a retry
    /// returns the data.
    ProcessFdQuotaExceeded,
    /// ENFILE: the system-wide fd table is full.
    SystemFdQuotaExceeded,
    Unexpected,
    UnixSocketsUnsupported,
};

/// The outcome of one `recvWithFds`.
pub const Received = struct {
    /// Bytes read into `data`. 0 is end of stream.
    data_len: usize,
    /// Fds written to the front of `fds_out`. The caller owns them.
    fd_count: usize,
    /// The kernel set MSG_CTRUNC: some attached fds did not reach
    /// `fds_out`. See the module doc for what happened to them.
    control_truncated: bool,
};

/// Bytes of control buffer that hold one SCM_RIGHTS message with `fd_count`
/// fds (`CMSG_SPACE`).
pub fn controlSpace(fd_count: usize) usize {
    if (comptime !supported) return 0;
    return std.Io.net.cmsg.space(fd_count * @sizeOf(Fd));
}

const recv_flags: u32 = if (supported and @hasDecl(posix.MSG, "CMSG_CLOEXEC")) posix.MSG.CMSG_CLOEXEC else 0;

/// One `recvmsg` into `data`, taking attached fds into `fds_out`.
///
/// The kernel is offered only as much of `control` as `fds_out` can report
/// (`CMSG_LEN`, not `CMSG_SPACE`: the padding of `CMSG_SPACE` holds one more
/// fd on 64-bit Linux), so every fd it delivers lands in `fds_out`. Received
/// fds are close-on-exec: atomically through `MSG_CMSG_CLOEXEC` on Linux,
/// and through `fcntl` right after the call on macOS.
pub fn recvWithFds(socket: Fd, data: []u8, control: []u8, fds_out: []Fd) RecvError!Received {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    const offered = @min(control.len, controlLen(fds_out.len));
    var iov = [1]posix.iovec{.{ .base = data.ptr, .len = data.len }};
    while (true) {
        var msg: posix.msghdr = .{
            .name = null,
            .namelen = 0,
            .iov = &iov,
            .iovlen = 1,
            .control = if (offered == 0) null else control.ptr,
            .controllen = @intCast(offered),
            .flags = 0,
        };
        const rc = posix.system.recvmsg(socket, &msg, recv_flags);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                const control_len = @min(@as(usize, @intCast(msg.controllen)), offered);
                const count = parseRights(control[0..control_len], fds_out);
                setCloexec(fds_out[0..count]);
                return .{
                    .data_len = syscallCount(rc),
                    .fd_count = count,
                    .control_truncated = msg.flags & posix.MSG.CTRUNC != 0,
                };
            },
            .INTR => continue,
            .AGAIN => {
                try waitReadable(socket);
                continue;
            },
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NOTCONN => return error.SocketUnconnected,
            .NOMEM, .NOBUFS => return error.SystemResources,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            // XNU reports the same fd-table limit as EMSGSIZE when its
            // free-slot check fails before it allocates (macOS 26 does this
            // with a large fd table; macOS 27 returned EMFILE in every
            // layout we tried). This call passes one iovec, so EMSGSIZE has
            // no other cause on a stream socket.
            .MSGSIZE => if (comptime builtin.target.os.tag.isDarwin()) {
                return error.ProcessFdQuotaExceeded;
            } else {
                log.debug("recvmsg failed: errno {d}", .{@backingInt(posix.E.MSGSIZE)});
                return error.Unexpected;
            },
            // Log the number, never the tag: `posix.E` does not name every errno.
            else => |err| {
                log.debug("recvmsg failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// The fds of every SOL_SOCKET/SCM_RIGHTS message in `control`, in native
/// byte order, written to the front of `fds_out`. Returns how many were
/// written; fds beyond `fds_out.len` are not reported (`recvWithFds` never
/// lets the kernel deliver more than `fds_out` holds).
///
/// Each header's `cmsg_len` is clamped to the bytes present. On truncation
/// macOS keeps the full length, longer than the buffer; std's
/// `cmsg.Iterator` then drops the header, and the fds that did fit would stay
/// open with no one to close them.
pub fn parseRights(control: []const u8, fds_out: []Fd) usize {
    if (comptime !supported) return 0;
    const header_size = @sizeOf(posix.cmsghdr);
    const data_offset = std.mem.alignForward(usize, header_size, std.Io.net.cmsg_align);
    var count: usize = 0;
    var offset: usize = 0;
    while (control.len - offset >= header_size) {
        const header = control[offset..];
        const cmsg_len = readHeaderField(header, "len");
        if (cmsg_len < header_size) break;
        const level = readHeaderField(header, "level");
        const kind = readHeaderField(header, "type");
        const usable = @min(cmsg_len, control.len - offset);
        if (level == posix.SOL.SOCKET and kind == posix.SCM.RIGHTS and usable > data_offset) {
            var at = offset + data_offset;
            const end = offset + usable;
            while (at + @sizeOf(Fd) <= end and count < fds_out.len) : (at += @sizeOf(Fd)) {
                fds_out[count] = std.mem.readInt(Fd, control[at..][0..@sizeOf(Fd)], builtin.cpu.arch.endian());
                count += 1;
            }
        }
        // A header whose aligned length runs to (or past) the end is the
        // last one. Checking the aligned step against the bytes left keeps a
        // hostile `cmsg_len` from moving `offset` past `control.len` (an
        // unaligned length can be in range while its aligned step is not).
        if (cmsg_len >= control.len - offset) break;
        const step = std.mem.alignForward(usize, cmsg_len, std.Io.net.cmsg_align);
        if (step >= control.len - offset) break;
        offset += step;
    }
    return count;
}

/// `CMSG_LEN` for `fd_count` fds, clamped so the product cannot overflow a
/// 32-bit `cmsg_len`.
fn controlLen(fd_count: usize) usize {
    const capped = @min(fd_count, 1 << 16);
    return std.mem.alignForward(usize, @sizeOf(posix.cmsghdr), std.Io.net.cmsg_align) + capped * @sizeOf(Fd);
}

/// Read one `cmsghdr` field from bytes that need not be aligned, widened to
/// `usize` (`len`) or `i64` (`level`, `type`).
fn readHeaderField(header: []const u8, comptime name: []const u8) if (std.mem.eql(u8, name, "len")) usize else i64 {
    const Field = @FieldType(posix.cmsghdr, name);
    const at = @offsetOf(posix.cmsghdr, name);
    const value = std.mem.readInt(Field, header[at..][0..@sizeOf(Field)], builtin.cpu.arch.endian());
    return @intCast(value);
}

fn setCloexec(fds: []const Fd) void {
    if (comptime !builtin.target.os.tag.isDarwin()) return;
    for (fds) |fd| {
        _ = std.c.fcntl(fd, posix.F.SETFD, @as(c_int, posix.FD_CLOEXEC));
    }
}

/// Block until `socket` is readable (or hung up). Only reached when a caller
/// handed the transport a non-blocking socket.
fn waitReadable(socket: Fd) RecvError!void {
    var fds = [1]posix.pollfd{.{ .fd = socket, .events = posix.POLL.IN, .revents = 0 }};
    while (true) {
        const rc = posix.system.poll(&fds, 1, -1);
        switch (posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .NOMEM => return error.SystemResources,
            else => |err| {
                log.debug("poll failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// Byte count of a successful syscall. Linux returns `usize`, libc `isize`.
fn syscallCount(rc: anytype) usize {
    return @intCast(rc);
}
