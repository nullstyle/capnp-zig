//! Raw reads on an AF_UNIX stream socket that take the file descriptors a
//! peer attaches (SCM_RIGHTS), and raw sends that attach them.
//!
//! Experimental. Linux and Darwin only (`supported`); on every other target
//! the calls return `error.UnixSocketsUnsupported` and parse nothing.
//!
//! ## Sending fds
//!
//! `sendWithFds` writes a whole message and attaches its fds, as one
//! SCM_RIGHTS control message, to the first `sendmsg` only: the receiver
//! gets them with the first bytes of the message. It never raises SIGPIPE:
//! it passes `MSG_NOSIGNAL`, and on Darwin it also sets `SO_NOSIGPIPE` on
//! the socket first (the documented Darwin way; macOS 27 honors either one
//! alone, measured). One message carries at most `max_fds_per_send` fds
//! (Linux's `SCM_MAX_FD`; macOS takes one more).
//!
//! A blocking `sendmsg` with fds does not always block on macOS: when the
//! path to the peer has less room than the control message (any room below
//! about 1 KiB for 253 fds, measured on macOS 27), XNU fails it with
//! EMSGSIZE at once. `sendWithFds` then waits until the socket is writable
//! and retries, so a busy peer delays the message instead of breaking the
//! connection. Linux blocks as usual.
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
//! So the transport reads every AF_UNIX connection with `tryRecvWithFds` (once
//! `poll` says the socket is readable, and the closer gave the read a claim) and
//! a control buffer of `max_fds_per_read` slots, and hands every fd it gets to
//! the closer (`closer`, its `.received` lane). macOS delivers at most one
//! send's fds (254) per `recvmsg`, so 512 slots never truncate there. With
//! fd passing on (`Transport.enableFdPassing`) it keeps up to
//! `max_fds_per_message` of a message's fds for its consumer instead, and
//! reads one frame at a time so that each fd lands in its own message;
//! every fd it does not hand out still goes to the closer.
//!
//! On Linux 5.15+ a peer can also send a byte out of band (`MSG_OOB`) with
//! fds attached. A normal `recvmsg` skips that message and the kernel frees
//! it, closing its fds inside the read, on the reading thread. So every
//! drain-mode transport sets `SO_OOBINLINE` (`setOobInline`) before its first
//! read: the byte is then read in line and its fds come through the control
//! buffer like any other. If the option cannot be set, every read fails.
//!
//! ## The process fd budget
//!
//! Every fd fd passing keeps alive counts against one process-wide budget
//! (`budget`, `RLIMIT_NOFILE / 4` by default): the fds a transport keeps
//! for a frame, the fds a `Peer` keeps for its imports, the dups of fds the
//! transport sends, and every fd in the closer's queues. Over it, received
//! fds go to the closer (with an event) and fd sends are refused with a
//! backpressure error; the connections stay up. See `fd_budget`.
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
//!   peer can chain them. The fds piling up behind it count against the
//!   process fd budget: first fd passing stops keeping and sending fds in
//!   the whole process, and once the lane holds the budget's limit of fds
//!   that arrived there uncounted (see `closer`), every AF_UNIX read that
//!   finds data reads nothing and closes its connection. Read claims keep
//!   the lane within one read (254 fds) of that limit, however many
//!   readers wake at once. A peer that can stall a close can deny service
//!   on every AF_UNIX connection that receives data meanwhile. It cannot
//!   fill the fd table while `RLIMIT_NOFILE` is 1024 or more; at the macOS
//!   default (256) one message of 254 fds can (see `budget`). TCP and QUIC
//!   connections, and AF_UNIX socket closes and shutdowns (the `.socket`
//!   lane), do not wait for it.
//! - The same holds on the send side for an fd this process sends whose
//!   close blocks, once the app and the receiver have closed their copies:
//!   the transport's dup of it stops the closer's `.sent` lane. Every dup
//!   alive, queued or waiting for its close, counts against the process fd
//!   budget. Once it is reached every transport refuses fd messages with
//!   `error.FdQueueFull` (and no fd is kept on receipt) until the close
//!   ends; messages without fds still go, and the fd table does not fill.
//! - ETOOMANYREFS (Linux): a user may have at most its `RLIMIT_NOFILE` fds
//!   in flight on AF_UNIX sockets (sent, not yet received), across all its
//!   processes; root and `CAP_SYS_RESOURCE` are exempt. A receiver that
//!   stops reading can push the user there. `sendWithFds` then sends
//!   nothing and returns `error.TooManyFdsInFlight`. A transport sending
//!   from its write queue sends that message without its fds instead (the
//!   receiver finds `attachedFd` past the message's fds, which the spec
//!   reads as no fd) and reports it with a backpressure event; the
//!   connection stays up.
//! - A transport's own socket: the kernel closes the fds still in flight on
//!   it inside its final close, and on macOS inside `shutdown(SHUT_RD)`.
//!   Linux closes the socket inline when nothing can be in flight (an empty
//!   receive queue after `shutdown(SHUT_RD)`); every other such close, and on
//!   macOS every one plus the read half of every `shutdown`, runs on the
//!   closer's `.socket` lane. A peer that leaves a blocking fd unread on its
//!   own connection when that connection is torn down stops that lane for
//!   as long as the close blocks. The socket closes queued behind it each
//!   hold one fd until then: on Linux only connections torn down with unread
//!   data, on macOS every AF_UNIX connection closed meanwhile. A listener
//!   from `unix.listen` takes no connection while the lane holds
//!   `closer.socketLaneBound()` jobs, so a peer that reconnects in a loop
//!   waits in the kernel's backlog instead of adding fds. On macOS a reader
//!   whose `shutdown` waits there notices on its 250 ms poll tick, and a
//!   close of the other end of that socket waits while its
//!   `shutdown(SHUT_RD)` is stuck disposing of a lingering fd (measured:
//!   the full linger; a stuck `close` does not hold it, and Linux never
//!   waits), which matters only when both ends are in this process.
//! - A listening socket: the connections still in its accept queue are
//!   released inside its final close, with the fds on their unread
//!   messages. A listener from `unix.listen` does that close on the
//!   `.socket` lane (`SocketFile.closeListenSocket`).
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

/// The process fd budget: one count of the fds fd passing holds (frame fds,
/// imports, sent dups, the closer's queues) and one limit for all of them
/// (`RLIMIT_NOFILE / 4` by default).
pub const budget = @import("fd_budget.zig");

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
    while (true) {
        if (try recvOnce(socket, data, control, fds_out, recv_flags)) |got| return got;
        try waitReadable(socket);
    }
}

/// `recvWithFds` that never waits: one `recvmsg` with `MSG_DONTWAIT`, null
/// when there is nothing to read (EAGAIN). The transport reads this way
/// after `poll` reported the socket readable, so a wakeup with nothing to
/// read (an out-of-band byte skipped, say) sends it back to `poll` and to
/// the closer's check instead of into a `recvmsg` that blocks.
pub fn tryRecvWithFds(socket: Fd, data: []u8, control: []u8, fds_out: []Fd) RecvError!?Received {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    return recvOnce(socket, data, control, fds_out, recv_flags | posix.MSG.DONTWAIT);
}

/// `setOobInline` failures.
pub const OobInlineError = error{
    Unexpected,
    UnixSocketsUnsupported,
};

/// Linux: set `SO_OOBINLINE` on `socket`. Since Linux 5.15 an AF_UNIX
/// stream socket takes `MSG_OOB`: the out-of-band byte, and the fds the
/// peer attached to it, ride on their own message. Without this option a
/// normal `recvmsg` skips that message and the kernel frees it, so its fds
/// are closed inside that `recvmsg`, on the reading thread (measured: a
/// lingering socket sent that way blocked the read for the 3 s linger, in
/// drain mode and with fd passing on). With it the byte is read in line,
/// as stream data, and its fds come through the control buffer like any
/// other. Older kernels keep the fds on the out-of-band message instead,
/// for the socket's final close; in line, they come through the control
/// buffer there too.
///
/// Darwin: nothing to do (it refuses `MSG_OOB` on AF_UNIX sockets with
/// EOPNOTSUPP, measured on macOS 27).
pub fn setOobInline(socket: Fd) OobInlineError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (comptime builtin.target.os.tag != .linux) return;
    const on: c_int = 1;
    const rc = posix.system.setsockopt(socket, posix.SOL.SOCKET, posix.SO.OOBINLINE, std.mem.asBytes(&on), @sizeOf(c_int));
    switch (posix.errno(rc)) {
        .SUCCESS => {},
        // Log the number, never the tag: `posix.E` does not name every errno.
        else => |err| {
            log.debug("setsockopt(SO_OOBINLINE) failed: errno {d}", .{@backingInt(err)});
            return error.Unexpected;
        },
    }
}

/// One `recvmsg` with `flags`; null on EAGAIN. EINTR is retried.
fn recvOnce(socket: Fd, data: []u8, control: []u8, fds_out: []Fd, flags: u32) RecvError!?Received {
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
        const rc = posix.system.recvmsg(socket, &msg, flags);
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
            .AGAIN => return null,
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

/// The most fds one `sendWithFds` attaches: Linux's `SCM_MAX_FD`. macOS
/// takes 254 and fails 255 with EINVAL; both are refused here before the
/// syscall.
pub const max_fds_per_send: usize = 253;

/// `sendWithFds` failures. EINTR and EAGAIN never surface: an interrupted
/// call is retried, and a non-blocking socket is polled until writable. On
/// Darwin neither does the EMSGSIZE that XNU returns, instead of blocking,
/// when the fds do not fit the room left on the path to the peer: the call
/// waits until the socket is writable and retries.
pub const SendError = error{
    /// More than `max_fds_per_send` fds. Nothing was sent.
    TooManyFds,
    /// Fds with no bytes to carry them. A stream socket cannot send fds on
    /// their own (Linux queues nothing for a zero-byte send). Nothing was
    /// sent.
    FdsWithoutData,
    /// EPIPE: the socket is shut down for writing, or the peer closed it.
    BrokenPipe,
    ConnectionResetByPeer,
    SocketUnconnected,
    /// ENOBUFS or ENOMEM; on Darwin also EMSGSIZE that waiting for room
    /// did not cure (a socket buffer too small for the fds).
    SystemResources,
    /// ETOOMANYREFS (Linux): this user already has as many fds in flight on
    /// AF_UNIX sockets as its RLIMIT_NOFILE. Backpressure: the receivers
    /// have not taken them yet. Nothing was sent.
    TooManyFdsInFlight,
    Unexpected,
    UnixSocketsUnsupported,
};

/// `dupCloexec` failures.
pub const DupError = error{
    /// EBADF: `fd` is not an open fd.
    InvalidFd,
    /// EMFILE: the process fd table is full.
    ProcessFdQuotaExceeded,
    Unexpected,
    UnixSocketsUnsupported,
};

const send_flags: u32 = if (supported and @hasDecl(posix.MSG, "NOSIGNAL")) posix.MSG.NOSIGNAL else 0;

/// Bytes of control buffer for one SCM_RIGHTS message of `max_fds_per_send`
/// fds.
const max_send_control_bytes = if (supported) std.Io.net.cmsg.space(max_fds_per_send * @sizeOf(Fd)) else 0;

/// Write all of `bytes` to `socket`, a connected AF_UNIX stream socket,
/// with `fds` attached as one SCM_RIGHTS message to the first `sendmsg`
/// only. Retries partial writes. The caller keeps owning `fds`: the kernel
/// takes its own reference to each file for the message in flight.
///
/// Never raises SIGPIPE (see the module doc). On Darwin this sets
/// `SO_NOSIGPIPE` on `socket`, which stays set. An error after the first
/// `sendmsg` succeeded leaves the message partly sent, with its fds already
/// delivered; the connection is then unusable for framed messages.
pub fn sendWithFds(socket: Fd, bytes: []const u8, fds: []const Fd) SendError!void {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    if (fds.len > max_fds_per_send) return error.TooManyFds;
    if (fds.len != 0 and bytes.len == 0) return error.FdsWithoutData;
    try suppressSigpipe(socket);
    var control_buf: [max_send_control_bytes]u8 align(std.Io.net.cmsg_align) = undefined;
    var control: []const u8 = buildRights(&control_buf, fds);
    var offset: usize = 0;
    while (offset < bytes.len) {
        offset += try sendOnce(socket, bytes[offset..], control);
        // The fds ride on the first chunk only.
        control = &.{};
    }
}

/// `fcntl(F_DUPFD_CLOEXEC)`: a new close-on-exec fd for the file `fd`
/// refers to. The caller owns the result.
pub fn dupCloexec(fd: Fd) DupError!Fd {
    if (comptime !supported) return error.UnixSocketsUnsupported;
    while (true) {
        const rc = if (builtin.target.os.tag == .linux and !builtin.link_libc)
            std.os.linux.fcntl(fd, posix.F.DUPFD_CLOEXEC, 0)
        else
            std.c.fcntl(fd, posix.F.DUPFD_CLOEXEC, @as(c_int, 0));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .BADF => return error.InvalidFd,
            .MFILE => return error.ProcessFdQuotaExceeded,
            else => |err| {
                log.debug("fcntl(F_DUPFD_CLOEXEC) failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// One SCM_RIGHTS message holding `fds`, at the front of `buf`; empty when
/// there are no fds.
fn buildRights(buf: *align(std.Io.net.cmsg_align) [max_send_control_bytes]u8, fds: []const Fd) []const u8 {
    if (fds.len == 0) return &.{};
    const data_len = fds.len * @sizeOf(Fd);
    const total = std.Io.net.cmsg.space(data_len);
    @memset(buf[0..total], 0);
    const header: *align(std.Io.net.cmsg_align) posix.cmsghdr = @ptrCast(buf);
    header.len = @intCast(std.Io.net.cmsg.len(@intCast(data_len)));
    header.level = posix.SOL.SOCKET;
    header.type = posix.SCM.RIGHTS;
    const data_offset = std.mem.alignForward(usize, @sizeOf(posix.cmsghdr), std.Io.net.cmsg_align);
    for (fds, 0..) |fd, i| {
        std.mem.writeInt(Fd, buf[data_offset + i * @sizeOf(Fd) ..][0..@sizeOf(Fd)], fd, builtin.cpu.arch.endian());
    }
    return buf[0..total];
}

/// One `sendmsg` of `bytes`, with `control` attached when it is not empty.
/// Returns the bytes the kernel took.
fn sendOnce(socket: Fd, bytes: []const u8, control: []const u8) SendError!usize {
    var iov = [1]posix.iovec_const{.{ .base = bytes.ptr, .len = bytes.len }};
    const msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iov,
        .iovlen = 1,
        .control = if (control.len == 0) null else control.ptr,
        .controllen = @intCast(control.len),
        .flags = 0,
    };
    var waited_for_room = false;
    while (true) {
        const rc = posix.system.sendmsg(socket, &msg, send_flags);
        switch (posix.errno(rc)) {
            .SUCCESS => return syscallCount(rc),
            .INTR => continue,
            .AGAIN => {
                try waitWritable(socket);
                continue;
            },
            // XNU refuses a control message larger than the room left on
            // the path to the peer with EMSGSIZE, on a blocking socket too,
            // where it does not wait (measured on macOS 27: any room below
            // about the control message's size). The socket is not writable
            // then. Once it is (the low-water mark, 2048 bytes, is more room
            // than 253 fds take) the retry fits; a second EMSGSIZE right
            // after that wait means the fds can never fit.
            .MSGSIZE => if (comptime builtin.target.os.tag.isDarwin()) {
                if (control.len == 0 or waited_for_room) return error.SystemResources;
                waited_for_room = true;
                try waitWritable(socket);
                continue;
            } else {
                log.debug("sendmsg failed: errno {d}", .{@backingInt(posix.E.MSGSIZE)});
                return error.Unexpected;
            },
            .PIPE => return error.BrokenPipe,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NOTCONN => return error.SocketUnconnected,
            .NOMEM, .NOBUFS => return error.SystemResources,
            .TOOMANYREFS => return error.TooManyFdsInFlight,
            // Log the number, never the tag: `posix.E` does not name every errno.
            else => |err| {
                log.debug("sendmsg failed: errno {d}", .{@backingInt(err)});
                return error.Unexpected;
            },
        }
    }
}

/// On Darwin, also set `SO_NOSIGPIPE` on the socket: the documented way
/// there, kept in case an XNU release ignores `MSG_NOSIGNAL`.
fn suppressSigpipe(socket: Fd) SendError!void {
    if (comptime !builtin.target.os.tag.isDarwin()) return;
    const one: c_int = 1;
    const rc = std.c.setsockopt(socket, posix.SOL.SOCKET, posix.SO.NOSIGPIPE, &one, @sizeOf(c_int));
    switch (posix.errno(rc)) {
        .SUCCESS => {},
        // XNU refuses socket options once a socket is shut down both ways;
        // a send there fails with EPIPE (and would raise SIGPIPE).
        .INVAL => return error.BrokenPipe,
        else => |err| {
            log.debug("setsockopt(SO_NOSIGPIPE) failed: errno {d}", .{@backingInt(err)});
            return error.Unexpected;
        },
    }
}

/// Block until `socket` is writable (or hung up). Reached for a
/// non-blocking socket, and on Darwin after EMSGSIZE (see `sendOnce`). A
/// `shutdown(SHUT_WR)` of `socket` makes it writable, so the transport's
/// `shutdown` wakes this wait, and the retry fails with EPIPE.
fn waitWritable(socket: Fd) SendError!void {
    var fds = [1]posix.pollfd{.{ .fd = socket, .events = posix.POLL.OUT, .revents = 0 }};
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
