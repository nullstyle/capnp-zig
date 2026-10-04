# HANDOFF — zig fork change branch: SCM_RIGHTS in `std.Io.net`

Paste this into a session working on the nullstyle zig fork. It is
self-contained. It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files beside this one). Suggested branch:
`fix/net-scm-rights`.

This is a document, not an upstream issue. Nothing here was filed upstream.

## Why capnp-zig cares

capnp-zig is adding fd passing over AF_UNIX (items 6 and 10-13 of
`docs/sprint-plan-2026-10-04.md`). Received fds are untrusted input from the
peer. Today std's control-message path cannot be used safely for them, so
capnp-zig's `fd_io` calls `recvmsg` and `sendmsg` itself and parses the
control buffer with its own clamped parser. capnp-zig's FD-0 suite
(`tests/rpc/transport/unix/unix_kernel_semantics_test.zig`) pins the kernel
behaviour that the fixes below rely on.

Every defect was reproduced at tagged **0.17.0** on 2026-10-04: macOS 27
(Darwin, arm64) and Linux 7.0 (aarch64, the local `e2e-cpp-rpc` container).
Line numbers are 0.17.0's. Re-locate them on fork HEAD.

## The defects

### 1. `cmsg.Iterator` drops a truncated header, so the fds in it leak

`lib/std/Io/net.zig:1536-1546`:

```zig
pub fn next(it: *Iterator) ?Message {
    if (it.control.len < @sizeOf(cmsghdr)) return null;
    const header: *align(cmsg_align) cmsghdr = @ptrCast(it.control.ptr);
    if (it.control.len < header.len) return null;   // :1539
    ...
```

When the control buffer is too small, the kernel sets `MSG_CTRUNC`. Linux
shrinks `cmsg_len` to the bytes it copied. Darwin does not: it leaves
`cmsg_len` at the full length, longer than the buffer, and it still installs
**every** fd in the receiver's table, including the ones whose numbers did not
fit. So on Darwin the iterator returns null for the header, the caller never
sees even the fd numbers that did fit, and every passed fd leaks. A peer that
sends more fds than the receiver expected causes this.

A related hazard: `cmsg.data(header)` (`net.zig:1522-1525`) slices
`header.len` bytes from a many-pointer. On a truncated Darwin header that
reads past the control buffer with no safety check.

### 2. `recvmsg` maps EMFILE to `unexpectedErrno`

`lib/std/Io/Threaded.zig:12939-12983` (`netReadPosix`, the `recvmsg` path) has
no arm for `.MFILE` or `.NFILE`, so they fall to
`else => |err| return posix.unexpectedErrno(err)` (`:12983`). That returns
`error.Unexpected` and, with unexpected-error tracing on (the Debug default),
prints the errno and a stack trace.

EMFILE is an ordinary result on Darwin: when the receiver is at
RLIMIT_NOFILE, `recvmsg` fails with EMFILE, installs nothing and leaves the
data queued. A retry returns the data with `controllen = 0` and no
`MSG_CTRUNC`; the kernel has dropped the fds. (Linux instead delivers the fds
that fit and sets `MSG_CTRUNC`.) A caller needs a named error to take the
retry path; `error.Unexpected` gives it nothing to match.

### 3. `sendmsg` maps EBADF and EINVAL to `errnoBug`

`Threaded.zig:13445` and `:13449` (`netWritePosix`):

```zig
.BADF => |err| return errnoBug(err), // File descriptor used after closed.
.INVAL => |err| return errnoBug(err), // Invalid argument passed.
```

`errnoBug` panics in Debug builds (`Threaded.zig:14442-14445`) and returns
`error.Unexpected` otherwise. Both errnos are ordinary when the control
buffer carries `SCM_RIGHTS`:

- **EBADF:** one of the fds in the control message is not open. The socket
  itself is fine. The fd list is the caller's data.
- **EINVAL:** too many fds in one `sendmsg` (more than 253 on Linux, more than
  254 on Darwin), among other control-message errors.

So a Debug build aborts the process on a recoverable condition, and a
release build cannot tell it from a real bug.

### 4. Received fds are close-on-exec on Linux only

`netReadPosix` passes `MSG_CMSG_CLOEXEC` where the platform defines it
(`Threaded.zig:12949-12950`). Darwin does not define it, and std does not set
`FD_CLOEXEC` afterwards. So the same `net_read` call returns CLOEXEC fds on
Linux and inheritable fds on Darwin. FD-0 pins both halves. This is a
consistency gap rather than a crash.

## Minimal repro

No dependencies; capnp-zig is not involved. It drives std's own `net_read`
and `net_write` operations with a control buffer. One case per run. Build
with `zig build-exe scmrights.zig` (macOS) or
`zig build-exe scmrights.zig -target aarch64-linux-musl` (Linux).

```zig
//! std SCM_RIGHTS defects at tagged Zig 0.17.0, driven through std's own
//! `net_read` / `net_write` operations with a control buffer. One case per run:
//!   scmrights iterator-truncated  (macOS)  cmsg.Iterator drops a truncated header
//!   scmrights recv-emfile         (macOS)  recvmsg EMFILE -> unexpectedErrno
//!   scmrights send-einval         (both)   one fd over the per-sendmsg limit -> errnoBug panic
//!   scmrights send-ebadf          (both)   a closed fd in SCM_RIGHTS -> errnoBug panic
const std = @import("std");
const builtin = @import("builtin");
const net = std.Io.net;
const cmsg = net.cmsg;
const A = net.cmsg_align;
const posix = std.posix;
const sys = posix.system;
const fd_t = posix.fd_t;

fn ok(rc: anytype) bool {
    return posix.errno(rc) == .SUCCESS;
}

fn isOpen(fd: fd_t) bool {
    const rc = if (builtin.os.tag == .linux and !builtin.link_libc) sys.fcntl(fd, posix.F.GETFD, 0) else sys.fcntl(fd, posix.F.GETFD);
    return ok(rc);
}

fn openCount() usize {
    var n: usize = 0;
    var fd: fd_t = 0;
    while (fd < 1024) : (fd += 1) {
        if (isOpen(fd)) n += 1;
    }
    return n;
}

fn highestOpen() fd_t {
    var hi: fd_t = 0;
    var fd: fd_t = 0;
    while (fd < 1024) : (fd += 1) {
        if (isOpen(fd)) hi = fd;
    }
    return hi;
}

fn rights(buf: []align(A) u8, fds: []const fd_t) []const u8 {
    const dl = fds.len * @sizeOf(fd_t);
    const total = cmsg.space(dl);
    @memset(buf[0..total], 0);
    const hdr: *align(A) posix.cmsghdr = @ptrCast(buf.ptr);
    hdr.len = @intCast(cmsg.len(@intCast(dl)));
    hdr.level = posix.SOL.SOCKET;
    hdr.type = posix.SCM.RIGHTS;
    @memcpy(cmsg.data(hdr)[0..dl], std.mem.sliceAsBytes(fds));
    return buf[0..total];
}

fn stdSend(io: std.Io, s: fd_t, bytes: []const u8, control: []const u8) !usize {
    const data: [1][]const u8 = .{bytes};
    return try (try io.operate(.{ .net_write = .{ .socket_handle = s, .data = &data, .control = control } })).net_write;
}

fn stdRecv(io: std.Io, s: fd_t, buf: []u8, control: []u8) !net.Stream.ReadResult {
    var bufs: [1][]u8 = .{buf};
    return try (try io.operate(.{ .net_read = .{ .socket_handle = s, .data = &bufs, .control = control } })).net_read;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const case = if (args.len > 1) args[1] else "";
    std.debug.print("os={t} zig={s} case={s}\n", .{ builtin.os.tag, builtin.zig_version_string, case });

    var sp: [2]fd_t = undefined;
    if (!ok(sys.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &sp))) return error.SocketPair;
    var p: [2]fd_t = undefined;
    if (!ok(sys.pipe(&p))) return error.Pipe;
    var ctl_buf: [cmsg.space(300 * @sizeOf(fd_t))]u8 align(A) = undefined;

    if (std.mem.eql(u8, case, "iterator-truncated")) {
        const before = openCount();
        _ = try stdSend(io, sp[0], "Z", rights(&ctl_buf, &.{ p[1], p[1], p[1] }));
        var data: [8]u8 = undefined;
        var control: [cmsg.space(@sizeOf(fd_t))]u8 align(A) = undefined; // room for one fd
        const r = try stdRecv(io, sp[1], &data, &control);
        const hdr: *align(A) posix.cmsghdr = @ptrCast(&control);
        std.debug.print("sent 3 fds; net_read: data_len={d} control_len={d} control_truncated={} cmsg_len={d}\n", .{ r.data_len, r.control_len, r.control_truncated, hdr.len });
        var it: cmsg.Iterator = .{ .control = control[0..r.control_len] };
        var messages: usize = 0;
        while (it.next()) |m| : (messages += 1) {
            const fds = std.mem.bytesAsSlice(fd_t, m.data);
            std.debug.print("  Iterator message: {d} fd(s)\n", .{fds.len});
            for (fds) |fd| _ = sys.close(fd);
        }
        std.debug.print("cmsg.Iterator yielded {d} message(s); fd table grew by {d} after closing what it yielded\n", .{ messages, openCount() - before });
    } else if (std.mem.eql(u8, case, "recv-emfile")) {
        _ = try stdSend(io, sp[0], "ABCD", rights(&ctl_buf, &.{ p[1], p[1], p[1] }));
        // Leave exactly two free slots below the soft limit.
        var rl = try posix.getrlimit(.NOFILE);
        rl.cur = @intCast(highestOpen() + 1 + 2);
        try posix.setrlimit(.NOFILE, rl);
        var fillers: usize = 0;
        while (fillers < 64) : (fillers += 1) {
            if (!ok(sys.dup(sp[0]))) break;
        }
        _ = sys.close(highestOpen());
        _ = sys.close(highestOpen());
        var data: [8]u8 = undefined;
        var control: [cmsg.space(16 * @sizeOf(fd_t))]u8 align(A) = undefined;
        if (stdRecv(io, sp[1], &data, &control)) |r| {
            std.debug.print("net_read ok: data_len={d} control_len={d} control_truncated={}\n", .{ r.data_len, r.control_len, r.control_truncated });
        } else |err| std.debug.print("net_read -> error.{t}\n", .{err});
    } else if (std.mem.eql(u8, case, "send-einval")) {
        const limit: usize = if (builtin.os.tag == .linux) 253 else 254;
        var many: [300]fd_t = undefined;
        @memset(&many, p[1]);
        std.debug.print("net_write with {d} fds in one SCM_RIGHTS cmsg (kernel limit {d}):\n", .{ limit + 1, limit });
        const n = stdSend(io, sp[0], "M", rights(&ctl_buf, many[0 .. limit + 1]));
        std.debug.print("net_write returned {any}\n", .{n});
    } else if (std.mem.eql(u8, case, "send-ebadf")) {
        const stale = p[1];
        _ = sys.close(stale);
        std.debug.print("net_write with an already-closed fd ({d}) in SCM_RIGHTS:\n", .{stale});
        const n = stdSend(io, sp[0], "M", rights(&ctl_buf, &.{stale}));
        std.debug.print("net_write returned {any}\n", .{n});
    } else {
        std.debug.print("usage: scmrights iterator-truncated|recv-emfile|send-einval|send-ebadf\n", .{});
    }
}
```

## Exact output (2026-10-04, tagged 0.17.0, Debug)

Stack traces are cut after the frames that matter; the cut is marked `[...]`.

`./scmrights iterator-truncated` on macOS, then on Linux for contrast:

```
os=macos zig=0.17.0 case=iterator-truncated
sent 3 fds; net_read: data_len=1 control_len=16 control_truncated=true cmsg_len=24
cmsg.Iterator yielded 0 message(s); fd table grew by 3 after closing what it yielded
exit=0
```

```
os=linux zig=0.17.0 case=iterator-truncated
sent 3 fds; net_read: data_len=1 control_len=24 control_truncated=true cmsg_len=24
  Iterator message: 2 fd(s)
cmsg.Iterator yielded 1 message(s); fd table grew by 0 after closing what it yielded
```

On macOS one fd number fit in the buffer, but the iterator yielded nothing,
and all three fds stayed installed.

`./scmrights recv-emfile` on macOS, then on Linux for contrast:

```
os=macos zig=0.17.0 case=recv-emfile
unexpected errno: 24
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/posix.zig:1674:40: 0x1047eb847 in unexpectedErrno (scmrights)
        std.debug.dumpCurrentStackTrace(.{});
                                       ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:12983:63: 0x10488348f in netReadPosix (scmrights)
                    else => |err| return posix.unexpectedErrno(err),
                                                              ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:12890:24: 0x10488299f in netRead (scmrights)
    return netReadPosix(socket_handle, data, control);
                       ^
[...]
net_read -> error.Unexpected
exit=0
```

```
os=linux zig=0.17.0 case=recv-emfile
net_read ok: data_len=4 control_len=24 control_truncated=true
```

`./scmrights send-einval` on macOS (255 fds) and on Linux (254 fds):

```
os=macos zig=0.17.0 case=send-einval
net_write with 255 fds in one SCM_RIGHTS cmsg (kernel limit 254):
thread 18154312 panic: programmer bug caused syscall error: INVAL
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:14443:34: 0x1022541bb in errnoBug (scmrights)
    if (is_debug) std.debug.panic("programmer bug caused syscall error: {t}", .{err});
                                 ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:13449:52: 0x10229a2cb in netWritePosix (scmrights)
                    .INVAL => |err| return errnoBug(err), // Invalid argument passed.
                                                   ^
[...]
exit=134
```

```
os=linux zig=0.17.0 case=send-einval
net_write with 254 fds in one SCM_RIGHTS cmsg (kernel limit 253):
thread 1 panic: programmer bug caused syscall error: INVAL
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:14443:34: 0x1108097 in errnoBug (scmrights)
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:13449:52: 0x1143b5b in netWritePosix (scmrights)
[...]
```

`./scmrights send-ebadf` on macOS and on Linux:

```
os=macos zig=0.17.0 case=send-ebadf
net_write with an already-closed fd (6) in SCM_RIGHTS:
thread 18154324 panic: programmer bug caused syscall error: BADF
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:14443:34: 0x1006941bb in errnoBug (scmrights)
    if (is_debug) std.debug.panic("programmer bug caused syscall error: {t}", .{err});
                                 ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:13445:51: 0x1006da217 in netWritePosix (scmrights)
                    .BADF => |err| return errnoBug(err), // File descriptor used after closed.
                                                  ^
[...]
exit=134
```

```
os=linux zig=0.17.0 case=send-ebadf
net_write with an already-closed fd (6) in SCM_RIGHTS:
thread 1 panic: programmer bug caused syscall error: BADF
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:14443:34: 0x1108097 in errnoBug (scmrights)
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:13445:51: 0x1143ac3 in netWritePosix (scmrights)
[...]
```

## The fix

1. **Iterator.** Clamp instead of dropping. When `header.len` runs past the
   buffer, yield one last message whose `data` is the bytes that are present
   (from the end of the header to the end of the buffer), then stop. Add a
   `truncated: bool` to `Message` so a caller can tell. Make `cmsg.data`
   take the buffer end into account too, or document that it must not be
   called on a header that the iterator did not yield. This is the parser
   kj uses, and the one capnp-zig's `fd_io` implements.
2. **`recvmsg` EMFILE.** Map `.MFILE` to `error.ProcessFdQuotaExceeded` and
   `.NFILE` to `error.SystemFdQuotaExceeded`, and add both to
   `NetRead.Error`. Document the Darwin behaviour: nothing was installed, the
   data is still queued, and a retry returns it without the fds.
3. **`sendmsg` EBADF and EINVAL.** When `control.len > 0`, return named
   errors instead of `errnoBug`: for example `error.BadControlFileDescriptor`
   for EBADF and `error.ControlMessageInvalid` for EINVAL (too many fds is
   the common cause). Keep `errnoBug` when there is no control data, where
   the existing comments are right.
4. **CLOEXEC (optional).** On targets without `MSG_CMSG_CLOEXEC`, set
   `FD_CLOEXEC` on each received `SCM_RIGHTS` fd right after `recvmsg`, and
   document the race window. Or document that `net_read` returns inheritable
   fds there.

## Verification

1. The repro: `iterator-truncated` on macOS yields one message with one fd
   and `truncated = true`, and the fd table grows by 2 (the fds the kernel
   installed but could not report; nothing can fix that part); `recv-emfile`
   returns `error.ProcessFdQuotaExceeded`; `send-einval` and `send-ebadf`
   return named errors in Debug and do not panic.
2. std tests: a truncated-buffer Iterator test fed with fixed bytes (a
   Darwin-shaped header whose `cmsg_len` exceeds the buffer), so it runs on
   every host; a POSIX `net_write` test with a closed fd and with too many
   fds; a Darwin `net_read` test at a lowered RLIMIT_NOFILE.
3. Optional integration check: capnp-zig's FD-0 suite still passes against
   the fork (it calls the kernel directly, so it should not change), and
   `fd_io` could then move to std's iterator. That is a capnp-zig follow-up,
   not part of this branch.

## Bookkeeping

Record the branch in the fork's change-branch list with a one-line rationale:
"SCM_RIGHTS: clamp truncated cmsg headers, name recvmsg EMFILE, sendmsg
EBADF/EINVAL are not programmer bugs when control data is present". Keep it on
the list until upstream carries each fix.
