# HANDOFF — zig fork change branch: AF_UNIX addresses in `std.Io.net`

Paste this into a session working on the nullstyle zig fork. It is
self-contained. It joins the fork's change-branch list (see the other
`handoff-zig-fork-*.md` files beside this one). Suggested branch:
`fix/net-unix-address`.

This is a document, not an upstream issue. Nothing here was filed upstream.

## Why capnp-zig cares

capnp-zig's Unix-socket transport (item 7 of
`docs/sprint-plan-2026-10-04.md`) cannot use std's `UnixAddress` or
`Socket.createPair` today. It uses raw `socket`, `bind`, `listen` and
`socketpair` calls instead, and it rejects abstract names. Every defect below
was reproduced at tagged **0.17.0** on 2026-10-04: macOS 27 (Darwin, arm64)
and Linux 7.0 (aarch64, the local `e2e-cpp-rpc` container). Line numbers are
0.17.0's. Re-locate them on fork HEAD.

## The defects

### 1. `UnixAddress.max_len` is 108 on every POSIX target; Darwin's `sun_path` is 104

`lib/std/Io/net.zig:843-846`:

```zig
pub const max_len = switch (native_os) {
    .windows => std.os.windows.PATH_MAX_WIDE,
    else => 108,
};
```

108 is Linux's `sun_path` size. Darwin's (and the BSDs') is 104. So
`UnixAddress.init` accepts a 105-108-byte path on macOS, and then
`addressUnixToPosix` copies it into the 104-byte `sockaddr.un.path`
(`lib/std/Io/Threaded.zig:14391`):

```zig
@memcpy(storage.un.path[0..path_len], a.path[a.path.len - path_len ..]);
```

In a safe build this panics with index out of bounds inside `listen` or
`connect`, so the process aborts. In a build without safety checks the same
copy would write past the 104-byte array (inferred from the code; not
reproduced here). A path length is ordinary input, so a caller cannot treat
this as a programmer bug.

### 2. Abstract names get an extra trailing NUL (Linux)

`Threaded.zig:14392-14396` appends a NUL whenever the path is shorter than
`sun_path`, and the address length includes it:

```zig
if (storage.un.path.len - path_len > 0) {
    @branchHint(.likely);
    storage.un.path[path_len] = 0;
    path_len += 1;
}
```

That is right for a filesystem path. It is wrong for an abstract name (a path
whose first byte is 0, `UnixAddress.isAbstract`). Linux takes an abstract
name's length from `addrlen`, so the trailing NUL becomes part of the name.
std binds `"\0name\0"`. C, kj (`unix-abstract:`) and Go bind and connect to
`"\0name"`, so they cannot reach a std listener, and std cannot reach theirs.

### 3. `Socket.createPair` cannot ask for AF_UNIX

`net.zig:1250-1251`:

```zig
pub const CreatePairOptions = struct {
    family: IpAddress.Family = .ip4,
```

`IpAddress.Family` has only `ip4` and `ip6`, and `netSocketCreatePair`
(`Threaded.zig:12626`) maps them to `AF_INET` and `AF_INET6`. Neither Linux
nor macOS supports `socketpair` on an IP family. They return EOPNOTSUPP (95
on Linux, 102 on Darwin), which falls to `unexpectedErrno`. So `createPair`
fails on every POSIX target. The one family that `socketpair` supports there,
`AF_UNIX`, cannot be requested.

### 4. Darwin errno 102 (`EOPNOTSUPP`) has no name in `std.c.darwin.E`

The macOS SDK (`usr/include/sys/errno.h:251`) defines, for every UNIX03
compile (the default):

```c
#define EOPNOTSUPP      102             /* Operation not supported on socket */
```

`ENOTSUP` is 45 (`errno.h:144`). The kernel returns 102 for socket
operations. std instead maps `OPNOTSUPP` to 45 (`lib/std/c/darwin.zig:1392`,
"The same code is used for `NOTSUP`"), and `E` has no value for 102. The enum
is non-exhaustive, so 102 is a valid `E` value without a name. Two results:

- On Darwin, every `.OPNOTSUPP =>` arm in std (26 mentions in `Threaded.zig`
  alone) matches 45 (`ENOTSUP`), not the 102 that socket calls return. The
  kernel's 102 falls to `else`.
- Formatting the errno with `{t}` panics with "invalid enum value". capnp-zig
  hit exactly this: its `setTcpNoDelay` logged the errno with `{t}`, and
  `setsockopt(TCP_NODELAY)` on an AF_UNIX socket returns 102 on macOS, so a
  Debug build panicked (item 4 of the sprint plan fixes the capnp-zig side).

### Related: `UnixAddress.listen` and `connect` report a fake IP address

`net.zig:887` and `:915` set `.address = .{ .ip4 = .loopback(0) }` on the
returned socket. A caller that asks the socket for its address gets
`127.0.0.1:0`, not the path. capnp-zig copies this pattern in its own
`Listener.initFd` and documents it. This is a design gap, not a crash, so it
is listed for context only.

## Minimal repro

No dependencies; capnp-zig is not involved. One case per run. Build with
`zig build-exe unixaddr.zig` (macOS) or
`zig build-exe unixaddr.zig -target aarch64-linux-musl` (Linux).

```zig
//! std.Io.net AF_UNIX defects at tagged Zig 0.17.0. One case per run:
//!   unixaddr long-path    (macOS)  a 105..108-byte path passes UnixAddress.init, then panics
//!   unixaddr abstract     (Linux)  std binds an abstract name with an extra trailing NUL
//!   unixaddr create-pair  (both)   Socket.createPair cannot ask for AF_UNIX
//!   unixaddr errno-102    (macOS)  EOPNOTSUPP (102) has no name in std.c.darwin.E
const std = @import("std");
const builtin = @import("builtin");
const net = std.Io.net;
const posix = std.posix;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const case = if (args.len > 1) args[1] else "";
    std.debug.print("os={t} zig={s} case={s}\n", .{ builtin.os.tag, builtin.zig_version_string, case });

    if (std.mem.eql(u8, case, "long-path")) {
        const sun_path_len = @typeInfo(@FieldType(posix.sockaddr.un, "path")).array.len;
        std.debug.print("UnixAddress.max_len={d} sockaddr.un.path.len={d}\n", .{ net.UnixAddress.max_len, sun_path_len });
        var buf: [net.UnixAddress.max_len]u8 = undefined;
        @memset(&buf, 'p');
        @memcpy(buf[0..5], "/tmp/");
        const path = buf[0..@min(sun_path_len + 1, buf.len)];
        const ua = try net.UnixAddress.init(path);
        std.debug.print("UnixAddress.init accepted a {d}-byte path; calling listen\n", .{path.len});
        var server = try ua.listen(io, .{});
        server.deinit(io);
        std.debug.print("listen returned\n", .{});
    } else if (std.mem.eql(u8, case, "abstract")) {
        const name = "capnp-fd0-abstract";
        const ua = try net.UnixAddress.init("\x00" ++ name);
        var server = try ua.listen(io, .{});
        defer server.deinit(io);
        // /proc/net/unix prints each NUL of an abstract name as '@'.
        var file_buf: [64 * 1024]u8 = undefined;
        const listing = try std.Io.Dir.cwd().readFile(io, "/proc/net/unix", &file_buf);
        var lines = std.mem.splitScalar(u8, listing, '\n');
        while (lines.next()) |line| {
            if (std.mem.indexOf(u8, line, name) != null) std.debug.print("/proc/net/unix: ...{s}\n", .{line[@min(line.len, 50)..]});
        }
        // Connect the way C, kj and Go do: addrlen counts the name, no trailing NUL.
        inline for (.{ 0, 1 }) |extra| {
            const linux = std.os.linux;
            const fd: i32 = @intCast(linux.socket(linux.AF.UNIX, linux.SOCK.STREAM, 0));
            defer _ = linux.close(fd);
            var sa: linux.sockaddr.un = .{ .family = linux.AF.UNIX, .path = @splat(0) };
            @memcpy(sa.path[1 .. 1 + name.len], name);
            const len: u32 = @intCast(@offsetOf(linux.sockaddr.un, "path") + 1 + name.len + extra);
            const rc = linux.connect(fd, @ptrCast(&sa), len);
            std.debug.print("raw connect, addrlen counts {s}: {t}\n", .{ if (extra == 0) "the name only (C/kj/Go)" else "the name plus a trailing NUL (std)", linux.errno(rc) });
        }
    } else if (std.mem.eql(u8, case, "create-pair")) {
        std.debug.print("CreatePairOptions.family is {s}\n", .{@typeName(@FieldType(net.Socket.CreatePairOptions, "family"))});
        if (net.Socket.createPair(io, .{})) |pair| {
            std.debug.print("createPair(.{{}}) ok\n", .{});
            pair[0].close(io);
            pair[1].close(io);
        } else |err| std.debug.print("createPair(.{{}}) -> error.{t}\n", .{err});
    } else if (std.mem.eql(u8, case, "errno-102")) {
        var fds: [2]posix.fd_t = undefined;
        if (posix.system.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &fds) != 0) return error.SocketPair;
        const opt = std.mem.toBytes(@as(c_int, 1));
        const rc = posix.system.setsockopt(fds[0], posix.IPPROTO.TCP, posix.TCP.NODELAY, &opt, opt.len);
        const e = posix.errno(rc);
        std.debug.print("setsockopt(IPPROTO_TCP, TCP_NODELAY) on AF_UNIX: rc={d} errno={d} name={?s} E.OPNOTSUPP={d}\n", .{ rc, @intFromEnum(e), std.enums.tagName(posix.E, e), @intFromEnum(posix.E.OPNOTSUPP) });
        std.debug.print("formatting it with {{t}}:\n", .{});
        std.debug.print("{t}\n", .{e});
    } else {
        std.debug.print("usage: unixaddr long-path|abstract|create-pair|errno-102\n", .{});
    }
}
```

## Exact output (2026-10-04, tagged 0.17.0, Debug)

Stack traces are cut after the frames that matter; the cut is marked `[...]`.
Each run's exit status follows it.

`./unixaddr long-path` on macOS:

```
os=macos zig=0.17.0 case=long-path
UnixAddress.max_len=108 sockaddr.un.path.len=104
UnixAddress.init accepted a 105-byte path; calling listen
thread 18146342 panic: index out of bounds: index 105, len 104
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:14391:28: 0x1049b39bf in addressUnixToPosix (unixaddr)
    @memcpy(storage.un.path[0..path_len], a.path[a.path.len - path_len ..]);
                           ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:12096:40: 0x1049b63c3 in netListenUnixPosix (unixaddr)
    const addr_len = addressUnixToPosix(address, &storage);
                                       ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/net.zig:886:54: 0x1049d7a87 in listen (unixaddr)
                .handle = try io.vtable.netListenUnix(io.userdata, ua, options),
                                                     ^
[...]
exit=134
```

`./unixaddr abstract` on Linux (aarch64):

```
os=linux zig=0.17.0 case=abstract
/proc/net/unix: ...01 2698925 @capnp-fd0-abstract@
raw connect, addrlen counts the name only (C/kj/Go): CONNREFUSED
raw connect, addrlen counts the name plus a trailing NUL (std): SUCCESS
```

The trailing `@` in `/proc/net/unix` is the extra NUL.

`./unixaddr create-pair` on macOS, then on Linux:

```
os=macos zig=0.17.0 case=create-pair
CreatePairOptions.family is @typeInfo(Io.net.IpAddress).@"union".tag_type.?
unexpected errno: 102
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/posix.zig:1674:40: 0x100c5b847 in unexpectedErrno (unixaddr)
        std.debug.dumpCurrentStackTrace(.{});
                                       ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:1450:37: 0x100cabd9b in unexpectedErrno (unixaddr)
        return posix.unexpectedErrno(err);
                                    ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Threaded.zig:12678:53: 0x100cab66f in netSocketCreatePair (unixaddr)
        else => |err| return syscall.unexpectedErrno(err),
                                                    ^
[...]
createPair(.{}) -> error.Unexpected
exit=0
```

```
os=linux zig=0.17.0 case=create-pair
CreatePairOptions.family is @typeInfo(Io.net.IpAddress).@"union".tag_type.?
unexpected errno: 95
[...]
createPair(.{}) -> error.Unexpected
```

`./unixaddr errno-102` on macOS:

```
os=macos zig=0.17.0 case=errno-102
setsockopt(IPPROTO_TCP, TCP_NODELAY) on AF_UNIX: rc=-1 errno=102 name=null E.OPNOTSUPP=45
formatting it with {t}:
thread 18145903 panic: invalid enum value
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Writer.zig:1298:83: 0x100ec9dbf in printValue__func_328 (unixaddr)
                .@"enum", .enum_literal, .@"union" => return w.alignBufferOptions(@tagName(value), options),
                                                                                  ^
/Users/nullstyle/.local/share/mise/installs/zig/0.17.0/lib/std/Io/Writer.zig:805:25: 0x100f883af in print__func_911 (unixaddr)
        try w.printValue(
                        ^
[...]
exit=134
```

## The fix

1. **`max_len`.** Derive it from the target's `sockaddr.un`:
   `@typeInfo(@FieldType(posix.sockaddr.un, "path")).array.len` on POSIX
   (104 on Darwin and the BSDs, 108 on Linux). Keep Windows as it is.
   `addressUnixToPosix` already handles a path that fills `sun_path` exactly
   (no NUL), so nothing else changes. If the fork wants a NUL-terminated
   filesystem path on Darwin, make `init` reject `len >= max_len` there
   instead; either way `init` must refuse what `listen` cannot store.
2. **Abstract names.** In `addressUnixToPosix`, append the NUL only when the
   path is not abstract (`a.path.len != 0 and a.path[0] != 0`). The address
   length for an abstract name is `@offsetOf(sockaddr.un, "path") + path.len`.
3. **`createPair`.** Let the caller ask for AF_UNIX. For example, give
   `CreatePairOptions` its own family enum `{ unix, ip4, ip6 }` with
   `.unix` as the POSIX default, and map `.unix` to `posix.AF.UNIX`. The
   pair then works. Its sockets report a fake `127.0.0.1:0`, because
   `addressFromPosix` maps every other family to that (`Threaded.zig:14366`);
   that is the same gap as the "Related" note above.
4. **Darwin errno 102.** Follow the UNIX03 SDK: `NOTSUP = 45` and
   `OPNOTSUPP = 102`. Then re-check every `.OPNOTSUPP` arm in `Threaded.zig`;
   they start to match on Darwin, which is the intent. If renaming 45 is too
   disruptive for one branch, at least add the name for 102 so `{t}` and
   `tagName` stop failing on it.

## Verification

1. The repro: `long-path` prints `listen returned` (or `init` returns
   `error.NameTooLong`); `abstract` prints `SUCCESS` for the name-only connect
   and the `/proc/net/unix` line has no trailing `@`; `create-pair` with
   `.family = .unix` returns a pair; `errno-102` prints a name and does not
   panic.
2. A std test per fix, on the targets where each applies: a Darwin-only
   long-path test, a Linux-only abstract round trip with a raw C-length
   connect, a POSIX `createPair(.unix)` round trip, and a Darwin check that
   `@tagName(@as(E, @enumFromInt(102)))` is `"OPNOTSUPP"`.
3. Optional integration check: capnp-zig's `rpc.transport.unix` (item 7 of
   its sprint plan) could then drop its raw `bind`/`connect` path. That is a
   capnp-zig follow-up, not part of this branch.

## Bookkeeping

Record the branch in the fork's change-branch list with a one-line rationale:
"UnixAddress: per-OS max_len, no NUL on abstract names, createPair can ask
for AF_UNIX, Darwin EOPNOTSUPP is 102". Keep it on the list until upstream
carries each fix.
