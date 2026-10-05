# Cap'n Proto RPC over Unix-domain sockets

**Status: Experimental, Linux and macOS.** On every other target the calls
return `error.UnixSocketsUnsupported` (see [Windows and other
targets](#windows-and-other-targets)). Nothing on this page is part of the
frozen Stable surface (`docs/api-snapshot.txt`).

`capnpc.rpc.transport.unix` runs the normal RPC stack over a socket file:

- `unix.listen` returns a `tcp.Listener`. `ServerSession.accept` and
  `Listener.accept` serve it as they serve a TCP listener.
- `unix.connect` returns a `*tcp.ClientSession`, wired as `tcp.connect` wires
  one.
- Every AF_UNIX connection closes the file descriptors that a peer attaches
  to its messages ([drain mode](#fds-a-peer-attaches-drain-mode)), unless you
  turn on [fd passing](#fd-passing).

This page gives the rules for the socket file, the fd-passing contract, the
platform differences, and the [threat table](#threat-table). The module docs
in `src/rpc/transport/unix/` (`socket.zig`, `fd_io.zig`, `fd_closer.zig`,
`fd_budget.zig`) and `src/rpc/peer/peer_fds.zig` give the details.

Examples:

- `zig build example-rpc-unix` runs the ping-pong example over a socket file
  (`examples/rpc_pingpong_unix.zig`).
- `zig build example-rpc-fd` passes the write end of a pipe from the server
  to the client (`examples/rpc_fd_passing.zig`). The run fails unless the fd
  arrives, works, and every copy of it is closed at the end.

Every code block on this page is a region of
`tests/docs/rpc_unix_snippets_test.zig`, which `zig build test-docs-snippets`
compiles and runs.

## Quick start

The server. `path` names a socket file in a private directory (see [Who can
connect](#who-can-connect)):

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
var listener = try rpc.transport.unix.listen(gpa, io, path, .{
    .socket_mode = 0o600, // the default: only this user can connect
    .reclaim_stale = false, // the default: a file left at `path` is AddressInUse
});
defer listener.close(); // removes the socket file if it is still ours
// getAddress() means nothing for a socket file; unixPath() is `path`.
const bound: []const u8 = listener.unixPath().?;
```

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
fn serveOne(gpa: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server) !void {
    // ServerSession.accept takes a Unix listener as it takes a TCP one.
    var session = try rpc.transport.tcp.ServerSession.accept(gpa, listener, .{});
    defer session.deinit();
    _ = try PingPong.setBootstrap(&session.peer, server);
    session.run();
}
```

The client:

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
fn connectAndPing(gpa: std.mem.Allocator, io: std.Io, path: []const u8, state: *ClientState) !void {
    const session = try rpc.transport.unix.connect(gpa, io, path, .{
        .connect_timeout_ms = 5_000, // Linux waits while the backlog is full
    });
    defer session.deinit();
    _ = try PingPong.Client.fromBootstrap(&session.peer, state, onBootstrap);
    session.run();
}
```

## The socket file

### Path rules

- **Length.** `path.len` must be shorter than `sun_path`: 104 bytes on macOS,
  108 on Linux. A longer path returns `error.NameTooLong`. (`unix.listen`
  does not use std's `UnixAddress`, which panics on 105-108-byte paths on
  macOS.)
- **Paths only.** A leading NUL byte (a Linux abstract name) returns
  `error.AbstractNameUnsupported`. Abstract names have no permission model.
  An empty path, or one with a NUL inside it, returns `error.BadPathName`.
- **Relative paths** resolve against the current directory at each call. After
  a `chdir`, `Listener.close` finds another file (or none) and leaves the
  socket file in place. Use absolute paths.

### Who can connect

There is no peer-credential check (`SO_PEERCRED`, `getpeereid`) in this
version. The file system is the only access control, so set it up with care:

- **Use a private directory.** Put the socket in a directory that you own,
  with mode 0700 (for example `$XDG_RUNTIME_DIR` on Linux, or a directory you
  create with mode 0700). To connect, a process needs search permission on
  every directory in the path. A private directory keeps every other user
  out, and nobody else can create, replace or rename files in it. Never put
  the socket directly in a directory that other users can write, such as
  `/tmp`: there another user can plant a file or a link at the path, or swap
  the socket file while `listen` sets its mode.
- **`socket_mode`** (default 0o600) is set after `bind` and before `listen`.
  Until `listen`, every connect is refused, so no client can connect while the
  file still has the mode the umask gave it. Bits outside 0o777 return
  `error.InvalidSocketMode`. Linux checks write permission on the socket file
  at connect. Do not rely on the file mode alone: older BSD-derived kernels
  did not check it, and the directory check works everywhere.
- **The umask** decides the first mode of the socket file and the mode of the
  `.lock` file (created 0600, less the umask). It cannot widen either one. It
  does not decide who can connect, because `socket_mode` replaces the mode
  before `listen`. Create the private directory with an explicit mode 0700;
  do not rely on the umask for it.
- **Sharing with a group.** Use a directory that you own, with that group as
  its group and mode 0750 (not group-writable), and `socket_mode = 0o660`.
- **Swap check.** Right after `bind`, `listen` records the socket file's
  (dev, ino). After the `chmod` it checks that the path still names that file,
  and returns `error.SocketPathChanged` if not. `chmod` follows a symbolic
  link, so this check only narrows the window in which another user who can
  write the directory swaps the path. The private directory closes it.

### The lock file

- `listen` opens `<path>.lock` (`O_NOFOLLOW`, `O_NONBLOCK`, close-on-exec,
  mode 0600) and takes `flock(LOCK_EX | LOCK_NB)` before it touches the
  socket file. It holds the lock until `Listener.close`.
- If another listener holds the lock, `listen` returns `error.AddressInUse`.
- A symbolic link at `<path>.lock` returns `error.SymLinkLoop`, and `listen`
  creates nothing where it points. A FIFO there cannot hang `listen`.
- The lock file is never removed. Removing it would let two servers lock two
  different files.
- The lock protects only servers that take it: build every server on a path
  with `unix.listen`.
- A child that forks without `exec` shares the lock fd and keeps the path
  locked until it exits.

### Stale socket files

A server that dies leaves its socket file behind.

- `reclaim_stale = false` (the default): `bind` fails, and `listen` returns
  `error.AddressInUse`.
- `reclaim_stale = true`: `listen` holds the lock, so no live server built
  with `listen` owns the path. It removes the file if it is a socket, then
  binds. A file of any other type stays, and `listen` returns
  `error.AddressInUse`.
- `listen` never probes the path with a connect. A server between its `bind`
  and its `listen` also refuses connects, so a refused connect does not prove
  that the file is stale.

### Closing and the address

- `Listener.close` removes the path only while it still names this listener's
  socket file (same dev and ino). Then it closes the socket and releases the
  lock, last. It is idempotent, and it wakes a thread parked in `accept`.
- The listening socket's final close runs on the closer's `.socket` lane
  ([drain mode](#fds-a-peer-attaches-drain-mode)). Connections still in the
  accept queue are released inside that close, with any fds on their unread
  messages, and one of those can block (measured: 3 s for a 3 s linger, on
  Linux and macOS). `close` closes the fd number at once and never waits.
  A listener you build with `Listener.initFd` on an AF_UNIX socket still
  closes inline: use `unix.listen`.
- **Accept waits while socket closes are stuck.** `Listener.accept`,
  `Listener.acceptFd` and `ServerSession.accept` on a `unix.listen` listener
  wait while the `.socket` lane holds `fd_io.closer.socketLaneBound()` jobs
  or more (a quarter of the process fd budget's limit, at least 16). New
  connections wait in the kernel's backlog meanwhile, outside this process's
  fd table. A `.backpressure` event (`error.SocketCloseQueueFull`) reports
  each wait, and `close` ends it with `error.ListenerClosed`.
- `Listener.getAddress()` has no meaning for a Unix listener: it returns
  `0.0.0.0:0`. Use `Listener.unixPath()`, which returns the bound path (null
  for TCP listeners and for `Listener.initFd`).
- The listening socket, the lock and the client socket are close-on-exec.
  There is no `TCP_NODELAY` on these sockets.

### Connect timeout

- Linux: a blocking connect to a listener whose backlog is full waits until
  the server accepts. `connect_timeout_ms` (default 30 s) bounds that wait
  with `SO_SNDTIMEO`, and resets it to 0 before the socket carries data. It
  returns `error.Timeout` when the time is up. Null waits without a bound.
- macOS never waits: a full backlog refuses the connect at once
  (`error.ConnectionRefused`).

## Windows and other targets

- **Windows:** `unix.listen` and `unix.connect` return
  `error.UnixSocketsUnsupported`. Fd passing is compiled out: `FdHandle` is
  an empty struct, `Peer.setExportFd` returns `error.FdPassingUnsupported`,
  and `Peer.importFd` returns null. The signatures are the same on every
  target. Windows AF_UNIX support (it has no `SCM_RIGHTS`) is deferred.
- **Other POSIX targets** (FreeBSD and the rest) get the same stubs. A
  transport that you build yourself on an AF_UNIX fd there
  (`Listener.initFd`, `Transport.init`) reads with a plain read: there is no
  drain mode, and the kernel decides what happens to attached fds.
- **The core module** (`capnpc-zig-core`) has no sockets and no
  `rpc.transport.unix`.

## Fds a peer attaches: drain mode

Any process that can connect to an AF_UNIX socket can attach open files to
the bytes it sends (`SCM_RIGHTS`). A receiver that does not ask for them does
not make them go away:

- macOS installs them in the receiver's fd table. A plain `read`, or a
  `recvmsg` with no control buffer, leaks them where nothing can find them.
- Linux closes them, but inside `recvmsg`, on the reading thread. The final
  close of a TCP socket with `SO_LINGER` and unsent data blocks that thread
  for the linger time.

So on Linux and macOS every transport on an AF_UNIX socket reads in **drain
mode**. This includes `unix.listen` and `unix.connect`, and also
`Listener.initFd`, `ServerSession`, `Connection.init` and `Transport.init` on
an AF_UNIX fd. Drain mode works like this:

- Each read waits until the socket is readable, takes a read claim in the
  closer (room for one read's fds; see [the process fd
  budget](#the-process-fd-budget)), then does one non-blocking `recvmsg`
  with a control buffer of 512 fd slots. A wakeup with nothing to read goes
  back to the wait and to the claim, so a reader never sits inside a
  `recvmsg`. macOS delivers at most one send's fds (254) per `recvmsg`, so
  the buffer never truncates there.
- On Linux every such socket gets `SO_OOBINLINE` before its first read.
  Since Linux 5.15 a peer can send one byte out of band (`MSG_OOB`) with fds
  attached. A normal `recvmsg` skips that message and the kernel frees it,
  closing its fds inside the read, on the reading thread (measured: 3 s for
  a 3 s linger, in drain mode and with fd passing on). In line, the byte is
  stream data and its fds come through the control buffer like any other.
  If the option cannot be set, every read fails. macOS refuses `MSG_OOB` on
  AF_UNIX sockets.
- Every fd that arrives goes to a closer thread. No received fd is closed on
  the reading thread or on the `Peer` thread. A `.resource_rejection` event
  (`.attached_fds`, `error.AttachedFdsRejected`) reports them.
- The closer has three lanes, each one thread with one queue, started on
  first use: `.received` (fds peers attached), `.socket` (the transport's own
  socket closes, and on macOS the read half of `shutdown`), and `.sent` (the
  copies of fds this process sends). A close that blocks in one lane does not
  delay the others.

A close can block on Linux **and** on macOS (macOS needs `SO_LINGER_SEC` and a
send queue that stays stuck). Linux has no cap on the linger time; macOS caps
it near 327 s. A tty, FUSE or NFS file can block a close for longer. See the
[threat table](#threat-table) for what a stuck close still costs.

## Fd passing

Fd passing attaches an fd to a capability (`CapDescriptor.attachedFd`), as
C++ does with `ClientHook::getFd` and `Capability::Server::getFd`.

### Turn it on, on both ends

`rpc.transport.unix.FdPassing` goes on `ListenOptions.fd_passing` and on
`ConnectOptions.fd_passing`:

- `max_fds_per_message: u8 = 0`: the most fds one inbound message keeps. The
  extras are closed. 0 (the default) keeps none: the connection stays in
  drain mode, **and sends none**. Values above 253 count as 253.
- `max_live_imported_fds: u32 = 64`: the most received fds the connection's
  `Peer` keeps attached to live imports at once.

One switch covers both directions, as in C++ (`maxFdsPerMessage`). Turn it
on only toward peers that turn it on too. A peer that did not ask still gets
the fds: macOS installs them, and Linux closes them on its reading thread.

For example, pass `.fd_passing = .{ .max_fds_per_message = 1 }` in the
options of both `unix.listen` and `unix.connect`. `ServerSession.accept` also
gives its peer `max_live_imported_fds`; a `Peer` that you build on a
`Listener.accept` connection takes it from `Peer.setMaxLiveImportedFds`.

With fd passing on, the transport reads **one frame at a time**: the 8-byte
head, the rest of the segment table, then the body. A read never crosses a
frame boundary, so each fd belongs to the frame whose bytes carried it, on
both kernels. (A bulk read attaches an fd to the read's last bytes on Linux
and to its first byte on macOS, so no one rule works there.) The transport
checks each header against the framer's limits before it reads or buffers the
body.

### Sending: `Peer.setExportFd`

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
fn serveWithFd(gpa: std.mem.Allocator, listener: *rpc.transport.tcp.Listener, server: *PingPong.Server, fd: rpc.transport.unix.FdHandle) !void {
    var session = try rpc.transport.tcp.ServerSession.accept(gpa, listener, .{});
    defer session.deinit();
    const export_id = try PingPong.setBootstrap(&session.peer, server);
    // Borrowed: keep `fd` open while the export has it, and call
    // clearExportFd before you close it early.
    try session.peer.setExportFd(export_id, fd);
    session.run();
}
```

- **What carries the fd:** every Call, Return, Resolve and Bootstrap Return
  that sends the export as a `senderHosted` capability, at most 253 fds per
  message. These carry none: loopback calls and returns, the source frame of
  an automatic third-party route, prebuilt (forwarded) Return frames, and
  anything sent through a `send_frame_override`. Fds through proxies are not
  supported.
- **The fd is borrowed.** The export stores the fd *number*. The transport
  makes a close-on-exec dup of it when it queues a message, and closes that
  dup on the closer thread after the send. Keep the fd open while the export
  has it, and call `clearExportFd` before you close it. If you close it first
  and the number is reused (any `open`, `accept` or `socket`), the next
  message that sends the capability carries **whatever file now has that
  number** to the peer. An export that is released forgets its fd.
- **Errors refuse one message, not the connection.** `error.FdQueueFull` is
  backpressure: more than 256 fds in flight on the transport, or the process
  fd budget is full. Retry later. A Call or Resolve returns the error;
  `sendReturnResults` returns it to the handler; a Bootstrap gets an
  exception Return (`overloaded` when the fds lacked room, `failed`
  otherwise).
- **Linux `ETOOMANYREFS`.** A user may have at most its `RLIMIT_NOFILE` fds in
  flight on AF_UNIX sockets, across all its processes. A queued message then
  goes out without its fds (`attachedFd` points past the message's fds,
  which the spec reads as "no fd"), with a `.backpressure` event
  (`error.TooManyFdsInFlight`).
- Over TCP and QUIC nothing is attached: `attachedFd` stays 0xff.

### Receiving: `Peer.importFd`

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
fn writeThroughAttachedFd(io: std.Io, client: PingPong.Client) !void {
    // Borrowed: valid until the capability is released. Do not close it;
    // dup it to keep it longer.
    const handle = client.peer.importFd(client.cap_id) orelse return error.NoFdAttached;
    const file: std.Io.File = .{ .handle = handle.fd, .flags = .{ .nonblocking = false } };
    // A peer can attach any open file: check its kind before you use it.
    if ((try file.stat(io)).kind != .named_pipe) return error.UnexpectedFdKind;
    try file.writeStreamingAll(io, "hello\n");
}
```

- **Which capabilities keep an fd:** `senderHosted` and `senderPromise`
  descriptors. The first descriptor wins: a second fd for the same import, in
  the same message or a later one, is closed. `thirdPartyHosted`,
  `receiverHosted` and `receiverAnswer` descriptors never keep one (the
  third-party case is optional in the spec and deferred here). An
  `attachedFd` index past the message's fds means no fd.
- **The import owns the fd.** `importFd` lends it until the import is
  released: `Client.release`, the end of a handler that did not retain a
  parameter capability, or `Peer.deinit` (the end of the session, after a
  disconnect too). The peer then hands the fd to the closer thread. Do not
  close it. `dup` it to keep it longer. Use it on the peer's thread, or dup
  it first: the session's `deinit` releases every import.
- **The fd goes with the Release.** Once the peer sends the Release for an
  import's last reference, its fd is closed, even if a promise export that
  resolved to that import still forwards calls to it. The remote may give
  the id to a new capability at once (C++ does), and that capability gets
  its own fd. A handoff (three-party) pin withholds the Release, and the fd
  stays until the unpin.
- **A promise import** gives null until it resolves, then the fd of what it
  resolved to.
- **Check the kind of file** (`fstat`, `std.Io.File.stat`) before you use it.
  A peer can attach any open file: a socket, a directory, a device, a FIFO
  whose other end it holds, or a file whose close blocks.
- **Limits.** Over a limit the capability still arrives, without its fd, and
  an event says why: more than `max_fds_per_message` in one message
  (`error.AttachedFdsOverLimit`), more than `max_live_imported_fds` live
  (`error.ImportedFdsOverLimit`), or the process fd budget
  (`error.FdBudgetExceeded`). Fds in two `recvmsg` batches within one frame
  are a protocol error: the connection closes
  (`error.MultipleAttachedFdBatches`).
- **Low level.** On a `Connection` with `enableFdPassing`, `on_message` can
  take the frame's fds with `takeFrameFd(index)`. The caller then owns the fd,
  and it leaves the process fd budget. `sendFrameWithFds` sends fds.

### Who owns each fd

| Fd | Owner | Closed when | Closed by |
|---|---|---|---|
| An fd given to `setExportFd` | The app (borrowed by the export) | The app closes it, after `clearExportFd` or the export's release | The app |
| The dup a transport queues for a send | The transport | After the send, a failed send, or teardown | The `.sent` lane |
| A received fd nobody keeps (drain mode, over a limit, a frame never dispatched, an untaken frame fd) | The transport | At once | The `.received` lane |
| A received fd an import keeps | The import | The import is released, or `Peer.deinit` | The `.received` lane |
| An fd taken with `Connection.takeFrameFd` | The app | When the app closes it | The app |
| The transport's own AF_UNIX socket | The transport | `deinit` | Inline on Linux when nothing can be in flight; otherwise the `.socket` lane |
| The listening socket of `unix.listen` | The listener | `Listener.close` | The `.socket` lane (a dup of it; the fd number is closed at once) |

### Events

Connections over AF_UNIX report `events.Source.unix`. Fd events use
`events.Resource.attached_fds`. Both enums are non-exhaustive: switch on them
with an `else` prong.

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
const FdEvents = struct {
    /// Fds this side closed instead of keeping (drain mode, a limit, the
    /// budget, a truncated read); `err` names the cause.
    rejections: usize = 0,
    /// Sends with fds this side refused (`error.FdQueueFull`).
    backpressure: usize = 0,
    last_err: ?anyerror = null,

    fn onEvent(ctx: *anyopaque, event: rpc.events.Event) void {
        const self: *FdEvents = @ptrCast(@alignCast(ctx));
        switch (event) {
            .resource_rejection => |e| if (e.resource == .attached_fds) {
                self.rejections += 1;
                self.last_err = e.err;
            },
            .backpressure => |e| if (e.resource == .attached_fds) {
                self.backpressure += 1;
                self.last_err = e.err;
            },
            else => {},
        }
    }
};
```

| Event | `err` | Meaning |
|---|---|---|
| `.resource_rejection` | `AttachedFdsRejected` | Fds nobody kept, closed (drain mode, or frame fds nobody took) |
| `.resource_rejection` | `AttachedFdsOverLimit` | More fds in one message than `max_fds_per_message` |
| `.resource_rejection` | `AttachedFdsTruncated` | `MSG_CTRUNC` (Linux at `RLIMIT_NOFILE`): the message keeps no fds |
| `.resource_rejection` | `ImportedFdsOverLimit` | The per-connection live-fd cap |
| `.resource_rejection` | `FdBudgetExceeded` | The process fd budget: the message keeps the fds that fit |
| `.resource_rejection` | `ProcessFdQuotaExceeded`, `SystemFdQuotaExceeded` | `recvmsg` hit EMFILE or ENFILE (macOS 26 can say EMSGSIZE). A second one in a row closes the connection |
| `.resource_rejection` | `FdCloseQueueFull` | The closer's `.received` lane is full (a stuck close), or no read claim came back within 1 s. The connection closes |
| `.backpressure` | `FdQueueFull`, `FdBudgetExceeded` | A send with fds was refused; the connection stays |
| `.backpressure` | `TooManyFdsInFlight` | Linux `ETOOMANYREFS`: a queued message went without its fds |
| `.backpressure` | `SocketCloseQueueFull` | A `unix.listen` listener waits in `accept`: the `.socket` lane is at its bound (source `.unix`, role `.server`) |
| `.protocol_error` | `MultipleAttachedFdBatches`, `InvalidFrame`, `FrameTooLarge` | A bad frame with fd passing on. The connection closes |

## Platform matrix

| | Linux | macOS | Windows, other targets |
|---|---|---|---|
| `unix.listen`, `unix.connect` | Yes | Yes | `UnixSocketsUnsupported` |
| Drain mode | Yes | Yes | No (no AF_UNIX transport) |
| Fd passing | Yes | Yes | Compiled out (`FdPassingUnsupported`, `importFd` null) |
| `sun_path` | 108 bytes | 104 bytes | n/a |
| Received fds close-on-exec | Atomic (`MSG_CMSG_CLOEXEC`) | `fcntl` right after `recvmsg` (a race window) | n/a |
| Fds per `sendmsg` | 253 (`SCM_MAX_FD`) | 254 (this library sends at most 253) | n/a |
| Bulk-read fd anchor | The read's last bytes | The read's first byte | n/a |
| A plain read of a message with fds | Closes them inside the read | Installs and leaks them | n/a |
| Control buffer too small | Closes what does not fit, sets `MSG_CTRUNC` | Installs every fd; only those that fit are visible | n/a |
| `recvmsg` at the fd limit | Delivers the fds that fit, sets `MSG_CTRUNC`, closes the rest | Fails with EMFILE (macOS 26: EMSGSIZE), closes the fds; a retry returns the data | n/a |
| Final close of a received lingering socket | Blocks (no cap) | Blocks (up to about 327 s) | n/a |
| `shutdown(SHUT_RD)` with fds unread | Does not close them | Closes them, and can block | n/a |
| Final close of a listening socket whose accept queue holds a message with a lingering fd | Blocks | Blocks | n/a |
| Close of the other end of a socket stuck disposing of a lingering in-flight fd | Does not wait | Waits while that socket's `shutdown(SHUT_RD)` is stuck (the transport's closer does that shutdown on macOS) | n/a |
| `MSG_OOB` on AF_UNIX | Since 5.15. A normal `recvmsg` skips the byte and closes its fds; `SO_OOBINLINE` reads it in line | Refused | n/a |
| `sendmsg` with fds on a full path | Blocks | Fails with EMSGSIZE at once (the library waits and retries) | n/a |
| Fds in flight per user | `RLIMIT_NOFILE` (`ETOOMANYREFS`) | No such limit | n/a |
| Full-backlog connect | Waits (`connect_timeout_ms`) | Refused at once | n/a |
| Default soft `RLIMIT_NOFILE` | Often 1024 | 256 | n/a |

The FD-0 suite (`tests/rpc/transport/unix/unix_kernel_semantics_test.zig`)
pins each kernel behavior in this table on the CI's Linux and macOS legs. If
a kernel changes, it goes red.

## The process fd budget

Fd passing keeps fds alive in several places. One process-wide count,
`rpc.transport.unix.fd_io.budget`, covers all of them:

- the fds a transport keeps for a frame, from `recvmsg` until they are taken
  or go to the closer;
- the fds a `Peer` keeps for its imports, until the import is released;
- a transport's dups of the fds it sends, until they are closed;
- every fd in the closer's `.received` and `.sent` lanes, until closed.

The transport's own sockets, the wake fds, and an fd the app took with
`takeFrameFd` do not count.

The limit is `RLIMIT_NOFILE / 4` (the soft limit, read at first use, at least
16). On macOS the default soft limit is 256, so the budget is 64. Over the
limit fd passing degrades and connections stay up: a received fd that does
not fit goes to the closer with an event, and a send with fds gets
`error.FdQueueFull`.

What a peer can make this process hold while a close blocks:

- **Counted fds:** below twice the limit plus one read (254 fds), whatever
  the number of connections. The fds that arrive at the closer's `.received`
  lane may pass the limit (they have nowhere else to go), so each read first
  takes a claim there: room for one read's worst case. Claims are granted
  while the limit has room for them, the first reader always gets one, and
  a reader whose claim does not come back closes its connection instead of
  reading. Readers that wake together therefore add at most one read past
  the limit, not one each (measured without claims: 16 readers put 4063 fds
  in a lane bounded at 16).
- **Socket closes:** at most `socketLaneBound()` jobs in the `.socket` lane
  (a quarter of the limit, at least 16), because a `unix.listen` listener
  stops accepting there. Each job holds one socket fd.

With the default limit that is about `RLIMIT_NOFILE / 2 + 254 +
RLIMIT_NOFILE / 16`: 830 fds at a soft limit of 1024, which leaves room for
the process's own fds.

To size it for your process, set it once, before the first AF_UNIX
connection:

<!-- verbatim: tests/docs/rpc_unix_snippets_test.zig -->
```zig
// Once, before the first AF_UNIX connection: the fds fd passing may
// hold in this process (default RLIMIT_NOFILE / 4).
const previous_limit = rpc.transport.unix.fd_io.budget.setLimit(256);
```

**Raise `RLIMIT_NOFILE` on macOS.** One message can carry 254 fds, so at the
default soft limit (256) one message can hit the fd limit on its own. If a
close in the `.received` lane blocks at that moment, that message's fds stay
in the table for as long as the close blocks (the security review measured
`socket()` failing with EMFILE on both kernels at a soft limit of 256). The
bounds above mean
something only from a soft limit of about 1024 up. Raise the soft limit to
1024 or more before the first AF_UNIX connection (for example with
`setrlimit` at startup); the library never changes it.

## Threat table

The attacker is a local process that can connect to the socket, or a peer
that the app connects to. Each row names its proof: a test in
`tests/rpc/transport/unix/` (file, then test name), unless it says
otherwise. Status values:

- **Defended:** the runtime prevents it.
- **Bounded:** the runtime limits the damage, at the stated cost.
- **Residual:** accepted and documented; not prevented.
- **App contract:** the app must follow the stated rule.

Rows 39-44, the read claims in row 7 and the bound in row 8 (open until
then) came from the security review of this table (2026-10-05).

| # | Threat | Status | Defense, and what is left | Proof |
|---|---|---|---|---|
| 1 | A peer attaches fds the receiver did not ask for | Defended | Drain mode on every AF_UNIX transport: a control buffer on every read, every fd to the closer. (macOS installed them; Linux closed them on the reader.) On Linux `SO_OOBINLINE` keeps a read from skipping an out-of-band message with fds (row 39) | `rpc_unix_fd_drain_test.zig` "a Connection on an AF_UNIX socket closes the pipe write end a peer attached to a frame (macOS leaked it)"; `unix_kernel_semantics_test.zig` "FD-0 a read with no control buffer leaks the fd on macOS (T4) and closes it on Linux"; `rpc_unix_linger_test.zig` "MSG_OOB: a lingering socket sent out of band reaches the closer like any fd, and the read does not wait for its close" |
| 2 | Fd flood: many fds per message, many messages | Bounded | Extras past `max_fds_per_message` closed at once; every fd of every read closed in drain mode; the process budget caps what is kept | `rpc_unix_fd_boundary_test.zig` "more fds than max_fds_per_message: the frame keeps the first ones, the extras are closed at once"; `rpc_unix_fd_drain_test.zig` "every attached fd on every read is closed: 20 frames with 3 fds each"; `rpc_unix_fd_limits_test.zig` "over the process fd budget a frame arrives without the fds that do not fit; they are closed and the connection stays" |
| 3 | Fd-table exhaustion by one connection | Bounded | `max_live_imported_fds` per connection; EMFILE on a read drops that message's fds, reports it, retries once | `rpc_unix_fd_peer_test.zig` "inbound: past max_live_imported_fds the capability arrives without its fd, the fd is closed, and an event says so"; `rpc_unix_fd_drain_test.zig` "EMFILE: the connection survives, the drop is reported once, and no fd stays open" |
| 4 | Fd-table exhaustion across connections, through the fds fd passing keeps | Bounded | One process budget (`RLIMIT_NOFILE / 4`) for frame fds, imports, sent dups and the closer's queues; read claims keep the `.received` lane within one read of it (row 7); the `.socket` lane is bounded too (row 8). Cost: a peer that fills the budget starves fd passing for every connection (fds dropped with events, sends refused); connections and `accept` keep working. Needs a soft `RLIMIT_NOFILE` of 1024 or more (row 41) | `rpc_unix_fd_limits_test.zig` "N connections at their per-connection fd cap: the budget holds the total, and accept still works"; "one budget for every kind: fds a frame keeps leave less room for sends, and sent dups less for frames"; `rpc_unix_linger_test.zig` "at a soft RLIMIT_NOFILE of 1024 and the default budget, peers that stall the received lane and then send from many connections at once cannot fill the fd table" |
| 5 | A received fd whose final close blocks (`SO_LINGER` socket, tty, FUSE, NFS), on Linux **and** macOS | Defended for readers, teardown and `Listener.close` | No received fd is closed on the reader or `Peer` thread; `deinit` never waits for a close; an out-of-band message is read in line, not skipped and closed inside the read (row 39); a `unix.listen` listener's final close, which disposes of the fds on its pending connections, runs on the `.socket` lane (row 40) | `rpc_unix_linger_test.zig` "a frame that carries a lingering socket dispatches at once, and close plus deinit stay fast"; "deinit of an AF_UNIX transport with an unread lingering socket in its queue does not block"; "MSG_OOB: a lingering socket sent out of band reaches the closer like any fd, and the read does not wait for its close"; "Listener.close does not wait for the final close of a pending connection that carries a lingering socket"; `unix_kernel_semantics_test.zig` "FD-0 the final close of a received lingering socket blocks the closing thread (Linux: inside recvmsg with no control buffer)" |
| 6 | A stuck received close holds other connections' socket closes | Defended | Separate lanes: `.received` for peers' fds, `.socket` for the transport's own sockets, `.sent` for sent dups | `rpc_unix_linger_test.zig` "a received fd whose close blocks holds up no other connection's socket close or shutdown" |
| 7 | A stuck received close, then more fds: the closer's queue grows | Bounded | Once the `.received` lane holds the budget's limit of fds that arrived there, every AF_UNIX read that finds data reads nothing and closes its connection (`FdCloseQueueFull`), on **every** AF_UNIX connection of the process, not only the sender's: a denial of service on AF_UNIX connections while the close is stuck. Each read first takes a claim worth one read's fds (254), granted only while the limit has room, so readers that wake together end at most one read past the limit, not one read each (measured without claims: 16 readers put 4063 fds in a lane bounded at 16). A reader whose claim does not come back within 1 s closes its connection. The fd table stays bounded at a soft `RLIMIT_NOFILE` of 1024 or more (row 41) | `rpc_unix_linger_test.zig` "while a received fd's close blocks, reconnecting peers cannot grow this process's fds past the bound"; "a reader already waiting for data takes no fds once the received lane is full"; "readers that wake together take at most one read past the received lane's bound, however many they are"; "at a soft RLIMIT_NOFILE of 1024 and the default budget, peers that stall the received lane and then send from many connections at once cannot fill the fd table"; `rpc_unix_fd_drain_test.zig` "fds past the closer-queue bound close the connection that sent them, with a typed cause"; `rpc_unix_fd_limits_test.zig` "fds the budget already counts end no connection when they move to the closer, even while a close there blocks" |
| 8 | A stuck **socket-lane** close, then many connections that close | Bounded | Once one close there blocks (a connection torn down with a lingering fd still unread), every later socket close it takes keeps that socket's fd until the blocked close ends. macOS sends every AF_UNIX close there; Linux every close with bytes unread, which a peer forces by tearing down mid-frame or by filling the `.received` lane. A `unix.listen` listener takes no connection while the lane holds `socketLaneBound()` jobs (a quarter of the budget's limit, at least 16), with a `.backpressure` event (`SocketCloseQueueFull`); new connections wait in the kernel's backlog. Cost: no new AF_UNIX connections on that listener while the close is stuck (on Linux as long as the attacker keeps the far end's window shut, on macOS up to about 327 s per lingering socket, chainable); existing connections, TCP and QUIC keep working. Connections the app opens itself (`unix.connect`, `Connection.init` on its own fd) are not gated | `rpc_unix_linger_test.zig` "behind a stuck socket-lane close, a Unix listener stops accepting at the lane's bound, so reconnecting peers cannot grow this process's fds past it"; "a connection torn down with a blocking fd still unread stalls only the socket lane" |
| 9 | An fd this process sends whose final close blocks | Bounded | Sent dups count in the process budget: at the limit every transport refuses fd messages (`FdQueueFull`); messages without fds still go | `rpc_unix_fd_send_test.zig` "while a sent dup's close blocks, the dups behind it stop at the process fd budget across many transports" |
| 10 | The kernel closes fds inside a `recvmsg` that hits the fd limit (Linux: what does not fit; macOS: all of that message's), on the reading thread; a lingering one blocks the reader | Residual | The transport reports the drop and keeps the connection; the close itself is the kernel's. The process budget and the read claims keep fd passing below half of `RLIMIT_NOFILE` plus one read, so only an app near its limit, or below the recommended soft limit of 1024 (row 41), gets there | `unix_kernel_semantics_test.zig` "FD-0 EMFILE: Linux delivers a partial list with CTRUNC; macOS fails the first recvmsg and drops the fds"; `rpc_unix_fd_boundary_test.zig` "EMFILE or MSG_CTRUNC: the frame arrives with no fds, the drop is reported, the connection stays" |
| 11 | macOS 26 reports the fd limit as EMSGSIZE | Defended | `recvWithFds` maps EMSGSIZE to `ProcessFdQuotaExceeded` on macOS | `unix_kernel_semantics_test.zig` "FD-0 EMFILE with a large fd table: macOS may report the limit as EMSGSIZE" |
| 12 | macOS installs every fd even when the control buffer is too small (a leak of the fds not visible) | Defended by the buffer size | 512 slots; one `recvmsg` never merges two sends with fds, so it never truncates. Residual only for a caller of `recvWithFds` that passes a small buffer: the leak is the fds sent minus the fds visible | `unix_kernel_semantics_test.zig` "FD-0 one recvmsg never merges two sends that carry fds, even with a 512-slot buffer"; `rpc_unix_fd_drain_test.zig` "a control buffer too small for the attached fds: every visible fd is closed; the macOS leak is sent minus visible" |
| 13 | A truncated or hostile `cmsghdr` | Defended | A clamped parser, not std's `cmsg.Iterator` (which drops a truncated header with fds in it) | `rpc_unix_fd_drain_test.zig` "fd_io.parseRights clamps a truncated header and walks every SCM_RIGHTS message"; `unix_kernel_semantics_test.zig` "FD-0 a hostile 64-bit cmsg_len clamps to the bytes present on every host" |
| 14 | An fd attributed to the wrong message | Defended | Exact-boundary reads with fd passing on | `rpc_unix_fd_boundary_test.zig` "fuzz: random read splits, fd placement and hostile headers; no leak, no cross-frame fd, nothing past the limits"; "exact-boundary reads put each fd in the frame whose bytes carried it (E1-E4, E3, E3b), reads smaller than a frame"; `unix_kernel_semantics_test.zig` "FD-0 bulk reads anchor an fd to the read's last bytes on Linux and its first byte on macOS" |
| 15 | A hostile frame header (segment count, frame size) | Defended | Checked as the bytes arrive, before any body byte is read or buffered | `rpc_unix_fd_boundary_test.zig` "a hostile segment count ends the connection at the head, before the framer sees it"; "a header past max_buffered_frame_bytes ends the connection before any byte of its body is read or buffered"; the fuzz test (row 14) |
| 16 | Fds in two batches within one message | Defended | Protocol error: every held fd closed, connection closed | `rpc_unix_fd_boundary_test.zig` "fds in two batches within one frame: a protocol error closes every fd and the connection" |
| 17 | A message that is never dispatched keeps its fds (end of stream, protocol error, failed read, teardown) | Defended | Its fds go to the closer at once | `rpc_unix_fd_boundary_test.zig` "a frame cut off by end of stream: its fds are closed when reading ends, not only at teardown"; "a protocol error closes the fds held for the frame being read at once, not at teardown"; "a failed recvmsg closes the fds held for the frame being read at once, not at teardown"; "teardown with a frame half read closes the fds it holds" |
| 18 | A hostile `attachedFd`: out of range, duplicate, a second fd for one import, on a third-party or receiver descriptor | Defended | Out of range is no fd; the first descriptor wins; the others are closed | `rpc_unix_fd_peer_test.zig` "inbound: an attachedFd past the frame's fds attaches nothing, and the fd is closed"; "inbound: a duplicate fd index goes to the first descriptor only"; "inbound: a second fd for the same import is closed, in one frame and in a later one; the first stays"; "inbound: thirdPartyHosted and receiverHosted descriptors never keep an fd" |
| 19 | A received fd survives `exec` (CLOEXEC race) | Residual on macOS | Linux sets close-on-exec atomically (`MSG_CMSG_CLOEXEC`). macOS has no such flag: `fcntl` runs right after `recvmsg`, and a `fork` + `exec` on another thread in between inherits the fd. On macOS spawn children with `posix_spawn` and `POSIX_SPAWN_CLOEXEC_DEFAULT`, or do not fork while serving AF_UNIX | `rpc_unix_fd_drain_test.zig` "fd_io.recvWithFds returns close-on-exec fds on both kernels (Linux: MSG_CMSG_CLOEXEC; macOS: fcntl right after)"; `unix_kernel_semantics_test.zig` "FD-0 fd round trip: the kernel header matches the fixture, fds map in order, CLOEXEC per OS" |
| 20 | Fd-number reuse between queueing and sending | Defended | The transport dups each fd when it queues the message | `rpc_unix_fd_send_test.zig` "the fds a transport sends refer to the caller's files" |
| 21 | Fd-number reuse after the app closes an fd that an export still names | App contract | The export stores the number. Call `clearExportFd` before you close the fd; otherwise the next send carries whatever file reuses the number | `rpc_unix_fd_peer_test.zig` "outbound: clearExportFd and a released export stop attaching; setExportFd checks its arguments" (the safe path) |
| 22 | Use after release of an imported fd | App contract | `importFd` lends the fd until the import is released (at the latest by `Peer.deinit`); then the closer closes it and the number can be reused. Dup it to keep it | `rpc_unix_fd_peer_test.zig` "inbound Return: a result cap's fd belongs to its import until the import is released"; "close hook: Peer.deinit closes the fd of every import still live" |
| 23 | A received fd of an unexpected kind | App contract | Check it with `fstat` before use | `examples/rpc_fd_passing.zig` and the "import-fd" snippet (`tests/docs/rpc_unix_snippets_test.zig`) check the kind |
| 24 | The runtime's own fds leak to a peer (wake socketpair, lock, listening socket) | Defended | Only fds given to `setExportFd` are attached | `rpc_unix_fd_peer_test.zig` "the wake socketpair's fds never cross the socket: only the attached fd does" |
| 25 | Fds sent over TCP or QUIC | Defended | Those bindings carry no fds: `attachedFd` stays 0xff | `rpc_unix_fd_peer_test.zig` "TCP: setExportFd leaves attachedFd at 0xff and the client gets no fd"; "outbound: on a binding that carries no fds (TCP, QUIC), setExportFd leaves attachedFd at 0xff" |
| 26 | Fds sent to a peer that did not opt in | Defended on this side | A connection sends fds only with its own fd passing on. The remote's setting is not known: turn fd passing on only toward peers that turn it on | `rpc_unix_fd_peer_test.zig` "an AF_UNIX connection without fd passing on sends no fds: attachedFd stays 0xff" |
| 27 | SIGPIPE on a send to a closed peer | Defended | `MSG_NOSIGNAL`, and `SO_NOSIGPIPE` on macOS | `rpc_unix_fd_send_test.zig` "sendWithFds to a closed peer returns BrokenPipe and raises no SIGPIPE" |
| 28 | `sendmsg` with fds fails with EMSGSIZE on macOS on a full path | Defended | Wait until writable, retry once | `rpc_unix_fd_send_test.zig` "sendWithFds on a blocking socket waits while the peer's buffer is full (macOS refused the fds with EMSGSIZE)"; `unix_kernel_semantics_test.zig` "FD-0 a blocking sendmsg with fds on a full path: Linux blocks, macOS fails at once with EMSGSIZE" |
| 29 | Linux `ETOOMANYREFS` (a receiver that stops reading) | Defended | Typed backpressure; the connection stays | `rpc_unix_fd_limits_test.zig` "Linux ETOOMANYREFS: a direct send is refused with FdQueueFull, a queued one goes without its fds, and the connection stays" |
| 30 | Byte order of the fds in a control message | Defended | Native byte order, pinned with fixed bytes (the big-endian CI job only compiles) | `unix_kernel_semantics_test.zig` "FD-0 fixed cmsg bytes decode to the same fd numbers in each ABI's byte order"; "FD-0 std builds exactly the native fixed cmsg bytes" |
| 31 | An allocation failure on an fd path leaks an fd | Defended | Reservations: no hand-off to the closer allocates or fails | `rpc_unix_fd_limits_test.zig` "OOM at every allocation of the Peer's fd paths leaks no fd, no byte and no budget unit"; "OOM at every allocation of a transport's fd paths leaks no fd and no budget unit"; "a failed closer-queue allocation at any point of a transport's fd paths leaks nothing" |
| 32 | The socket file is swapped before `chmod` | Narrowed; closed by the private directory | A (dev, ino) check around the `chmod`; `close` unlinks only its own inode. The listen-time check has no test (it needs a race inside `listen`) | `rpc_unix_session_test.zig` "close unlinks only its own inode: a file that replaced the path stays"; "the socket file gets socket_mode (0600 by default) and bits outside 0o777 are refused" |
| 33 | A stale-file reclaim race | Defended | The lock, held for the listener's life; no connect probe | `rpc_unix_session_test.zig` "racing reclaimers on a stale file: exactly one wins, every loser gets AddressInUse while it lives"; "a reclaimer against a live listener gets AddressInUse, and the live listener still serves" |
| 34 | A link at `<path>.lock` | Defended | `O_NOFOLLOW` (and `O_NONBLOCK` for a FIFO) | `rpc_unix_session_test.zig` "a symbolic link at <path>.lock is refused, and listen creates nothing where it points" |
| 35 | A path that does not fit `sun_path` (std panics on 105-108 bytes on macOS), an abstract name | Defended | Checked before any syscall | `rpc_unix_session_test.zig` "a path as long as sun_path returns NameTooLong; one byte shorter binds and connects"; "abstract names, empty paths and paths with a NUL are refused" |
| 36 | Who is the peer? | Residual | No peer-credential check in this version; access control is the directory and `socket_mode` | None (deferred) |
| 37 | `fork` without `exec` | Residual | The child has no closer threads: a transport used in the child queues fds that nothing closes. The child also keeps the listener's lock | None (documented in `fd_io.zig` and `socket.zig`) |
| 38 | Linux kernels before 6.8 run the AF_UNIX fd garbage collector inside a socket close | Residual | The final close of a peer's unreachable in-flight fds can happen inside any AF_UNIX close, the transport's inline one included | None (from the kernel source; documented in `fd_io.zig`) |
| 39 | A peer attaches fds to an out-of-band byte (`MSG_OOB`, Linux 5.15+), which a normal `recvmsg` skips: the kernel frees it and closes its fds inside the read, on the reading thread, outside the closer and the budget (measured: 3 s for a 3 s linger, in drain mode and with fd passing on) | Defended | Every drain-mode transport sets `SO_OOBINLINE` before its first read: the byte is stream data and its fds come through the control buffer to the closer. If the option cannot be set, every read fails. macOS refuses `MSG_OOB` on AF_UNIX | `rpc_unix_linger_test.zig` "MSG_OOB: a lingering socket sent out of band reaches the closer like any fd, and the read does not wait for its close" (Linux; macOS checks the refusal); `unix_kernel_semantics_test.zig` "FD-0 MSG_OOB on AF_UNIX: Linux skips an out-of-band byte in a normal read and closes its fds inside it, SO_OOBINLINE reads it in line; macOS refuses it" |
| 40 | A peer connects, attaches a lingering fd and leaves without being accepted: the listening socket's final close disposes of it, and `Listener.close` blocks (measured: 3 s on Linux and macOS) | Defended for `unix.listen` listeners | The final close runs on the `.socket` lane (on a dup; the fd number closes at once, and a thread in `accept` still wakes). A listener built with `Listener.initFd` on an AF_UNIX socket still closes inline: use `unix.listen` | `rpc_unix_linger_test.zig` "Listener.close does not wait for the final close of a pending connection that carries a lingering socket"; `rpc_unix_session_test.zig` "Listener.close wakes a thread parked in accept"; `unix_kernel_semantics_test.zig` "FD-0 the final close of a listening socket closes the fds on its pending connections' unread messages, and blocks on a lingering one" |
| 41 | A small soft `RLIMIT_NOFILE` (the macOS default is 256) | App contract | One message carries up to 254 fds. At 256 it can reach the fd limit on its own (row 10), and keep the table full while a received close blocks (the security review measured `socket()` failing with EMFILE on both kernels at 256). Raise the soft limit to 1024 or more before the first AF_UNIX connection; the library never changes it | `rpc_unix_linger_test.zig` "at a soft RLIMIT_NOFILE of 1024 and the default budget, peers that stall the received lane and then send from many connections at once cannot fill the fd table" (the recommended setting holds) |
| 42 | A reader wakes with nothing to read (an out-of-band byte skipped, a spurious wakeup), blocks in `recvmsg`, and takes a later message's fds without the closer's check | Defended | The read after `poll` is non-blocking (`MSG_DONTWAIT`); nothing to read goes back to `poll` and to the claim | `rpc_unix_fd_drain_test.zig` "fd_io.tryRecvWithFds never waits: nothing to read on a blocking socket is null at once, then the data with its fds" |
| 43 | The remote reuses the id of an import whose last reference this side released while a promise export still pins it, and the new capability's fd loses to the old one's ("the first fd wins"): the app writes the new capability's data into the old file, across principals through a broker | Defended | An import's fd is closed when the Release for its last wire reference goes out, even if a promise pin keeps the entry; a handoff pin withholds the Release, and the fd stays until the unpin sends it | `rpc_unix_fd_peer_test.zig` "inbound: after the Release of a promise-pinned import, a new capability that reuses its id gets its own fd, not the old one"; "close hook: an import pinned by a resolved promise export closes its fd with its last wire ref, since the Release lets the remote reuse the id"; "close hook: an import under a handoff pin and a promise pin keeps its fd while the Release is withheld, and closes it when the unpin sends the Release"; "close hook: a handoff-pinned import keeps its fd past its last wire ref, until the unpin" |
| 44 | macOS: a close of the other end of a socket whose `shutdown(SHUT_RD)` is stuck disposing of a lingering fd waits for it (the transport's closer does that shutdown on macOS) | Residual (macOS) | Matters only when both ends are in this process (an in-process client and server, a socketpair): close the other end after the transport's teardown has ended, or on a thread that may wait. Linux does not wait | `unix_kernel_semantics_test.zig` "FD-0 a close of the other end of a socket stuck disposing of a lingering fd: macOS waits for it, Linux does not" |

## Not covered

- Fds through proxies, `thirdPartyHosted` fds, and a Unix `VatNetwork` for
  three-party handoff.
- Windows AF_UNIX, FreeBSD fd passing, abstract socket names.
- Peer credentials (`SO_PEERCRED`, `getpeereid`).
- A generated `Client.fd()` helper: use `client.peer.importFd(client.cap_id)`.
- Fd passing against the C++ implementation is not yet run in CI.
