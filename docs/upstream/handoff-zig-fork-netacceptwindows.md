# HANDOFF — zig fork change branch: netAcceptWindows CANCELLED abort

**Status (2026-10-17): open in std. capnp-zig no longer goes through std's
`netAcceptWindows` for its TCP accepts.**

- Tagged 0.17.0 and the fork checkout (`feat/evented-0.17`) still have all
  three `.CANCELLED => unreachable` arms.
- Since capnp-zig `8c36f35` (for v0.27.0), capnp-zig accepts TCP
  connections on Windows with its own AFD accept (`windows_accept.accept`
  in `src/rpc/transport/tcp/windows_accept.zig`). `Listener.accept`,
  `Listener.acceptFd`, `ServerSession.accept` and the `WorkerPool` workers
  use it. `Listener.close` and `WorkerPool` shutdown cancel the parked waits
  (`windows_accept.stopWaits`) before they close the handle. `WorkerPool`
  no longer dials its own listener on Windows.
- On Windows, capnp-zig still calls std's accept only in
  `createLoopbackSocketPair` (`runtime.zig`) and in a test helper. Each of
  them accepts a connection that it dialed just before, and nothing closes
  the listener under that accept.
- Every other std user that closes a listener under a parked accept still
  aborts. capnp-zig's Windows CI run 38918354154 (2026-10-16, before
  `8c36f35`) panicked at `Threaded.zig:12836` in all three capnp-zig accept
  entry points (`tests/rpc/transport/tcp/rpc_listener_close_test.zig`).
- The `LOCAL_DISCONNECT` read and write arms that this handoff asked for
  are now in `handoff-zig-fork-netreadwindows-stray-cancel.md`.

Paste this into a session working on the nullstyle zig fork. It is
self-contained. Context: upstream zig does not accept this contribution
path, so the fix lands as a maintained change branch on the fork (one of
a tracked list of side branches). Suggested branch name:
`fix/netacceptwindows-cancelled`.

## The defect

File: `lib/std/Io/Threaded.zig`, function `netAcceptWindows`.
Re-verified at tagged **0.17.0** (2026-10-03); first found at
0.17.0-dev.1683+5ceec001b around line 12695. Re-locate on fork HEAD. The
function performs two AFD ioctls and switches on the completion status.
BOTH switches contain:

```zig
.CANCELLED => unreachable,
```

| 0.17.0 line | Site |
|---|---|
| 12822 | `fn netAcceptWindows` |
| 12836 | `.CANCELLED => unreachable` for `IOCTL.AFD.WAIT_FOR_LISTEN` |
| 12860 | `.CANCELLED => unreachable` for `IOCTL.AFD.ACCEPT` |
| 12867 | `fn deferAcceptAfd` (the sibling cleanup) |
| 12881 | `.CANCELLED => unreachable` for `IOCTL.AFD.DEFER_ACCEPT` |

`STATUS_CANCELLED` is treated as impossible on the theory that only
Io-level cancelation cancels the IRP. But Windows also completes pending
IRPs with STATUS_CANCELLED when:

1. another thread CLOSES the listening handle (`NtClose` cancels the
   handle's pending IRPs), or
2. anything calls `CancelIoEx` on the handle.

Both are ordinary teardown for a multi-threaded server (N acceptors
parked in `accept()`, a coordinator closing the listener to shut down).
The thread panics "reached unreachable code" and the PROCESS aborts —
uncatchable by the caller.

## Evidence

- capnp-zig CI, windows-latest, run 33029339794 (nullstyle/capnp-zig):
  64 threads parked in accept; the moment the pool closed the listener,
  multiple threads panicked simultaneously at
  `Io/Threaded.zig:12695 netAcceptWindows`, SIGABRT.
- capnp-zig worked around it embedder-side (wake every parked accept
  with self-connect dials BEFORE closing — commit `76ae51d` there), but
  the workaround fails under ephemeral-port exhaustion (the dials
  themselves fail), and then the abort is unavoidable. The std arm is
  the root cause. (On Windows, `8c36f35` replaced that workaround: see the
  status at the top.)

## Minimal repro (Windows)

```zig
const std = @import("std");

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const addr = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try addr.listen(io, .{});
    const T = struct {
        fn acceptor(srv: *std.Io.net.Server, io_: std.Io) void {
            // Old behavior: PROCESS ABORT here when main closes the
            // socket. Fixed behavior: returns an error; thread exits.
            _ = srv.accept(io_) catch return;
        }
    };
    const t = try std.Thread.spawn(.{}, T.acceptor, .{ &server, io });
    std.Io.sleep(io, std.Io.Duration.fromMilliseconds(200), .awake) catch {};
    server.socket.close(io); // cancels the parked AFD wait
    t.join(); // pre-fix: never reached (SIGABRT)
    std.debug.print("clean: accept returned an error instead of aborting\n", .{});
}
```

(This compiles for `x86_64-windows` against tagged 0.17.0, checked
2026-10-03 with `zig build-exe -target x86_64-windows -fno-emit-bin`.
Adjust `listen`/`Server` spelling if the fork's std API has drifted.)

## The fix

Map `.CANCELLED` to a recoverable error instead of `unreachable`:

- In BOTH `netAcceptWindows` switches:
  `.CANCELLED => return error.SocketNotListening,`
  (`SocketNotListening` is already in the accept error set — the POSIX
  path maps `.INVAL` to it for the analogous listener-gone race. If a
  distinct `error.Canceled` fits the fork's Io error taxonomy better,
  that also works; the requirement is "error, not abort".)
- In `deferAcceptAfd`: the function is void cleanup — make
  `.CANCELLED => {}` (tolerated, like its `else` arm which already
  swallows unexpected statuses).

### Moved: `STATUS_LOCAL_DISCONNECT` on read and write

This section asked for a `LOCAL_DISCONNECT` arm in `netReadWindows` and
`netWriteWindows`. That request, with more statuses and the soak counts, is
now in `handoff-zig-fork-netreadwindows-stray-cancel.md`. The fork can put
both fixes on one branch.

## Verification

1. The repro above: aborts before, prints "clean" after.
2. capnp-zig cannot check this fix any more: since `8c36f35` its
   Windows accepts do not go through `netAcceptWindows` (see the status at
   the top). Use the repro above.
3. Whatever std test convention the fork uses for Windows-only Io
   behavior: a test asserting accept returns an error (not aborts) when
   the listener closes underneath it, gated to Windows.

## Bookkeeping

Record the branch in the fork's change-branch list with a one-line
rationale ("close/CancelIoEx during parked accept must error, not abort
the process") so rebases onto upstream keep carrying it.
