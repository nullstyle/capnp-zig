# HANDOFF — zig fork change branch: netAcceptWindows CANCELLED abort

Paste this into a session working on the nullstyle zig fork. It is
self-contained. Context: upstream zig does not accept this contribution
path, so the fix lands as a maintained change branch on the fork (one of
a tracked list of side branches). Suggested branch name:
`fix/netacceptwindows-cancelled`.

## The defect

File: `lib/std/Io/Threaded.zig`, function `netAcceptWindows` (verified
at 0.17.0-dev.1683+5ceec001b, around line 12695; re-locate on fork
HEAD). The function performs two AFD ioctls and switches on the
completion status. BOTH switches contain:

```zig
.CANCELLED => unreachable,
```

once for `IOCTL_AFD_WAIT_FOR_LISTEN` and once for `IOCTL_AFD_ACCEPT`.
A third instance sits in the sibling `deferAcceptAfd` cleanup
(`IOCTL_AFD_DEFER_ACCEPT` switch).

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
  the root cause.

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

(Adjust `listen`/`Server` spelling to the fork's current std API if it
has drifted.)

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

## Verification

1. The repro above: aborts before, prints "clean" after.
2. If convenient, integration-check with capnp-zig: point its toolchain
   at the patched fork (its `mise.toml` pins zig; `mise exec` or PATH
   override) and run
   `zig build soak -Dquic=true -Doptimize=ReleaseSafe -- --transport tcp --no-chaos --seconds 20 --workers 64`
   on Windows — pre-fix this aborted at teardown before capnp-zig's own
   workaround landed.
3. Whatever std test convention the fork uses for Windows-only Io
   behavior: a test asserting accept returns an error (not aborts) when
   the listener closes underneath it, gated to Windows.

## Bookkeeping

Record the branch in the fork's change-branch list with a one-line
rationale ("close/CancelIoEx during parked accept must error, not abort
the process") so rebases onto upstream keep carrying it.
