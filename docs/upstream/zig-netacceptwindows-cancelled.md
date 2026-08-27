# DRAFT upstream report (ziglang/zig) — NOT FILED

Filing is the repo owner's call; this draft carries the full evidence.
Local context: capnp-zig fixed its side in `76ae51d` (teardown never
closes the listener under a parked accept), but the std arm remains
reachable for any Windows server whose listen handle closes—or has
CancelIoEx called—while a thread is parked in accept, and under
ephemeral-port exhaustion no user-space teardown can avoid it.

---

**Title:** `std.Io.Threaded.netAcceptWindows`: `.CANCELLED => unreachable`
is reachable via handle close / CancelIoEx — turns recoverable teardown
into a process abort

**Version:** 0.17.0-dev.1683+5ceec001b (also present at current master at
time of writing; `lib/std/Io/Threaded.zig`, `netAcceptWindows`)

## What happens

`netAcceptWindows` waits for a connection with the AFD
`IOCTL_AFD_WAIT_FOR_LISTEN` ioctl and switches on the completion status:

```zig
switch ((try deviceIoControl(&.{ ... })).u.Status) {
    .SUCCESS => {},
    .CANCELLED => unreachable,
    .INSUFFICIENT_RESOURCES => return error.SystemResources,
    else => |status| return windows.unexpectedStatus(status),
}
```

(The same arm appears again for `IOCTL_AFD_ACCEPT` below it.)

`STATUS_CANCELLED` is treated as impossible, presumably on the theory
that only Io-level cancelation cancels the IRP and that path is handled
elsewhere. But Windows also completes pending IRPs with
`STATUS_CANCELLED` when:

1. another thread CLOSES the listening handle while accept is parked
   (`NtClose` cancels the handle's pending IRPs), or
2. anything calls `CancelIoEx` on the handle.

Both are ordinary teardown shapes for a multi-threaded server
(N acceptor threads parked in `accept()`, a coordinator closing the
listener to shut down). In both cases the thread panics
"reached unreachable code" and the PROCESS aborts — there is no way for
the caller to catch it.

## Reproduction

Windows, `std.Io.Threaded`: park thread A in `net.Server.accept`, have
thread B close the listening socket, ~always panics at
`netAcceptWindows` with `reached unreachable code`. Observed in the wild
in capnp-zig's CI (windows-latest) at 64 parked acceptors:
run 33029339794 of nullstyle/capnp-zig — multiple threads panic at
`Io/Threaded.zig:12695 netAcceptWindows` the moment the listener closes
under them.

A user-space workaround exists (wake every parked accept with self-dial
"nudges" BEFORE closing) but is not always available: under
ephemeral-port exhaustion (~16k dynamic ports, 120s TIME_WAIT) the
nudge dials themselves fail, and the only remaining teardown is the one
that aborts.

## Suggested fix

Map `.CANCELLED` to an error the caller can act on rather than
`unreachable` — `error.SocketNotListening` fits the existing error set
(the listener is gone), or a dedicated `error.Canceled`. POSIX paths
return recoverable errors for the analogous close-during-accept races
(`.BADF`/`.INVAL` arms in the same file map to `errnoBug`/
`error.SocketNotListening` rather than aborting the process on the
async path).

The second `.CANCELLED => unreachable` (in the `IOCTL_AFD_ACCEPT`
switch) has the same exposure and should get the same treatment.
