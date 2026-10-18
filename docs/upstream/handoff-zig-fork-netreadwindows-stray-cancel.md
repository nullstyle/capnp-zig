# HANDOFF — zig fork change branch: Windows socket reads and writes (stray CANCELLED, unmapped disconnects)

**Status (2026-10-17): open.** Tagged 0.17.0 and the fork checkout
(`feat/evented-0.17`) have every defect that this handoff names. capnp-zig
works around the read side: its own AFD receive since v0.26.0, and its
own disconnect arms on main (for v0.27.0). The write and shutdown sides
have no workaround, so their stack traces stay in capnp-zig's Windows soak
logs until std maps the statuses.

Paste this into a session that works on the nullstyle zig fork. It is
self-contained. The fix lands as a maintained change branch on the fork.
Suggested branch name: `fix/netreadwindows-stray-cancel`. It is the same
file and the same AFD layer as `fix/netacceptwindows-cancelled`
(`handoff-zig-fork-netacceptwindows.md`), so one branch can carry both.

All line numbers are for `lib/std/Io/Threaded.zig` at tagged **0.17.0**.
Find them again on the fork HEAD (on `feat/evented-0.17` they are 16 to 53
lines later).

## 1. A receive that Windows cancels by itself aborts the read

| 0.17.0 line | Site |
|---|---|
| 12990 | `fn netReadWindows` |
| 13010 | `.CANCELLED => unreachable` |
| 3053-3062 | `batchApc`: a `CANCELLED` operation goes to `unused`, not to `completed` |
| 19575-19579 | `deviceIoControl`: an Io cancellation of the task returns `error.Canceled` before the status switch |

Windows can end an AFD receive with `STATUS_CANCELLED` when nothing asked
for a cancel. capnp-zig's Windows CI loops found this (about 11,000 runs
of one test, about 1% failed). In every failure:

- The receive ended within about 0.1 ms after it was posted.
- Nothing was in flight after it: `NtCancelIoFileEx(fd, NULL)` returned
  `STATUS_NOT_FOUND`.
- The receive took no bytes, and the rest of the stream arrived in order.
- It came right after a timed read on the same thread. The timeout
  cancellation of that earlier read raced the data completion of its
  receive.

The cause is inside Windows. The full description is on
`SpuriouslyCancelledReceive` in capnp-zig's
`tests/rpc/transport/tcp/rpc_tick_idle_test.zig`.

What std does with it:

- **`netReadWindows`:** the Io did not cancel the task, so
  `deviceIoControl` returns the status, and `.CANCELLED => unreachable`
  aborts the process: a panic in Debug and ReleaseSafe, undefined behavior
  in ReleaseFast and ReleaseSmall.
- **The batch path (`batchAwaitConcurrent`):** `batchApc` puts the
  cancelled operation on the `unused` list. The await then returns success
  with nothing completed and nothing pending. The caller cannot see that
  its receive ended, and no receive is posted any more.

### Evidence

- **Run 38039927959** (workflow "flake-tcp-rt3 loop", branch
  `diag/flake-tcp-rt3` at `b4d2abb`, 2026-10-10). Its "experiment" leg did
  a blocking std read right after timed reads that raced arriving data.
  28 of 1,600 runs panicked at `Threaded.zig:13010:23` in
  `netReadWindows` (3, 5, 9 and 11 in its 4 shards).
- **Run 38083790370** (workflow "flake-tcp-rt4 loop", branch
  `diag/flake-tcp-rt4` at `dc7c279`, 2026-10-10). The same experiment with
  capnp-zig's own receive (below): 1,600 of 1,600 runs passed, with 0
  panics. 47 reads in it met a stray cancel. Each one needed at most 2 new
  posts, and the longest run of cancels lasted about 16 ms.

Both branches are deleted, and their commits are on no branch. Use the run
logs. GitHub deletes them after the retention period of the repository
(90 days by default).

### capnp-zig workaround

capnp-zig `27c3d84` (v0.26.0): on Windows, when the Io can run the receive
batch concurrently (std's Threaded Io always can), every TCP socket read
(timed, untimed, and the `Connection` read loop) uses capnp-zig's own AFD
receive (`windowsReceive` in
`src/rpc/transport/tcp/stream_transport.zig`). When a receive ends
`CANCELLED` with no bytes and without a request, it posts the receive again
(`SpuriousCancelRetry`: the first new post at once, then 50 us doubling to
5 ms; it gives up with `error.Unexpected` after 10 s of cancels). When the
fork fixes both std sites, check whether capnp-zig still needs its own
receive.

### The fix

- In `netReadWindows`, a `CANCELLED` status must not be `unreachable`. An
  Io cancellation never reaches the switch (`deviceIoControl` returns
  `error.Canceled` first). So a `CANCELLED` there is a stray cancel, or a
  cancel from another thread (a close of the handle, or `CancelIoEx`).
  Post the receive again for a stray cancel, as capnp-zig does, or return
  a named error. The requirement is "no abort, and no lost bytes".
- In `batchApc`, the batch cannot tell a cancel that it asked for from a
  stray one. A possible change: put a `CANCELLED` operation that the batch
  did not cancel on the `completed` list with its status. The caller then
  sees it. This is a design choice for the fork. If the fork keeps the
  current behavior, document that a caller must add the operation again
  when an await returns with nothing completed.

## 2. Disconnect statuses that std does not map

`netReadWindows` (13012) and `netWriteWindows` (13526) map only
`CONNECTION_RESET` and `REMOTE_DISCONNECT`:

```zig
.CONNECTION_RESET, .REMOTE_DISCONNECT => return error.ConnectionResetByPeer,
```

`netShutdownWindows` (13614) maps only `SUCCESS` and
`INSUFFICIENT_RESOURCES`. Its `.CANCELLED => unreachable` arm (13629) is
not reached. The call is synchronous (`nonblocking = false`). On
`CANCELLED`, the synchronous path of `deviceIoControl` sends the call
again, or returns `error.Canceled` for an Io cancellation (19602-19605).
`netWriteWindows` is asynchronous, so its `.CANCELLED => unreachable`
(13524) can abort the process, as in section 1, for example after a
`CancelIoEx` on another thread. Every other status falls to
`else => windows.unexpectedStatus(status)`. That gives `error.Unexpected`.
With unexpected-error tracing on (the Debug default), it also prints the
NTSTATUS and a stack trace.

### Evidence

Nightly 38377271133 (2026-10-13, `bbf91e7`), job 115228694695 "RPC soak
(chaos + deadlines, windows-latest)". Its Debug lane
(`zig build soak -- --seconds 120 --workers 8`) printed these traces. The
soak closes connections on purpose (chaos and deadline sessions). The
transport gate of that lane passed: `transport_errors` (2,619) was equal to
`chaos_closes`.

| NTSTATUS | First frame | Traces |
|---|---|---|
| `LOCAL_DISCONNECT` (0xC000013B) | capnp-zig `windowsReceiveResult` (a copy of std's read mapping) | 2,974 |
| `CONNECTION_ABORTED` (0xC0000241) | capnp-zig `windowsReceiveResult` | 4 |
| `INVALID_DEVICE_STATE` (0xC0000184) | std `netWriteWindows` (13528) | 31 |
| `LOCAL_DISCONNECT` (0xC000013B) | std `netWriteWindows` (13528) | 11 |
| `PIPE_DISCONNECTED` (0xC00000B0) | std `netWriteWindows` (13528) | 7 |
| `CONNECTION_RESET` (0xC000020D) | std `netShutdownWindows` (13631) | 29 |

capnp-zig's receive used std's read mapping, so the first two rows are
what std's own `netReadWindows` returns for the same receives. capnp-zig
main (for v0.27.0) maps both of them to `error.ConnectionResetByPeer` in
`windowsReceiveResult`. capnp-zig writes through std's `net_write` and
shuts down through std's `netShutdown`. It ignores a failed shutdown, but
std prints the trace first.

### The fix

- **Read and write:** add the local disconnect statuses to the reset arm.

  ```zig
  .CONNECTION_RESET, .REMOTE_DISCONNECT, .LOCAL_DISCONNECT, .CONNECTION_ABORTED => return error.ConnectionResetByPeer,
  ```

  `LOCAL_DISCONNECT` means that the local network transport closed the
  connection. `CONNECTION_ABORTED` means that the local system aborted it.
  Both are disconnects, not unexpected states.
- **Write only:** the soak saw `PIPE_DISCONNECTED` and
  `INVALID_DEVICE_STATE` on sends. The log does not show the state of the
  socket, so what they mean here is unverified. If they mean a send after
  the local end shut down, `error.SocketUnconnected` fits: POSIX
  `netWritePosix` maps `EPIPE` and `ENOTCONN` to it (13456, 13460).
- **Shutdown:** map `CONNECTION_RESET` to `error.ConnectionResetByPeer`.
  It is already in `net.ShutdownError`.
- **Write:** map `CANCELLED` to a named error, not `unreachable`, in
  `netWriteWindows` (13524).

If the fork prefers other named errors, that also works. The requirement
is a named error, not `error.Unexpected`.

## Verification

1. **Stray cancel:** no deterministic repro exists, because the trigger is
   inside Windows. Loop the experiment of run 38039927959: a blocking read
   right after timed reads that race arriving data, on one socket and one
   thread. Before the fix, about 1 run in 57 panicked (28 of 1,600). After
   it, no run panics and every byte arrives in order.
2. **Status mapping:** move the status switches of `netReadWindows`,
   `netWriteWindows` and `netShutdownWindows` into small functions of
   `IO_STATUS_BLOCK`, as capnp-zig's `windowsReceiveResult` is. Then a
   plain test can check each status on every host. capnp-zig's test "a
   completed Windows receive maps as std's netReadWindows maps it, except
   CANCELLED and the local disconnects" does this.
3. **Integration:** point capnp-zig's toolchain at the patched fork and run
   `zig build soak -- --seconds 120 --workers 8` on Windows. The log must
   have no `error.Unexpected NTSTATUS=` line from `netWriteWindows` or
   `netShutdownWindows`.

## Bookkeeping

Record the branch in the fork's change-branch list with a one-line
rationale ("a stray AFD receive cancel must not abort or lose the read;
LOCAL_DISCONNECT, CONNECTION_ABORTED, PIPE_DISCONNECTED and
INVALID_DEVICE_STATE are disconnects, not Unexpected"). Then rebases onto
upstream keep it.
