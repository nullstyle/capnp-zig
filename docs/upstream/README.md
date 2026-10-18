# Upstream handoffs

This file is the ledger of the handoffs that capnp-zig gives to its
upstreams: quic-zig, and the nullstyle Zig fork. Each handoff is a file in
this directory. It is a document for the owner to relay, not a filed
issue.

- One row per handoff file. "Where fixed" gives the upstream ref and
  commit. "Workaround to delete" names the capnp-zig code that exists only
  because of the defect, and that can go when capnp-zig pins a fixed
  release.
- A status line at the top of a handoff file wins over this table. When
  you change one, change both.
- Update this file in each release (see [RELEASING.md](../../RELEASING.md)).
  "Checked" is the date when someone last read the row against the file
  and the code.
- capnp-zig pins tagged Zig 0.17.0 (`mise.toml`) and quic-zig v0.38.0
  (`build.zig.zon`).
- Downstream asks to capnp-zig are in [the downstream ledger](../downstreams.md).

## quic-zig

| Handoff | Asks for | Status | Where fixed | Workaround to delete | Checked |
|---|---|---|---|---|---|
| [handoff-quic-zig-at-rest-stream-gc.md](handoff-quic-zig-at-rest-stream-gc.md) | Run the stream GC on a connection "at rest". | fixed | quic-zig v0.37.2 (`51a34c0`, 2026-10-08) | None. capnp-zig moved to v0.37.2 in v0.24.0 and dropped it. | 2026-10-17 |
| [handoff-quic-zig-handshake-timer.md](handoff-quic-zig-handshake-timer.md) | A handshake liveness timer. | fixed | quic-zig v0.19.0 (`c1cb50e`) | None. capnp-zig keeps `ServerOptions.handshake_timeout_ms` as defense in depth, by design. | 2026-10-17 |
| [handoff-quic-zig-ticket-keys.md](handoff-quic-zig-ticket-keys.md) | Session-ticket keys as a server config, three follow-up candidates, and a way to move the Debug loop-thread latch. | delivered | quic-zig v0.27.0 (`9d2ab6e`), v0.29.0 (`b9a15e6`) and v0.30.1 (`ccf6ae2`, `Server.adoptLoopThread`) | None. capnp-zig uses all of it. | 2026-10-17 |
| [handoff-quic-zig-write-budget-headroom.md](handoff-quic-zig-write-budget-headroom.md) | A local `streamWrite` must not fill the whole memory budget. | fixed | quic-zig v0.38.0 (`77be067`, 2026-10-09) | None. capnp-zig v0.25.0 dropped its half-budget write cap. One rule stays with the embedder (see the file). | 2026-10-17 |

Not written yet: owner decision D1 (option B) in
[sprint-plan-2026-10-15.md](../sprint-plan-2026-10-15.md) asks for a
quic-zig handoff. It asks quic-zig to tag a release that contains
`021591e`, with CHANGELOG and DOWNSTREAM-NOTE entries.

## Zig fork

| Handoff | Asks for | Status | Where fixed | Workaround to delete | Checked |
|---|---|---|---|---|---|
| [handoff-zig-fork-debug-allocator-slabs.md](handoff-zig-fork-debug-allocator-slabs.md) | `std.heap.DebugAllocator` must give back the slabs of emptied buckets. | open: DRAFT from capnp-swift (2026-10-06), a copy of its `docs/handoffs/` file. This copy takes its session counts and resident sizes from capnp-swift `docs/plan-2026-10-06.md`. capnp-swift's copy still has the old figures. Not checked against the fork. | none | `tests/native/fuzz_abi.zig` counts live bytes over `std.heap.page_allocator` (`CountingAllocator`) instead of a `DebugAllocator`. | 2026-10-17 |
| [handoff-zig-fork-dispatch-operation-switch.md](handoff-zig-fork-dispatch-operation-switch.md) | A non-exhaustive `Io.Operation` switch in `Io/Dispatch`. | superseded (checked 2026-10-03) | Zig 0.17.0 handles every variant. The net operations still `@panic("TODO")`, which is feature work for the fork's `feat/evented-dispatch`. | None of its own. Evented stays off for the processReplacePath row. | 2026-10-17 |
| [handoff-zig-fork-evented-processreplacepath.md](handoff-zig-fork-evented-processreplacepath.md) | The evented backends set `Io.VTable` fields that no longer exist. | open: `zig build check-evented-canary` still passes on 0.17.0 (it passes only while the compile fails) | none | `evented_available = false` in `src/io_backend.zig`, and the expected-fail canary (`tools/evented_canary.zig`, `build/evented_canary.zig`). When the canary goes red, set the flag. | 2026-10-17 |
| [handoff-zig-fork-fetch-recompress-root.md](handoff-zig-fork-fetch-recompress-root.md) | A standalone `zig fetch` must recompress from the stripped package root. | open at 0.17.0. capnp-swift filed the same defect as its `handoff-zig-fork-fetch-cache-strip.md`. | none | No code. Fetch inside a project (`zig fetch --save`, or `zig build`), never a bare `zig fetch <url>` into a shared cache. | 2026-10-17 |
| [handoff-zig-fork-ios-nullfile.md](handoff-zig-fork-ios-nullfile.md) | `Io.Threaded` must compile for iOS. | open: DRAFT from capnp-swift (its H2). The fork's `feat/evented-0.17` checkout still has the defect. | Upstream Zig commit `f842de1845` (2026-10-04, "std.process: match can_spawn and can_replace to Apple SDK availability") changes both files of the defect. No Zig tag has it (the newest tag is 0.17.0). Not built or tested here. | The three std overrides in `tests/apple/apple_check_root.zig` (capnp-swift's root has the same three). `fd_passing.target_supported` names this defect as one of its two reasons to leave out the iOS family, so check it again too. | 2026-10-17 |
| [handoff-zig-fork-netacceptwindows.md](handoff-zig-fork-netacceptwindows.md) | `netAcceptWindows` must not reach `unreachable` on `STATUS_CANCELLED`. | open in std (status line 2026-10-17) | none | `src/rpc/transport/tcp/windows_accept.zig`, the own AFD accept since `8c36f35` (for v0.27.0). `Listener` and `WorkerPool` use it. | 2026-10-17 |
| [handoff-zig-fork-netreadwindows-stray-cancel.md](handoff-zig-fork-netreadwindows-stray-cancel.md) | Windows socket reads: a stray `STATUS_CANCELLED`, and the read, write and shutdown statuses that std does not map. | open (status line 2026-10-17). New for v0.27.0; not sent yet. | none | In `src/rpc/transport/tcp/stream_transport.zig`: the own AFD receive `windowsReceive` (since v0.26.0) and the local-disconnect arms in `windowsReceiveResult` (for v0.27.0). The write and shutdown sides have no workaround. | 2026-10-17 |
| [handoff-zig-fork-scm-rights.md](handoff-zig-fork-scm-rights.md) | Safe SCM_RIGHTS control messages in `std.Io.net`. | open at 0.17.0 (written 2026-10-04) | none | `src/rpc/transport/unix/fd_io.zig` calls `recvmsg` and `sendmsg` itself and parses the control buffer with its own clamped parser. | 2026-10-17 |
| [handoff-zig-fork-stream-read-readresult.md](handoff-zig-fork-stream-read-readresult.md) | `net.Stream.read` must compile: it must destructure `ReadResult`. | open at 0.17.0 (written 2026-10-03) | none | `stdNetReadVec` and `netReadLen` in `src/rpc/transport/tcp/stream_transport.zig`, and the wake-channel drain in `src/rpc/transport/tcp/connection.zig`, submit `net_read` themselves instead of calling `Stream.read`. | 2026-10-17 |
| [handoff-zig-fork-unix-address.md](handoff-zig-fork-unix-address.md) | AF_UNIX addresses and `Socket.createPair` in `std.Io.net`. | open at 0.17.0 (written 2026-10-04) | none | `src/rpc/transport/unix/socket.zig` uses raw `socket`, `bind`, `listen` and `socketpair` calls, and refuses abstract names. | 2026-10-17 |
