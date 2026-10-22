# Upstream handoffs

This file is the ledger of the handoffs that capnp-zig gives to its
upstreams: quic-zig, and the nullstyle Zig fork. Each handoff is a file in
this directory. It is a document for the owner to relay, not a filed
issue.

- One row per handoff file. Two rows at the end of the Zig table have no
  file; they record std workarounds that no handoff asks about yet.
- "Where fixed" gives the upstream ref and commit. "Workaround to delete"
  names the capnp-zig code that exists only because of the defect, and
  that can go when capnp-zig pins a fixed release.
- A status line at the top of a handoff file wins over this table. When
  you change one, change both.
- Update this file in each release (see [RELEASING.md](../../RELEASING.md)).
  "Checked" is the date when someone last read the row against the file
  and the code.
- capnp-zig pins tagged Zig 0.17.0 (`mise.toml`) and quic-zig v0.38.0
  (`build.zig.zon`).
- Downstream asks to capnp-zig are in [the downstream ledger](../downstreams.md).

The refs that the 2026-10-21 rows were read against (`git ls-remote` on
2026-10-21, then a read-only fetch into a scratch clone):

- Upstream Zig: `0.17.x` at `c99f5c98c5` and `master` at `6817928c37`,
  both 2026-10-21. The newest Zig tag is still `0.17.0` (`7647adab80`). A
  fix that is only on `0.17.x` reaches capnp-zig with a Zig tag after
  0.17.0 and a capnp-zig toolchain move.
- The nullstyle fork: `build/stable` at `d80d6d31bf` (its release tag
  `stable/20261011-0550-d80d6d3`) and `feat/evented-0.17` at `017b3e4966`,
  both 2026-10-10. Both branch from the 0.17.0 tag, so they do not have
  the `0.17.x` fixes. capnp-zig does not build with the fork.
- quic-zig: `main` at `b0d7d78` (2026-10-13). The newest tag is v0.42.2
  (`2aa5bae`).

## Handoffs to send

The owner sends these four. Each one is still open.

1. [handoff-zig-fork-netacceptwindows.md](handoff-zig-fork-netacceptwindows.md),
   to the fork. All three `.CANCELLED => unreachable` arms are still in
   upstream `0.17.x`, upstream `master` and the fork.
2. [handoff-zig-fork-netreadwindows-stray-cancel.md](handoff-zig-fork-netreadwindows-stray-cancel.md),
   to the fork. It covers the read, write and shutdown sides. Tell the fork
   that upstream `0.17.x` `c9f7ad0e57` already maps `LOCAL_DISCONNECT` on
   read and write. The other asks are open everywhere. The fork can carry
   this and item 1 on one branch.
3. [handoff-zig-fork-unix-address.md](handoff-zig-fork-unix-address.md),
   only defects 3 and 4, to the fork. The fork fixed defects 1 and 2 in
   `03263c1c44`. The status line of the file gives the delta.
4. [handoff-quic-zig-tag-hardening.md](handoff-quic-zig-tag-hardening.md),
   to quic-zig: a tag that contains `021591e`.

The other files need no message now:

- The four older quic-zig files: a quic-zig tag fixed or delivered each
  one.
- `handoff-zig-fork-debug-allocator-slabs.md` and
  `handoff-zig-fork-fetch-recompress-root.md`: capnp-swift wrote the same
  drafts (`handoff-zig-fork-debug-allocator-slabs.md` and
  `handoff-zig-fork-fetch-cache-strip.md` in its `docs/handoffs/`), and
  nothing blocks on them. capnp-zig's workarounds are a test allocator and
  a rule for running `zig fetch`. If the owner sends them, send one copy
  of each.
- `handoff-zig-fork-dispatch-operation-switch.md`: superseded by Zig
  0.17.0.
- `handoff-zig-fork-evented-processreplacepath.md`,
  `handoff-zig-fork-ios-nullfile.md` and
  `handoff-zig-fork-stream-read-readresult.md`: upstream `0.17.x` has each
  fix. The fork already has the evented and `Stream.read` fixes
  (`b98ef17f06`). It gets the iOS fix when it rebases onto a later 0.17
  release.
- `handoff-zig-fork-scm-rights.md`: the fork has all four fixes
  (`03263c1c44`).
- The two rows with no file: they need a public socket-option call in
  `std.Io.net`. That is new std API, not a fix, and the capnp-zig
  workarounds are small and safe. Write one handoff for both when the fork
  plans socket options.

## quic-zig

| Handoff | Asks for | Status | Where fixed | Workaround to delete | Checked |
|---|---|---|---|---|---|
| [handoff-quic-zig-at-rest-stream-gc.md](handoff-quic-zig-at-rest-stream-gc.md) | Run the stream GC on a connection "at rest". | fixed | quic-zig v0.37.2 (`51a34c0`, 2026-10-08) | None. capnp-zig moved to v0.37.2 in v0.24.0 and dropped it. | 2026-10-21 |
| [handoff-quic-zig-handshake-timer.md](handoff-quic-zig-handshake-timer.md) | A handshake liveness timer. | fixed | quic-zig v0.19.0 (`c1cb50e`) | None. capnp-zig keeps `ServerOptions.handshake_timeout_ms` as defense in depth, by design. | 2026-10-21 |
| [handoff-quic-zig-ticket-keys.md](handoff-quic-zig-ticket-keys.md) | Session-ticket keys as a server config, three follow-up candidates, and a way to move the Debug loop-thread latch. | delivered | quic-zig v0.27.0 (`9d2ab6e`), v0.29.0 (`b9a15e6`) and v0.30.1 (`ccf6ae2`, `Server.adoptLoopThread`) | None. capnp-zig uses all of it. | 2026-10-21 |
| [handoff-quic-zig-write-budget-headroom.md](handoff-quic-zig-write-budget-headroom.md) | A local `streamWrite` must not fill the whole memory budget. | fixed | quic-zig v0.38.0 (`77be067`, 2026-10-09) | None. capnp-zig v0.25.0 dropped its half-budget write cap. One rule stays with the embedder (see the file). | 2026-10-21 |
| [handoff-quic-zig-tag-hardening.md](handoff-quic-zig-tag-hardening.md) | A tag at `b0d7d78` or later that contains `021591e`, with CHANGELOG and DOWNSTREAM-NOTE entries. | open: written 2026-10-21, not sent yet (owner decision D2 = B in [sprint-plan-2026-10-21.md](../sprint-plan-2026-10-21.md)) | none: `021591e` and `b0d7d78` are on quic-zig `main`, and `git tag --contains 021591e` prints nothing | None. capnp-zig stays on v0.38.0 and moves in its own release after the tag exists. | 2026-10-21 |

## Zig fork

| Handoff | Asks for | Status | Where fixed | Workaround to delete | Checked |
|---|---|---|---|---|---|
| [handoff-zig-fork-debug-allocator-slabs.md](handoff-zig-fork-debug-allocator-slabs.md) | `std.heap.DebugAllocator` must give back the slabs of emptied buckets. | open: DRAFT from capnp-swift (2026-10-06), a copy of its `docs/handoffs/` file. This copy takes its session counts and resident sizes from capnp-swift `docs/plan-2026-10-06.md`. capnp-swift's copy still has the old figures. Not checked against the fork. | none: no commit after 0.17.0 changes `lib/std/heap/debug_allocator.zig` on upstream `0.17.x` or the fork. Upstream `master` `969db474f3` (2026-10-21) changes only the `ArrayList` type in its tests. | `tests/native/fuzz_abi.zig` counts live bytes over `std.heap.page_allocator` (`CountingAllocator`, `:58`) instead of a `DebugAllocator`. | 2026-10-21 |
| [handoff-zig-fork-dispatch-operation-switch.md](handoff-zig-fork-dispatch-operation-switch.md) | A non-exhaustive `Io.Operation` switch in `Io/Dispatch`. | superseded (checked 2026-10-03) | Zig 0.17.0 handles every variant. The net operations are still `@panic("TODO")` (`lib/std/Io/Dispatch.zig:1713-1716` at 0.17.0 and on upstream `0.17.x`), which is feature work for the fork's `feat/evented-dispatch`. | None of its own. Evented stays off; see the next row. | 2026-10-21 |
| [handoff-zig-fork-evented-processreplacepath.md](handoff-zig-fork-evented-processreplacepath.md) | The evented backends set `Io.VTable` fields that no longer exist. | fixed upstream and in the fork, in no Zig tag. `zig build check-evented-canary` still passes on 0.17.0 (it passes only while the compile fails). | Upstream `0.17.x`: Uring in `6080247206` (2026-10-10, "Io.Uring fixes & updates"), Dispatch in `ff08725400` (2026-10-21, "fix(Io.Dispatch): synchronize with updates to VTable"). Fork `build/stable` and `feat/evented-0.17`: `b98ef17f06` (2026-10-10). | `evented_available = false` (`src/io_backend.zig:51`) and the expected-fail canary (`tools/evented_canary.zig`, `build/evented_canary.zig`). The canary goes red when capnp-zig pins a Zig with the fix. A compile is not enough to set the flag: at 0.17.0, both backends answer listen, accept and connect with `...Unavailable` (`Uring.zig:774-777`, `Dispatch.zig:454-457`), Uring panics on `net_write` (`Uring.zig:2120`), and Dispatch panics on `net_receive`, `net_send`, `net_read` and `net_write` (`Dispatch.zig:1713-1716`). Upstream `0.17.x` still has these stubs. Run a net_read behavior probe first. | 2026-10-21 |
| [handoff-zig-fork-fetch-recompress-root.md](handoff-zig-fork-fetch-recompress-root.md) | A standalone `zig fetch` must recompress from the stripped package root. | open. capnp-swift filed the same defect as its `handoff-zig-fork-fetch-cache-strip.md`. | none: `f.package_root = tmp_directory_path;` is still in `lib/compiler/Maker/Fetch.zig` on upstream `0.17.x` (`:802`), upstream `master` (`:807`) and the fork's `build/stable` (`:792`). | No code. Fetch inside a project (`zig fetch --save`, or `zig build`), never a bare `zig fetch <url>` into a shared cache. | 2026-10-21 |
| [handoff-zig-fork-ios-nullfile.md](handoff-zig-fork-ios-nullfile.md) | `Io.Threaded` must compile for iOS. | fixed upstream, in no Zig tag. DRAFT from capnp-swift (its H2). The fork still has the defect (`Threaded.zig:358` on `build/stable`). | Upstream `0.17.x`: `f842de1845` ("std.process: match can_spawn and can_replace to Apple SDK availability") and `6171a2be74` ("std.Io.Threaded: only use spawnDarwin in processReplace when spawning is supported", closes upstream issue 37061), both 2026-10-04. Not built or tested here. | The three std overrides in `tests/apple/apple_check_root.zig` (`:44`, `:52`, `:53`; capnp-swift's root has the same three). `fd_passing.target_supported` (`src/rpc/transport/fd_passing.zig:25`) names this defect as one of its two reasons to leave out the iOS family, so check it again too. | 2026-10-21 |
| [handoff-zig-fork-netacceptwindows.md](handoff-zig-fork-netacceptwindows.md) | `netAcceptWindows` must not reach `unreachable` on `STATUS_CANCELLED`. | open in std and the fork. Send it. | none: the three arms are at `Threaded.zig:12941`, `:12965` and `:12986` on upstream `0.17.x`, `:12984`, `:13008` and `:13029` on upstream `master`, and `:12867`, `:12891` and `:12912` on the fork's `build/stable`. | `src/rpc/transport/tcp/windows_accept.zig` (`accept`, `:102`; `stopWaits`, `:270`), the own AFD accept since `8c36f35` (v0.27.0). `Listener` and `WorkerPool` use it. | 2026-10-21 |
| [handoff-zig-fork-netreadwindows-stray-cancel.md](handoff-zig-fork-netreadwindows-stray-cancel.md) | Windows socket reads: a stray `STATUS_CANCELLED`, and the read, write and shutdown statuses that std does not map. | open, partly fixed upstream. Send it. | Upstream `0.17.x` `c9f7ad0e57` (2026-10-10, "map LOCAL_DISCONNECT to ConnectionResetByPeer on Windows", a partial fix of upstream issue 31956) maps `LOCAL_DISCONNECT` in `netReadWindows` and `netWriteWindows`. Still open on upstream `0.17.x`, `master` and the fork: `.CANCELLED => unreachable` in `netReadWindows` (`Threaded.zig:13115` on `0.17.x`) and `netWriteWindows` (`:13629`), the `batchApc` path, `CONNECTION_ABORTED` on read, `CONNECTION_ABORTED`, `PIPE_DISCONNECTED` and `INVALID_DEVICE_STATE` on write, and `CONNECTION_RESET` on shutdown. The fork does not have `c9f7ad0e57`. | In `src/rpc/transport/tcp/stream_transport.zig`: the own AFD receive `windowsReceive` (`:2091`, since v0.26.0) with `SpuriousCancelRetry` (`:2304`), and the local-disconnect arms in `windowsReceiveResult` (`:2380-2387`, since v0.27.0). The write and shutdown sides have no workaround. | 2026-10-21 |
| [handoff-zig-fork-scm-rights.md](handoff-zig-fork-scm-rights.md) | Safe SCM_RIGHTS control messages in `std.Io.net`. | fixed in the fork, not upstream. | Fork `03263c1c44` (2026-10-10, "Fix Unix address bounds and ancillary descriptor ownership"), on `build/stable` and `feat/evented-0.17` and in the fork tag `stable/20261011-0550-d80d6d3`. It covers all four defects, in its own shape: it captures the Darwin rights message before truncation and closes the descriptors it does not deliver (1), names EMFILE and ENFILE (2), returns `error.InvalidControlMessage` for EBADF and EINVAL when control data is present (3), and sets CLOEXEC on Darwin after `recvmsg` (4). Read from the diff; not built or tested here. Upstream `0.17.x` and `master` have none of it. | `src/rpc/transport/unix/fd_io.zig` calls `recvmsg` (`:298`) and `sendmsg` (`:553`) itself and parses the control buffer with its own clamped parser. It stays while capnp-zig pins stock Zig. | 2026-10-21 |
| [handoff-zig-fork-stream-read-readresult.md](handoff-zig-fork-stream-read-readresult.md) | `net.Stream.read` must compile: it must destructure `ReadResult`. | fixed upstream and in the fork, in no Zig tag. | Upstream `0.17.x` `c629ee4e08` (2026-10-04, "Io.net.Stream: fix read post control data changes"). Fork `b98ef17f06` (`lib/std/Io/net.zig:1328-1334` on `build/stable`). | `stdNetReadVec` (`:1953`) and `netReadLen` (`:1968`) in `src/rpc/transport/tcp/stream_transport.zig`, and the wake-channel drain in `src/rpc/transport/tcp/connection.zig` (`:565`), submit `net_read` themselves instead of calling `Stream.read`. They can go after a toolchain move to a Zig with the fix. | 2026-10-21 |
| [handoff-zig-fork-unix-address.md](handoff-zig-fork-unix-address.md) | AF_UNIX addresses and `Socket.createPair` in `std.Io.net`. | defects 1 and 2 fixed in the fork; defects 3 and 4 open everywhere. Send the delta. | Fork `03263c1c44`: `UnixAddress.max_len` comes from `sockaddr.un` (`lib/std/Io/net.zig:881-884` on `build/stable`), and a Linux abstract name gets no trailing NUL. Still open on the fork and upstream: `CreatePairOptions.family` is `IpAddress.Family` (`net.zig:1293` on `build/stable`, `:1268` on `0.17.x`), and Darwin `OPNOTSUPP` is 45 (`lib/std/c/darwin.zig:1392`). Upstream `0.17.x` has all four defects (`max_len` is still 108, `net.zig:860-863`). | `src/rpc/transport/unix/socket.zig` uses raw `socket`, `bind` and `listen` calls (`:547`, `:593`, `:624`), and refuses abstract names. The fd tests call `socketpair` themselves (`tests/rpc/transport/unix/fd_test_support.zig:57`). | 2026-10-21 |
| none (no file) | `std.posix.setsockopt` must not treat EBADF and EINVAL as `unreachable`. On an accepted socket whose peer resets it, macOS returns EINVAL. | not written; not worth sending now (see "Handoffs to send") | none: `lib/std/posix.zig:1085-1088` on upstream `0.17.x` (`:1075` is the function at 0.17.0). Its Windows path is `@compileError("use std.Io instead")` (`:1081`). | `setTcpNoDelay` calls `std.posix.system.setsockopt` itself and logs a failure (`src/rpc/transport/tcp/runtime.zig:354-377`). | 2026-10-21 |
| none (no file) | A public way to set `TCP_NODELAY` on a Windows socket. std's sockets are raw AFD handles, so `ws2_32.setsockopt` rejects them. | not written; not worth sending now (see "Handoffs to send") | none: std's AFD helper `setSocketOptionAfd` is private (`lib/std/Io/Threaded.zig:12381` at 0.17.0, `:12486` on `0.17.x`, `:12527` on `master`), and `std.Io.net` has no socket-option call. | `setTcpNoDelay` returns at once on Windows, so Nagle stays on there (`src/rpc/transport/tcp/runtime.zig:342-349`; phase 4 of [windows-first-class-plan.md](../windows-first-class-plan.md)). | 2026-10-21 |
