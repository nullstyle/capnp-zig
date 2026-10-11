# Upgrading to capnp-zig v0.26.0

This guide is for projects that depend on capnp-zig. v0.26.0 is a Windows
reliability release. It bundles these changes:

- Fixes for socket reads on Windows. Windows could cancel a receive
  although nothing had asked it to. In CI this was seen right after a
  timed read whose deadline raced arriving data on the same socket. A
  timed read then failed with `error.Unexpected`, and an untimed read
  (`Transport.read`, and the `Connection` read loop) hit an `unreachable`
  in std 0.17.0's `netRead`: a panic in Debug and ReleaseSafe, undefined
  behavior in ReleaseFast and ReleaseSmall. With std's Threaded Io (what
  `process_init` and `threaded` give), every Windows socket read of the
  transport now posts its receive again in that case. An untimed read on
  an Io that cannot run the transport's receive batch concurrently still
  uses that Io's own read, without the re-post.
- The e2e server and client can run over QUIC (`--transport quic`).
- The schema tooling runs on Deno instead of Python. This matters only to
  contributors who regenerate bindings in this repository.
- The QUIC guide lists the frozen baseline wire constants.

The Zig toolchain does not change: it stays at tagged 0.17.0. quic-zig and
the http3-zig pair do not change. Serialization, codegen, QUIC and RPC on
Linux and macOS do not change. No Stable API line changes, no Experimental
snapshot line moves, and there is no Breaking entry. The `0.26.0` section of
[CHANGELOG.md](../CHANGELOG.md) is the authoritative list of changes.

## Who should upgrade

- **Every Windows user of the TCP transport.** That covers
  `Transport.readTimeout`, `Transport.read`, and `Connection`. The cause of
  the stray cancellation is inside Windows and is not known. In CI it was
  seen only after a timed read whose deadline raced arriving data, on the
  same socket and thread. A reader that does a deadline read before its
  blocking reads makes that sequence. A `Connection` makes it across two
  threads when code calls `conn.transport.readTimeout` before `run` (a
  `WorkerPool` accept hook, for example). Tests recreate that case, but CI
  has not shown it. In v0.25.0, an untimed read hit std's `unreachable` on
  any cancellation of its receive that std did not ask for, including a
  `CancelIoEx` from outside the transport (see item 4 below).
- Linux and macOS users get no behavior change, other than the e2e and
  tooling changes above.

This release has no security advisory.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.26.0` | `capnpc_zig-0.26.0-...` |
| quic-zig | `v0.38.0` (tag at `77be067`; no change since v0.25.0) | `quic-0.38.0-DnSYve_hPwBp5d2c8QUoZiNWi-rHT6QFR4pK2DueXEAC` |
| http3-zig (optional) | `v0.5.7` (tag at `e9867fc`; no change since v0.25.0) | `http3_zig-0.5.7-ayZ03AxnEwCS-38QeydGjOHQ1sAQEWT8xWjf82hAOPBw` |

capnp-zig v0.26.0 pins the same quic-zig release as v0.25.0, with the same
dependency options, so http3-zig v0.5.7 still links next to it with one quic
module. That was measured with v0.25.0 at the v0.5.7 tag, not again for
v0.26.0.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.26.0
   ```

   Codegen does not change (codegen ABI 2), so files from the 0.23.0 to
   0.25.0 plugins compile against this runtime.

2. **On Windows, end a waiting read with an Io cancellation.** Only an Io
   cancellation of the reading task, or a timed read's deadline, ends a
   read that waits for data. `Transport.shutdown` and `close` do not end a
   pending receive on Windows, as before. They set the closing flag, so a
   receive that Windows cancels after them ends the read with 0.

3. **Do not close the raw handle under a pending read.** Call
   `Transport.close` or `shutdown` first. If Windows cancels the receive,
   the one posted again would fail on the closed handle with
   `error.Unexpected`, or would read from another object if Windows has
   reused the handle value. `deinit` under a pending read stays unsafe,
   because it frees the read buffer.

4. **A `CancelIoEx` from outside the transport now leaves a read
   waiting.** In v0.25.0 it acted like the stray cancellation: an untimed
   read hit std's `unreachable`, and a timed read failed with
   `error.Unexpected`. Now the receive is posted again and the read keeps
   waiting. To end a read, use an Io cancellation.

5. **A read gives up only after a long run of cancellations.** If Windows
   keeps cancelling a read's receives, the read fails with
   `error.Unexpected` only after 10 s. In Windows CI run 38083790370, on
   this release's code with debug logging added, each of the 53 reads that
   met a stray cancellation posted its receive again at most twice. Each
   of those reads ended at most about 16 ms after its first cancellation.
   That time includes the wait for data or for the deadline. In the earlier
   run 38079015619, a candidate build that posted the receive again at
   once each time, one read met 9 cancellations in a row.

6. **Contributors who regenerate bindings in this repository need Deno.**
   `mise.toml` pins it. Run `mise run bootstrap:capnp` once after the
   upgrade. Projects that only depend on capnp-zig need nothing new.

## What is new

Read the `0.26.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with the tests behind each fix and the CI runs behind its numbers.
