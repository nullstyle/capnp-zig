# Upgrading to capnp-zig v0.26.0

This guide is for projects that depend on capnp-zig. v0.26.0 is a Windows
reliability release. It bundles these changes:

- Fixes for socket reads on Windows. After a timed read whose deadline
  raced arriving data, Windows could cancel the next receive on that socket
  although nothing had asked it to. A timed read then failed with
  `error.Unexpected`, and an untimed read (`Transport.read`, and the
  `Connection` read loop) aborted the process, because std 0.17.0's
  `netRead` treats that cancellation as unreachable. Every Windows socket
  read of the transport now posts its receive again in that case.
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
  `Transport.readTimeout`, `Transport.read`, and `Connection`. The crash
  needs a timed read before an untimed read on the same socket. That
  happens when code calls `conn.transport.readTimeout` before `run` (a
  `WorkerPool` accept hook, for example), or when a reader does a deadline
  read before its blocking reads.
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

capnp-zig v0.26.0 pins the same quic-zig release as v0.25.0, so http3-zig
v0.5.7 still links next to it with one quic module.

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

4. **A `CancelIoEx` from outside the transport no longer ends a read.** It
   looks like the stray cancellation: the receive is posted again and the
   read keeps waiting. Use an Io cancellation instead.

5. **A read gives up only after a long run of cancellations.** If Windows
   keeps cancelling a read's receives, the read fails with
   `error.Unexpected` only after 10 s. In CI, a stray cancellation needed
   at most two re-posts and about 16 ms.

6. **Contributors who regenerate bindings in this repository need Deno.**
   `mise.toml` pins it. Run `mise run bootstrap:capnp` once after the
   upgrade. Projects that only depend on capnp-zig need nothing new.

## What is new

Read the `0.26.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with the tests and CI evidence behind each fix.
