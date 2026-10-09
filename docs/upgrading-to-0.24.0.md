# Upgrading to capnp-zig v0.24.0

This guide is for projects that depend on capnp-zig. v0.24.0 is a QUIC
release, paired with http3-zig v0.5.6. It bundles these changes:

- quic-zig moves from v0.32.0 to v0.37.2. One stream reaches the path's
  rate on the default windows, quic-zig's part of an idle connection is
  about 22 KB of heap (was about 92 KB), and every probe timeout carries
  previously sent data.
- A reply larger than a server's `max_connection_memory` no longer ends the
  session. capnp-zig's own stream writes stop at half of that budget, so
  the peer's frames always have room.
- The move found two quic-zig defects with idle connections (a stream that
  ended was not freed, and a server connection never came to rest).
  quic-zig v0.37.2 fixes both, so capnp-zig needs no workaround.
- Host answer cancellation over the WASM host ABI (feature bit `12`).

The Zig toolchain does not change: it stays at tagged 0.17.0.
Serialization, codegen and TCP RPC do not change, and no Stable API line
changes. One entry is Breaking (17 Experimental QUIC error sets gain one
error), and it has a **Migration** paragraph in the `0.24.0` section of
[CHANGELOG.md](../CHANGELOG.md). That section is the authoritative list of
changes.

## Who should upgrade

- **Every QUIC user.** Throughput is higher, and a reply larger than a
  server's memory budget works. On v0.23.0 and older, such a reply ended
  the session with `ExcessiveLoad`.
- **Projects that also link http3-zig.** Move to http3-zig v0.5.6 in the
  same change. It pins the same quic-zig, so the program has one quic
  module.
- **WASM hosts that answer calls later.** Feature bit `12` tells you when a
  caller gives up on a call, so you can stop the work
  (`docs/wasm_host_abi.md`).

Projects that do not build with `-Dquic=true` and do not use the WASM host
ABI get no behavior change. This release has no security advisory.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.24.0` | `capnpc_zig-0.24.0-...` |
| quic-zig | `v0.37.2` (tag at `51a34c0`) | `quic-0.37.2-DnSYvb2bPwDUgMMPDFV3tX0CgvsCbn2u2ed60n4tUtZ-` |
| http3-zig (optional) | `v0.5.6` (tag at `6566fa2`) | `http3_zig-0.5.6-ayZ03MVFEwB0H8S3-2BwZg_r2C92DUPQAPlUWF4SkWuU` |

capnp-zig v0.24.0 and http3-zig v0.5.6 link into one program with one quic
module and one BoringSSL (measured at the v0.5.6 tag, Debug and
ReleaseSafe). http3-zig v0.5.5 pins quic-zig v0.32.0 and pairs with
capnp-zig v0.23.0 and v0.22.0.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.24.0
   ```

   Codegen does not change (codegen ABI 2, as in v0.23.0), so files from
   the 0.23.0 plugin compile against this runtime.

2. **Move the other quic pins in the same change.** A build that also
   depends on quic-zig or http3-zig must pin quic-zig v0.37.2 (http3-zig
   v0.5.6) with the same options as capnp-zig: `.target`,
   `.release = optimize != .debug`, `.@"sanitize-c" = "trap"`. Otherwise
   the build makes two quic modules, each with its own BoringSSL.

3. **Add `error.AckFrequencyNotNegotiated` to exhaustive switches** over
   the Experimental QUIC error sets that the Breaking entry lists. capnp-zig
   never returns it in practice: only `requestAckFrequency` and
   `requestImmediateAck` do, and capnp-zig calls neither.

4. **Keep `max_connection_memory` at least twice the announced connection
   window.** The defaults (32 MiB budget, 16 MiB window) already match. The
   budget bounds what a connection holds, not what the peer sends, so a
   smaller budget can still end a connection when an honest peer sends
   ahead of the reader ("Current Limits" in docs/quic-transport.md).

5. **Expect more memory per busy connection.** For a reader that keeps up,
   receive windows grow up to 16 MiB and send buffers follow the peer's
   credit, all inside `max_connection_memory`. A slow reader's windows
   never grow. Budget servers with `ServerOptions.max_connection_memory`.

6. **Native mode: frames over about 2 MiB stall with the default
   windows.** This is not new (v0.23.0 stalls the same way). Use baseline
   mode for such frames, or raise the receiver's
   `transport_params.initial_max_stream_data_uni` above the largest frame.

7. **An `EmbeddedSession` host keeps the same loop.** Feed, then service,
   then tick, as before ("Embedder rules" in docs/quic-transport.md). No
   extra call is needed on quic-zig v0.37.2.

## What is new

Read the `0.24.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with measurements.
