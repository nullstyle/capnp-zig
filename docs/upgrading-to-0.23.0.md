# Upgrading to capnp-zig v0.23.0

This guide is for projects that depend on capnp-zig. v0.23.0 is an RPC
correctness release. It bundles these changes:

- Fixes for capabilities that reached the wrong object, without an error:
  a generated `setXClient` that passed an imported capability back, and
  loopback calls (calls on your own exports) that read their capabilities
  from the remote's side.
- A capability pipelined into another call's params now resolves for the
  C++, Go, Rust and Python reference clients (capnp-swift handoff H9).
- Every question gets exactly one terminal, also under memory pressure,
  after a cancel, and for a forwarded call whose caller sent Finish first.
- New Experimental embedder hooks for capnp-swift (H5), a retained
  bootstrap, and a `type_resolver` facade for generics.
- The Experimental `native` module: capnp-swift's sans-IO RPC connection
  and its C ABI now live in capnp-zig (handoff H7, `docs/native-abi.md`).
- Level-3 vat hosting over the WASM host ABI (feature bit `11`).
- Every Zig block in the README and the serialization guide now compiles on
  Zig 0.17.0, and a gate keeps it that way.

The Zig toolchain does not change: it stays at tagged 0.17.0. No Stable API
line changes. One entry is Breaking (generated code needs the 0.23.0
runtime), and it has a **Migration** paragraph in the `0.23.0` section of
[CHANGELOG.md](../CHANGELOG.md). That section is the authoritative list of
changes.

## Who should upgrade

- **Every RPC user.** If your code passes a capability it received back to
  the side it came from (through a generated `setXClient`), v0.22.0 and
  older could send one of your own exports with the same id instead. The
  remote then called the wrong object, and no error was raised.
- **Servers that talk to C++, Go, Rust or Python clients.** A client that
  pipelines a capability into another call's params got an exception from
  a capnp-zig server. v0.23.0 resolves it before the handler runs.
- **Code that calls its own exports through the Peer** (`Peer.sendCallResolved`
  with an `.exported` target, or a generated local Client). Capabilities in
  the params and results of such a call reached the remote's objects, took
  references nobody released, and sent stray Release frames.
- **capnp-swift and other C or Swift hosts.** Read `docs/native-abi.md`.

This release has no released security advisory. QUIC is unchanged: the pin
stays at quic-zig v0.32.0.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.23.0` (tag at `aca9824`) | `capnpc_zig-0.23.0-nUduFUsRTgCn5wyHqoKC0ZXgBemZlU7y6bWBmH3RpW9U` |
| quic-zig | `v0.32.0` (tag at `ffdb251`; no change since v0.22.0) | `quic-0.32.0-DnSYvcGOPADCEZMispvPbD9RznRtyIGq77zHIgmS8iVl` |
| http3-zig (optional) | `v0.5.5` (tag at `380ead3`) | `http3_zig-0.5.5-ayZ03DI5EwD2bajKDfR009PM7jlRI3lzSnrmT2PlWgS4` |

capnp-zig v0.23.0 pins the same quic-zig release as v0.22.0, so http3-zig
v0.5.5 still links next to it with one quic module.

## Checklist for every consumer

1. **Bump the pin, and regenerate with the same release's plugin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.23.0
   ```

   Generated code from the 0.23.0 plugin needs the 0.23.0 runtime (codegen
   ABI 2). A file from a newer plugin compiled against an older runtime
   stops at a one-line version-skew error. Build the plugin from the same
   package: `b.dependency("capnpc_zig", ...).artifact("capnpc-zig")`
   (docs/build-integration.md). Files from older plugins still compile.

2. **A pipelined param that names a failed answer now fails the call.** The
   Peer answers the call with a copy of that answer's exception, and the
   handler does not run. Before, the handler ran with a `.promised` entry.
   The C++ reference instead passes a broken capability.

3. **Hand-built `Client.init(peer, id)` still means "export first".** The
   generated code now records where a Client's id comes from
   (`Client.origin`). A Client you build by hand from an import id should
   set `.origin = .imported`, or it can still collide with a local export.

4. **Loopback calls with a pipelined capability fail closed.** A
   `receiverAnswer` in the params or results of a call on your own export is
   refused with `error.LoopbackPromisedCapabilityUnsupported`.

5. **Cancelling a call on your own export** keeps its question id and its
   `max_loopback_questions` slot until the handler answers, the same as a
   remote call. A graceful `shutdown()` waits for that answer, or for the
   drain bound.

## What is new

Read the `0.23.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, including the new Experimental API.
