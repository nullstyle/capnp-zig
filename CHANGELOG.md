# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Breaking

- **RPC public exports** are now domain-shaped. Use `rpc.wire`,
  `rpc.caps`, `rpc.promises`, `rpc.events`, `rpc.transport`, `rpc.peer`,
  `rpc.integration`, `rpc.generated`, and `rpc.testing`. Removed top-level
  compatibility aliases include `rpc.protocol`, `rpc.framing`, `rpc.cap_table`,
  `rpc.promise_pipeline`, `rpc.connection`, `rpc.runtime`,
  `rpc.transport_binding`, `rpc.host_peer`, and `rpc._internal`.
- **RPC test steps** are now named by domain: `test-rpc-wire`,
  `test-rpc-caps`, `test-rpc-promises`, `test-rpc-transport`,
  `test-rpc-peer`, `test-rpc-integration`, and `test-rpc-quic`. The old
  `test-rpc-level*` steps were removed.
- **Zig 0.17-dev** is the active target for this branch. Zig 0.16 is no longer
  a compatibility gate.
- **Zig floor moved to current master** (`0.17.0-dev.813+2153f8143`,
  minimum `0.17.0-dev.813` in `build.zig.zon`): `build.zig` was migrated to
  the new `std.Build` configure/make split (`Run.addPassthruArgs()` replaces
  `b.args`), and `std.meta.Int` usage moved to the `@Int` builtin. Older
  0.17-dev snapshots no longer build this branch.
- **QUIC transport temporarily unavailable**: the pinned `quic_zig`
  dependency (and its `boringssl_zig` dependency) have not been migrated to
  current Zig master yet, so `-Dquic=true` builds fail until the dependency
  is updated and repinned. `quic_zig` is now declared lazy, so default
  builds neither fetch nor compile it; the QUIC CI job is disabled until
  then.

### Added

- **Call deadlines and cancellation** (`rpc.time`, `Peer.setClock`,
  `PeerTimeouts.default_call_timeout_ms`, `Peer.setQuestionDeadline`,
  `Peer.cancelQuestion`, `Peer.checkDeadlines`): outbound questions can now
  carry monotonic deadlines. Expiry delivers a local exception Return to the
  caller, sends a Finish (`releaseResultCaps`) to the remote, absorbs the
  remote's late Return silently per spec, and emits a `timeout` event. Time
  comes from an injectable clock (`rpc.time.Clock`); attaching a TCP
  connection injects an `std.Io`-backed clock automatically, and
  `rpc.time.TestClock` drives deterministic tests.
- **Transport tick and idle reaping**
  (`Connection.Options.tick_interval_ms` / `idle_timeout_ms`): the TCP run
  loop can poll with a bounded timeout, driving the peer's deadline sweep
  on each tick and reaping connections that see no traffic within the idle
  bound (POSIX only, mirroring wake-pipe support).
- **Graceful shutdown drain bound**
  (`PeerTimeouts.shutdown_drain_timeout_ms`, `WorkerPool.shutdownGraceful`):
  `Peer.shutdown` now stamps a drain deadline; if in-flight questions
  outlive it they are force-cancelled (with a `shutdown_drain` timeout
  event) and the shutdown completes. The worker pool gained a graceful
  variant that stops accepting, waits up to a bound for active connections
  to finish, then closes stragglers.
- `rpc.events` gained a `timeout` event (`call_deadline`, `idle_connection`,
  `shutdown_drain`).
- `rpc.events`, a redacted transport-general observer API shared by TCP, QUIC,
  host-peer, and peer dispatch.
- QUIC multi-session server fanout through `rpc.transport.quic.Server` and
  `ServerSession`, while `Connection.initServer()` remains the single-session
  helper.
- QUIC native-mode stress and malformed-frame coverage.
- `io_backend.Backend.init(.evented, ...)` support where Zig exposes
  `std.Io.Evented`.

### Changed

- **Write-queue backpressure semantics are now class-aware.** Caller-initiated
  sends (calls, bootstrap) surface `error.WriteQueueFull` /
  `error.WriteQueueBytesExceeded` to the caller, roll back the question, and
  leave the connection healthy. Protocol-mandated frames (Return, Finish,
  Release, Resolve, Disembargo, Abort, Unimplemented) treat enqueue overflow
  as unrecoverable divergence: a peer-level backpressure event is emitted and
  the transport is closed instead of silently dropping protocol state.
- RPC peer internals are split into semantic modules under `peer/return`,
  `peer/forward`, `peer/provide`, and `peer/third_party`, making the codebase
  easier to navigate without changing the standard `rpc.capnp` wire protocol.
- CI now installs the pinned Zig master snapshot via `mlugg/setup-zig` (Zig
  had been absent from CI since it was removed from `mise.toml`, so every job
  failed with `zig: command not found`).

### Fixed

- **Graceful shutdown now completes when the last in-flight question is
  answered by a normal Return.** The drain-completion check in the return
  orchestration was comptime-disabled because it probed a non-public decl
  (`@hasDecl` cannot see private fns across files), so `Peer.shutdown`'s
  callback only ever fired for empty peers or force-cancelled drains.

### Existing Baseline

- **Wire format** (`src/serialization/message.zig`, `src/serialization/message/*`): Full Cap'n Proto binary
  format support including segment management, pointer encoding/decoding
  (struct, list, far, capability), text/data serialization, and packed encoding.
  Key types: `Message`, `MessageBuilder`, `StructReader`, `StructBuilder`, and
  typed list readers/builders for all primitive types.

- **Schema types** (`src/serialization/schema.zig`): In-memory representation of Cap'n Proto
  schema graphs mirroring `schema.capnp` -- `Node`, `Field`, `Type`, `Value`,
  and supporting types.

- **Schema parsing** (`src/serialization/request_reader.zig`): Parser for
  `CodeGeneratorRequest` messages received from the Cap'n Proto compiler plugin
  protocol over stdin.

- **Schema validation** (`src/serialization/schema_validation.zig`): Validation and
  canonicalization of schema graphs with configurable traversal limits and
  nesting depth.

- **Code generation** (`src/capnpc-zig/`): Compiler plugin that generates
  idiomatic Zig Reader and Builder types from `.capnp` schemas. Supports
  structs, enums, constants, unions, groups, nested types, default values, and
  schema manifests with JSON serde exports.

- **Reader convenience** (`src/serialization/reader.zig`): High-level `Reader` type for
  segment-framed message reading, including packed-format support and
  stream-based message reading.

- **RPC runtime** (`src/rpc/`, experimental): Cap'n Proto RPC implementation
  over TCP using synchronous POSIX I/O with concurrent read/write transport.
  Includes the domain-shaped runtime surface under `rpc.wire`, `rpc.caps`,
  `rpc.promises`, `rpc.transport`, `rpc.peer`, and `rpc.integration`.

- **Interop testing**: Dockerized end-to-end tests against the Go Cap'n Proto
  reference implementation (`vendor/ext/go-capnp/`), serving as the RPC hard
  gate.

- **Benchmarks**: Ping-pong RPC benchmark with configurable iterations and
  payload size. Packed and unpacked serialization benchmarks.

- **Build system**: Zig 0.17-dev build with `build.zig` providing targets for
  `build`, `test`, `check`, `bench-*`, and `example-rpc`. Justfile aliases for
  common tasks.

- **WASM host ABI** (`src/wasm/`): Language-neutral ABI specification and Zig
  build target for wasm-based Cap'n Proto RPC hosts.

- **Quality hardening**: Comprehensive quality passes covering error handling,
  bounds checking, resource cleanup, and documentation across all layers.
