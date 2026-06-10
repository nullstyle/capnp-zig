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

- **Project policy files**: `LICENSE` (MIT — the README claimed it; now the
  text exists), `SECURITY.md` (private reporting path plus an explicit
  in-scope/out-of-scope list for a library that parses untrusted bytes),
  and `CONTRIBUTING.md` (toolchain, gates, conventions).
- **Coverage-guided fuzz targets** (`tests/fuzz/fuzz_targets.zig`,
  `zig build test-fuzz`): `std.testing.fuzz`-based targets for validated
  `Message.init`, packed decode, stream framer chunking, and RPC peer frame
  dispatch. CI runs them deterministically; the nightly workflow fuzzes for
  10 minutes with crash-vs-timeout discrimination.
- **Public API snapshot gate** (`tools/api_snapshot.zig`,
  `docs/api-snapshot.txt`): the library's full pub-decl surface (~1,670
  declarations with signatures) is captured by comptime reflection;
  `zig build check-api` fails CI on drift and `zig build api-snapshot`
  regenerates the file, making API changes an explicit reviewable act.
- **Cross-target compile gate** (`zig build check-compile -Dtarget=...`):
  a run-free variant of `check`, exercised in CI for `aarch64-linux-gnu`,
  `x86-linux-gnu` (32-bit), and `powerpc64-linux-gnu` (big-endian).
- **Release-mode thread-affinity checks** (`Peer.enableRuntimeThreadChecks`,
  `Connection.runtime_thread_checks`): the single-threaded-per-peer contract
  is always enforced in Debug; release builds can now opt in to the same
  panic-on-violation checking at the cost of one thread-id read per entry
  point.
- **Pressure events** (`rpc.events.pressure`): bounded peer/transport
  resources (outbound and inbound question tables, queued promise calls and
  bytes, resolved imports, write-queue items and bytes) emit a single event
  when occupancy crosses 80% of budget from below — an early-warning signal
  ahead of the existing hard-rejection events.
- **Metrics surfaces**: `Peer.stats()` returns a point-in-time gauge
  snapshot (questions in flight, cancelled questions, inbound questions,
  exports, queued calls/bytes, resolved imports/answers);
  `Transport.queueStats()` / `Connection.writeQueueStats()` expose write
  queue occupancy; `rpc.events.call_latency` reports per-call wall time on
  Return dispatch whenever the peer has a time source.
- **Soak harness** (`zig build soak`, `tools/soak_rpc.zig`): sustained
  concurrent bootstrap+call traffic over loopback TCP against a WorkerPool
  server, with chaos sessions (abrupt mid-flight disconnects) and deadline
  sessions (1ms deadlines racing a deliberately slow server method, so
  cancellation and late-Return absorption run against live traffic). A
  nightly workflow (`.github/workflows/nightly.yml`) runs it for 2 minutes
  in Debug and 1 minute in ReleaseSafe alongside extended gates.
- **Configurable TCP frame cap**
  (`Connection.Options.max_buffered_frame_bytes`): per-connection bound on
  inbound frame assembly, mirroring the QUIC transport's per-message bound.
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

- **QUIC transport re-enabled on Zig master**: `quic_zig` repinned to
  upstream `0e4d540` (which itself targets `0.17.0-dev.813`), the
  transport adapter migrated to upstream's new tagged-union
  `conn.path.Address` API (a one-to-one variant map mirroring
  `std.Io.net.IpAddress`), and the `quic-transport` CI job restored.
- **`PeerLimits.max_resolved_imports` default raised 4096 → 10,000** to
  match the capability table's hard cap, so promise-heavy workloads no
  longer hit a lower hidden wall first.
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

- **Cross builds of the WASM host no longer inherit the host `-Dtarget`**:
  the wasm example/ABI modules now import a wasm-targeted core module, so
  `zig build check-compile -Dtarget=<foreign>` works.
- **The tick-loop `poll(2)` wrapper is now portable**: poll results are
  classified via `errno` instead of assuming Darwin's signed return type,
  fixing Linux-target compiles of the connection run loop.
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
