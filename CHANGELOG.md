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

- **`zig build check-test-compile`** compiles every registered test
  binary without running it, and CI runs it for `x86_64-windows` on a
  Linux runner (new cross-target matrix entry) — Windows test-suite
  compile rot is now caught on every push without a Windows machine.
- **Persistence / sturdy refs (RPC level 2)** (`rpc.peer.persistence`,
  `Peer.setPersistentExport`, `Peer.setRestorer`, `Peer.sendSave`,
  `Peer.sendRestore`): applications can mark an export persistent with a
  save handler that produces an app-defined sturdy-ref payload when the
  remote calls `Persistent.save()` (dispatched through the normal
  inbound-call path), call `save()` on imported capabilities and receive
  the sturdy-ref bytes in a `QuestionCallback`-style callback, and
  rehydrate refs on reconnect through a restorer hook served on the
  bootstrap capability (`connect -> bootstrap -> restore(ref) -> resume
  calling`). Sturdy refs are opaque `Data` bytes and restore uses a
  documented vat-level `Restorer` convention interface, since both are
  realm/vat-specific per the spec. New peer state is budgeted
  (`PeerLimits.max_persistent_exports`, `max_sturdy_ref_bytes`) with
  pressure/rejection events, and `PeerStats` gains `persistent_exports`,
  `saves_served`, and `restores_served`. Experimental.
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

- **Zig e2e interop gate is reliable on CI runners** (`tools/e2e_runner.zig`):
  port readiness now requires the accepted connection to survive a short
  probe window — a bare connect is a false positive against docker
  published ports, whose userland proxy accepts before the in-container
  server has bound (this made every `zig-client:*:python` case lose the
  startup race on CI while passing on faster local machines). The zig e2e
  client is pre-built once per phase so the first case no longer spends its
  20s case timeout on a cold `zig build` (the `zig-client:game_world:cpp`
  exit=124), and failing cases now always print their captured output so
  CI logs are diagnosable without re-running.
- **`check-api` is target-stable** (verified by running the checker against
  the same snapshot in a linux/amd64 container): the snapshot gate had
  failed on Linux CI since it landed because the surface rendered
  differently per platform. `io_backend.InitError` no longer unions
  `std.Io.Evented.init`'s target-specific error set (failures coalesce
  into the new `error.EventedBackendInitFailed`), `Backend.initEvented`
  drops its platform-typed options parameter (`EventedInitOptions`
  removed; nothing consumed it), `rpc.peer.state` thread-affinity helpers
  wrap `std.Thread.Id` in `OwnerThreadId` so signatures stop rendering the
  target's integer width, and `tools/api_snapshot.zig` collapses
  compiler-assigned `__struct_<n>` suffixes that shift across targets and
  unrelated edits.
- **Hardening gate passes again**: the `st.on_save.?`/`st.on_restore.?`
  unwraps introduced with persistence and the long-standing `wake_fds.?`
  unwrap in the tick loop are restructured away (save/restore hook context
  and handler now travel together as one optional; the wake-pipe drain
  uses an optional capture), so the gate needs no new allowlist entries.
- **`rpc_tick_idle_test` no longer races thread spawn**: the traffic
  feeder writes before it sleeps, so a delayed spawn on a loaded CI
  runner cannot leave the connection idle past the reap bound before the
  first byte arrives.
- **Hardening gate compiles on Windows**: argument parsing uses
  `std.process.Args.Iterator.initAllocator`, the cross-platform form
  (plain `init` is a compile error on Windows, where CI also runs the
  gate).
- **Test suite compiles on Windows**: fake-Io transport tests passed
  integer literals where `std.Io.net.Socket.Handle` is a pointer
  (`HANDLE`) on Windows; they now use a portable dummy handle. Verified
  by cross-compiling every registered test root for `x86_64-windows` in
  Debug and ReleaseSafe.
- **`bench-check` is self-contained**: the step now installs the
  benchmark binaries it spawns (`bench/baselines.json` points at
  `./zig-out/bin/...`), so it works from a fresh checkout — in CI every
  case failed with `FileNotFound` because nothing built them.
- **Nightly fuzzing fails only on findings**: zig master's experimental
  coverage-guided fuzzer can crash its own infrastructure (build-runner
  `tmp/` cleanup races the fuzzer log file; maker slice panics). The
  nightly job now classifies maker crashes and fuzzer-runtime panics as
  warnings; a fuzz target dying still fails the job.
- **e2e zig client/server compile for Windows**: cross-platform argument
  iteration, a comptime-stubbed wall clock (no libc dependency), and
  removal of the dead `--listen-fd` flag whose `parseInt(fd_t, ...)`
  cannot type-check where `fd_t` is a pointer.
- **Argument parsing is cross-platform in every tool and benchmark**
  (`docs_examples_smoke` — which `zig build check` compiles on the
  Windows CI job — plus `api_snapshot`, `bench_check`, `soak_rpc`,
  `e2e_runner`, and both benchmarks): `Args.Iterator.initAllocator`
  replaces the POSIX-only `init`, with iterator lifetimes hoisted where
  parsed slices escape. The e2e runner's raw-socket layer and the
  benches' monotonic clock remain deliberately POSIX-only.
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
