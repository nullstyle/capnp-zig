# Stability Matrix

This document describes the stability level of each module in capnpc-zig and
provides guidance for downstream consumers.

## Platform Support

macOS, Linux, and Windows are all first-class targets and development
operating systems; per-push CI gates every tier below. Status of the
work bringing Windows to full parity is tracked in
[windows-first-class-plan.md](windows-first-class-plan.md) — this table is
updated as phases land.

| Layer | Linux | macOS | Windows |
|---|---|---|---|
| Serialization / wire format / packing | full | full | full |
| Codegen + `capnpc-zig` plugin (incl. CLI options and `capnp`-driven suites) | full | full | full (see note) |
| RPC protocol engine (`Peer`, persistence, promises) | full | full | full |
| TCP transport: connect/accept/read/write | full | full | full |
| TCP transport: ticks, idle reaping, wake | full | full | full (reader-thread bridge; see plan doc) |
| TCP transport: `TCP_NODELAY` | full | full | blocked upstream (std's AFD sockets accept no winsock setsockopt; AFD option helper not exposed) |
| Soak harness | full | full | full (nightly lane; success requires positive traffic/chaos counters) |
| Self-interop e2e (zig↔zig loopback, `zig build e2e-self`) | full | full | full |
| Cross-implementation e2e (docker reference impls) | full | full | local only (Docker Desktop/WSL2); hosted runners cannot run Linux containers |
| Deterministic fuzz smoke | full | full | full |
| Structural fuzz: L3/L4 peer frames, QUIC framers, persistence restore | full | full | full |
| ThreadSanitizer lane (`test-tsan`, threaded transport suites) | **runs clean**: 11/11 steps, 26/26 tests, no races reported (run 31634824440) | not available (libtsan SIGSEGVs at startup on this Zig pin) | not available |
| Coverage-guided fuzzing (`--fuzz`) | full | full | blocked upstream (zig fuzzer is ELF/Mach-O only) |
| Evented `std.Io` backend | compile-checked only — no sockets (see below) | compile-checked only — no sockets (see below) | blocked upstream (`EventedBackendUnsupported`) |
| QUIC transport | experimental (`-Dquic=true`; four-root evidence runs in Debug + ReleaseSafe, and the full repository is also tested against the QUIC-enabled root) | experimental (four-root Debug + ReleaseSafe evidence; locally proven 64/64 on macOS) | experimental (native Debug + ReleaseSafe no-skip evidence **executes and passes** on `windows-latest`; the full QUIC-root suite is not run there) |

The targeted QUIC evidence gate is intentionally different from testing the
entire repository through the QUIC-enabled library root. The three-OS native
CI matrix runs exactly four roots in Debug and ReleaseSafe. A native Zig
scanner rejects `SkipZigTest`, enforces per-root floors 26 + 1 + 17 + 8 = 52,
and makes the four runnable artifacts direct build dependencies; it does not
parse output or rely on a CI-only shell. Linux alone also runs the full
build/check/test/API/docs surface against the QUIC root. At this sprint's local
handoff, macOS runs all four evidence roots in both modes (per-root minimum
test counts enforced by `tools/quic_test_evidence.zig`). Windows full-tree test
cross-compilation passes 113/113, and native Windows runtime acceptance is no
longer pending: the `QUIC targeted transport (windows-latest)` job runs both
evidence roots natively, in Debug and ReleaseSafe with skips rejected, and
passes. Scope, because the job's own step list is the honest limit — only the
evidence roots run on Windows; "Build QUIC-enabled targets", "Check
QUIC-enabled build graph", "Run the full suite against the QUIC library root",
the strict QUIC API snapshot, and the QUIC doc-snippet fixtures are all
skipped there and remain Linux-only.

Getting that leg green required fixing a real defect rather than adjusting an
expectation: Windows AFD reports an oversized datagram as a failed receive
(`STATUS_BUFFER_OVERFLOW` -> `error.MessageOversize`, payload and sender
address discarded, `flags.trunc` never set), where POSIX reports it as a
successful receive with `MSG_TRUNC`. That error propagated out of the
connection step, so one spoofed oversized UDP datagram could kill a Windows
QUIC connection. The bridge now normalizes both spellings into one
`truncated` outcome.

That pending claim also rests on a layer below us, so it is worth stating
precisely what the dependency does and does not prove. In quic-zig v0.10.1
(the pinned tag), `windows-latest` is a tier-1 **blocking** CI leg —
`advisory: false` in the test matrix — running the full `zig build test`, so
the protocol engine, wire/frame codecs, conformance suite, and in-memory TLS
handshakes genuinely execute on Windows rather than merely cross-compiling.
Exactly three tests are excluded there, and all three are the real-socket loop
smokes (two entering `runUdpServer`, one entering `runUdpClient`). They carry
an unconditional `if (builtin.os.tag == .windows) return error.SkipZigTest`
guard, so they have never run on Windows and have never reported a failure:
the UDP socket loop is **untested on Windows, not known-broken**. Verified
against the pinned tag, not taken on report.

The practical consequence for our own Windows QUIC acceptance is that the
socket layer is unproven on *both* sides of the boundary. Treat a Windows
runtime run as genuine discovery work that may surface real defects, not as a
formality expected to confirm a working path.

**And it did — and what it found was ours, not upstream's.** The hosted `QUIC
targeted transport (windows-latest)` leg reported **60/61**, failing exactly
one case: `QUIC UDP receive bridge reports truncation without processing
partial bytes`. That test delivers a 9-byte datagram into a 2-byte buffer and
expects truncation to be reported; on Windows the receive *failed* instead,
and `udp_receive_bridge.zig` propagated the error to its caller. So an
oversized inbound datagram tore down the endpoint on Windows, where every
other platform dropped it and kept serving.

The platform difference is upstream: `std.Io`'s receive error set has no
truncation variant, so Windows (where AFD returns `STATUS_BUFFER_OVERFLOW`)
surfaces `error.MessageOversize` and leaves `flags.trunc` hardcoded false,
while POSIX signals `MSG_TRUNC` and returns the prefix that fit. Deciding to
tear the endpoint down over it was ours. The bridge now normalizes both into a
single `truncated` outcome — the mapping is Windows-gated and matched
exactly, not pattern-matched against whatever generic error appears, because
`BUFFER_OVERFLOW` is the only source of `MessageOversize` on that path.

The same question then had to be answered for the fanout paths, which had
kept the old semantics on *every* platform: `Server.receiveOneFor` and
`Listener.receiveOne` both returned `error.DatagramTooLarge`, and
`Server.run` closes the server on a failed step. One spoofed oversized UDP
datagram from any host was therefore a kill switch for every session on the
port. All receive paths now share one policy in
`src/rpc/transport/quic/datagram_drop.zig`: drop the datagram, count it,
`warn`, and emit a redacted `resource_rejection` observer event
(`Resource.udp_datagram_bytes`). `Server.droppedDatagramCount()` and
`StepResult.dropped_datagram` expose it; `receiveOne` reports it as `null`,
the same as an idle poll, because there is nothing the caller can usefully do
differently. Two tests cover it — one drives a live fanout session through a
spoofed oversized datagram and back, one covers the bare `Listener` — and both
were ablation-checked against a restored `return error.DatagramTooLarge`.

The 64 KiB default `udp_rx_buffer_size` is above the 65507-byte IPv4 UDP
payload ceiling, so the exposure only ever reached endpoints tuned closer to
their path MTU. Note the flip side of dropping: a *legitimate* peer whose
datagram exceeds `udp_rx_buffer_size` is now silently unserved. That is what
the counter and the observer event are for — an rx buffer sized under the
peers' path MTU is a misconfiguration, and this is how it becomes visible.

Windows QUIC still stays **experimental**: the hosted leg remains the gate for
the error arm, since the mapping is comptime-gated and unreachable off Windows.

Note on **Windows codegen**: the upstream prebuilt Windows tools still contain
the executables but not the standard schema tree. The repository now routes
every `capnp`-driven serialization/codegen invocation through one helper that
injects `-Ivendor/ext/capnproto/c++/src`, so absolute imports such as
`/capnp/stream.capnp` use the same vendored standard schemas on Linux, macOS,
and Windows. The Windows job downloads the pinned archive with a checksum,
places `capnp.exe` on `PATH`, fails up front if the tool is unavailable, and
runs the ordinary native suite. This is full native codegen coverage; local
developers without `capnp` retain the documented optional-test skips.

Note on the **Evented `std.Io` backend**: "compile-checked only" is the whole
claim, and the limit is upstream, not here. At the pinned toolchain
(`0.17.0-dev.1683`) `std.Io.Evented` resolves to `std.Io.Dispatch` on macOS and
`std.Io.Uring` on Linux, and neither carries a working socket vtable — Dispatch
implements `netClose` alone, Uring only `netBindIp` / `netClose` /
`netShutdown`, and everything else (`netListenIp`, `netAccept`, `netConnectIp`,
…) is an `...Unavailable` stub. Every RPC path is socket-based, so the selector
compiles and then fails at runtime on a real connection. `zig build
-Dio-backend=evented check` proves the selector still *builds*; it does not
prove the backend works, and no lane executes it because none can.

## Stability Levels

| Level | Meaning |
|---|---|
| **Stable** | API is settled. Breaking changes follow semver (major bump). Bug fixes and additive changes only within a minor version. |
| **Experimental** | Functional but the API may change across any release. Use at your own risk; pin to an exact version. |
| **Internal** | Implementation detail. Not part of the supported consumer API. May change or be removed without notice, even when exposed through a test-only facade. |

## Module Status

### Stable

| Module | Path | Notes |
|---|---|---|
| Wire Format | `src/serialization/message.zig`, `src/serialization/message/*` | Core serialization: segments, pointers, structs, lists, text, data, packed encoding. Thoroughly tested and interop-validated. |
| Schema Types | `src/serialization/schema.zig` | In-memory schema representation (Node, Field, Type, Value). The frozen `Type` union remains unchanged; additive `TypeMetadata`, `TypeExpression`, parameters, and brands preserve AnyPointer sub-kinds and generic applications beside it. |
| Schema Parsing | `src/serialization/request_reader.zig` | Parses `CodeGeneratorRequest` from Cap'n Proto wire format, including bounded recursive brand bindings, node/method parameters, AnyPointer sub-kinds, and interface brands. |
| Schema Validation | `src/serialization/schema_validation.zig` | Validates and canonicalizes schema graphs. Additive `*WithBrand` entry points enforce a concrete root brand; legacy entry points use an empty root brand while still honoring concrete nested metadata. |
| Code Generation | `src/capnpc-zig/generator.zig`, `src/capnpc-zig/struct_gen.zig`, `src/capnpc-zig/types.zig` | Generates idiomatic Zig Reader/Builder types, including additive `nestedLists()`, constrained-AnyPointer `pointerKinds()`, and finite concrete `brands()` views while preserving every erased/raw field accessor. A shared bounded resolver handles arbitrary nested lists (including generic-struct terminals), cross-file applications, lexical inheritance, exact arity, and parameter indexes; `CodegenBudget.max_brand_specializations` bounds emitted applications. Pointer defaults recursively materialize before Builder mutation. Generic RPC clients remain erased. |
| Reader Convenience | `src/serialization/reader.zig` | Segment-aware message reader with packed support. |
| Canonicalization (schema-free) | `src/serialization/canonical.zig` | The spec's canonical form: `canonicalize` / `canonicalizeFlat` / `isCanonical` plus the builder-direct `canonicalizeFlatFromBuilder` / `canonicalizeFromBuilder`. Line-cited port of the C++ reference; byte behavior pinned by acceptance-suite ports and `capnp convert binary:canonical` differential tests. Promoted from Experimental deliberately: downstream consensus consumers use `canonicalizeFlat` bytes as signing preimages, so byte drift is a network fork, not an API break. |

#### Stable RPC — the two-party core (new in 0.3.0)

The two-party Cap'n Proto RPC Level-1 core is promoted to Stable in 0.3.0 on a
**frozen public surface**: the Stable RPC entry points below are pinned by the
`docs/api-snapshot.txt` snapshot and gated in CI (`zig build check-api`) — a
diff is a reviewed breaking change, not an accident. The complementary
Experimental surface evolves in `docs/api-snapshot-experimental.txt`, which is
**no longer ungated**: CI runs `check-api-experimental` (and, on Linux with
QUIC enabled, `check-api-experimental-quic`), so the committed experimental
snapshots must match the tree. Drift there is not a breaking change — the fix
is to regenerate and commit — but it can no longer go unnoticed. This became
possible once the stored thread ids were widened to `u64`: `std.Thread.Id` is
u32 on Linux/Windows and u64 on macOS, and `@typeName` renders the resolved
integer, so the snapshots used to differ by the platform that generated them.

| Module | Path | Frozen surface |
|---|---|---|
| RPC Protocol | `src/rpc/wire/protocol.zig` | Wire readers/builders for RPC messages (Call, Return, Resolve, Disembargo, …). |
| RPC Framing | `src/rpc/wire/framing.zig` | Segment-framed message reassembly from byte streams. |
| RPC Capability Table | `src/rpc/caps/table.zig` | Export/import tracking for capabilities. |
| RPC Connection (narrowed) | `src/rpc/transport/tcp/connection.zig` | The narrowed public `Connection` surface: `init` (post-F3 canonical shape, opaque socket handle), `Options.default()`, `enableWake`, lifecycle. Demoted `attachTransport*`/`initDetached*` variants remain Experimental. |
| TCP `ClientSession` | `src/rpc/transport/tcp/*` | `ClientSession.connect` / `connectHost` — the one-call client lifecycle (`run`/`close`/`requestStop`/`deinit`). |
| TCP `ServerSession.accept` | `src/rpc/transport/tcp/*` | The `ServerSession.accept` consumer entry point + its `run`/`close`/`requestStop`/`deinit` lifecycle. The `ServerSession` struct/type *beyond* `.accept` is Experimental. |
| RPC Peer — two-party entry points | `src/rpc/peer/mod.zig` | The canonical two-party `Peer` surface: `init`, `attachConnection`, `sendBootstrap`, the `sendCall*` family, `addExport`, `addPromiseExport`, `setBootstrap`, `releaseImport`, basic promise resolution, and lifecycle — plus the `CallError` error set (`rpc.peer.CallError`), the public callback typedefs (which keep `anyerror`), and `PeerLimits`. Demoted ctor/attach variants (F3), L3 origination, and `resolvePromiseExportToImport` stay Experimental (see below). |

**Honest disclosures for the Stable RPC core:**

- **Soaked, with committed regression evidence.** A `bench-rpc` regression gate
  and the RPC soak harness (latency percentiles + a flat memory-growth curve
  asserted at ≥100 concurrent peers) run against the two-party core. This is
  real soak evidence, not a claim. Scope of the bench gate, precisely: the
  **pipelined throughput** case (calls/sec) is enforced in CI. The **sequential
  round-trip latency** percentiles (p50/p99) and its calls/sec are measured and
  printed but marked *advisory* — a serialized round-trip on a shared CI runner
  is dominated by hypervisor/neighbor scheduling, and was observed swinging p99
  105µs → 210µs on identical code, so enforcing it produced random red builds.
  Run `zig build -Doptimize=ReleaseFast bench-check -- --enforce-advisory` on a
  quiet machine to gate those too.

  Measured variance across 5 successive green CI runs on `main`: the
  serialization/ping-pong wall-clock cases span 1.07×–1.25×, and every
  allocation counter is **exactly** invariant (1.00×). The pipelined throughput
  case spans **1.75×** — the widest of the fourteen, despite earlier wording
  here calling it stable within ~3%. Bands are now sized per case from that
  data: 40% above the worst observed run for wall-clock, 10% for allocation
  counts, 25% for allocation bytes (a new gated metric — a change that keeps the
  call count identical while growing each buffer was previously invisible).
  Treat the allocation metrics as the deterministic signal and the wall-clock
  cases as coarse.

  Answer-lifecycle regressions additionally cover
  synchronous reentrant `Finish` during results and Bootstrap `Return`
  delivery (queued promised-answer replay before immediate cleanup), the
  `releaseResultCaps` flag on a late `Return` after a cancelling `Finish`, and
  rejection of question-id reuse while an early-Finish tombstone is
  undischarged.
- **Cross-implementation e2e is local-Docker only.** The Zig↔Zig self-interop
  e2e (`zig build e2e-self`) runs in hosted CI on every push, but the
  cross-implementation matrix against the C++, Go, Python, and Rust reference
  peers runs only on a local Docker host (Docker Desktop / WSL2) — hosted CI
  runners cannot run the Linux-container reference matrix. Conformance against
  the reference impls is verified locally before each release, not on every push.

### Experimental

Everything below is outside the frozen contract and may break at any 0.x minor
bump. The L3 three-party arc in particular is **lightly soaked and
Experimental**: main has a Zig↔C++ TCP success/failure matrix plus Go
source-backed blocker recon, but not a full reference matrix or production
interop contract.

| Module | Path | Notes |
|---|---|---|
| RPC Runtime | `src/rpc/transport/tcp/runtime.zig` | Listener and socket helpers. API will evolve. |
| RPC Connection — demoted variants | `src/rpc/transport/tcp/connection.zig` | The `attachTransport*` / `initDetached*` (F3-demoted) constructors and any raw-socket-handle entry points. The narrowed `Connection` (`init`/`Options.default`/`enableWake`/lifecycle) is Stable — see above. |
| RPC Peer — beyond the two-party core | `src/rpc/peer/mod.zig` | The Experimental parts of `Peer`: retained-answer `CallOptions`, `sendCall*WithOptions`, explicit retained Finish and their generated `*WithOptions` wrappers; the F3-demoted ctor/attach variants; `test_hooks`; and everything under the L3 / reflected-cap surfaces below. Existing automatic `sendCall*` and generated `callXxx` behavior remains Stable — see above. |
| RPC Peer — retained outbound answers | `src/rpc/peer/mod.zig`, `src/rpc/peer/retained_questions.zig` | `.result_lifetime = .retained` suppresses automatic Finish after Return until `finishRetainedQuestion` succeeds or ownership transfers. Admission defaults to 1024 per peer; the new `PeerLimits.max_retained_questions` field is Experimental even though the pre-existing fields in that configuration type stay frozen. Caller-owned and transferred gauges, pressure/rejection events, retry-safe Finish state, cancellation/close cleanup, and synchronous-Return registration are regression covered. Generated Client/PipelinedClient and `callXxxPipelined` methods have additive `WithOptions` variants; StreamClient fire-and-forget methods remain automatic. Not frozen. |
| RPC Peer — reflected-cap resolve | `src/rpc/peer/mod.zig` (`resolvePromiseExportToImport`) | Resolve a promise export to a *caller-hosted* cap (drives the `senderLoopback`/`receiverLoopback` Disembargo). New in 0.3.0; verified in loopback + a partial cross-impl matrix (see supported-surface Known limitations). Not frozen. |
| RPC Peer — L3 three-party origination | `src/rpc/peer/mod.zig`, `src/rpc/peer/provide/*`, `src/rpc/peer/third_party/*` | The full Level-3 arc: `sendProvide`, `sendAccept`, `resolvePromiseExportToThirdParty`, `sendThirdPartyAnswer`, `registerPendingThirdPartyAwait`, `setHandoffPickupHandler`, `ProvideHandle`, `thirdPartyHosted` emission, retained promised-answer target transfer, and opt-in automatic redirected results through `ThirdPartyResultPolicy.vat_network`. The automatic mode resolves contact through the attached `VatNetwork`, gives the introduced peer a synthetic answer with capability remap/pipelining/Finish, and leaves `.reject` as the default plus `.application` as the manual compatibility mode. The 22-test redirected-return binary preserves manual and retained-answer regressions and covers 19 automatic cases: scalar delivery, capability pins/proxy use and Release, pipeline-before-result, early Finish plus pipelined-child/cap drain, missing-network refusal, one handler dispatch, pre-/post-delivery send boundaries, allocation-index OOM cleanup, distinct network/source/target allocators, reentrant source/target deinit, source/target close without deinit, and route drain. Other focused allocator, teardown, embargo, auto-pickup callback-failure, transferred-source Finish maintenance, and cross-peer proxy regressions cover Zig↔Zig; `just e2e-l3-cpp` covers the existing Zig↔C++ TCP success/failure matrix but does not add redirected-result or retained-answer scenarios. `just e2e-l3-go` source-checks the current Go blocker: Network3PH hook names exist, but required runtime 3PH paths remain TODO. Not frozen. |
| RPC Peer — L4 Join runtime pilot | `src/rpc/peer/mod.zig`, `src/rpc/peer/provide/*`, `src/rpc/vat/join.zig` | `sendJoinExperimental` can originate raw Join parts, receive-side Join state accumulation/target equality/Finish cleanup/send-failure fallback/OOM rollback are regression covered, and an Experimental `JoinNetwork` path now returns Zig JoinResult payloads that the joiner resolves into a direct `Accept` and callable cap. `JoinCoordinator` is the first Experimental Zig-shape helper above the raw sender: it emits compact key parts, rejects duplicate local part numbers before sending, collects matching JoinResults, sends direct Accept, retains/releases the accepted cap, Finishes each JoinResult question on the peer where that part originated with per-question retry state, treats malformed/exception JoinResults and mismatched successful JoinResults as terminal failed results with retryable Finish state, releases retained `Joined` leases when a later terminal JoinResult failure makes the aggregate impossible, Finishes JoinResults after terminal direct Accept Returns, suppresses internal direct-Accept auto-Finish during synchronous pickup, sends that Finish after the callback unwinds, keeps the Accept answer retryable until later cleanup drains it, neutralizes accepted-cap/Accept-answer peer links if the direct peer deinits first, and can cancel after JoinResults, after a direct Accept question is pending, or during deinit before freeing its callback context. Transparent cross-peer proxy exports can relay Join requests to their source peer, hold downstream JoinResult lifetime until upstream Finish, and keep relay/back-link state when forwarding that downstream Finish fails so a retry can drain it. `JoinNetwork` returned buffers are caller-allocator owned; `AddressedJoinNetwork` adds a registry/connector proof with opaque app addresses, already-live direct peers, and an optional app connector for unknown addressed provisions; `just e2e-l4-zig` runs the registry path over real Zig↔Zig TCP. Regressions cover the legacy direct-cap pilot plus the JoinResult→Accept path, coordinator accepted-cap release, duplicate-send rejection, malformed/exception JoinResult terminal cleanup including mixed retained-result cleanup, mismatched successful JoinResult cleanup, post-JoinResult and post-Accept-send cancel cleanup, drop-time pending Join/Accept cleanup, partial-Finish retry without replaying successful Finishes, terminal direct-Accept JoinResult cleanup including malformed Accept Returns, synchronous direct-Accept Finish OOM retry plus later release-time/transfer drain including `releaseAccepted()` partial failure retry and `takeAccepted()` transfer while Finish remains retryable, direct Accept peer teardown neutralization, sendPart OOM rollback, mismatch without retained caps, partial-Join cancel cleanup, callback failure after retention, JoinResult send-failure rollback including fallback exception send failure before delivery, distinct Join-host/Accept-host allocator ownership for promised targets, addressed registry unknown/stale/duplicate handling, connector malformed-token/no-dial, network-teardown-before-release, and OOM-before-dial handling, proxy relay success through the real coordinator, owner teardown including downstream Finish send failure, source teardown before/after downstream Return, source unavailable, unsupported source-target rejection, downstream Join send failure, downstream results/exception Return relay failure, unexpected downstream Return cleanup, downstream Finish retry, target mismatch through relay, and OOM rollback. The C++ L3 lane still confirms no callable generic C++ L4 Join hook is available, and the Go source probe confirms Join wire/twoparty shapes but no runtime dispatch, so there is no Stable `sendJoin`, no production Join addressing policy or bundled dialer, no multi-hop relay beyond transparent proxy relay, and no cross-implementation L4 runtime claim. See `rpc-l4-join-readiness.md`. Not frozen. |
| RPC Vat Join Network | `src/rpc/vat/join.zig` | Experimental L4 JoinResult addressing seam plus `LoopbackJoinNetwork` for Zig runtime tests and `AddressedJoinNetwork` for the non-loopback registry/TCP pilot. `JoinNetwork.hostJoinResult()` and `JoinNetwork.connectJoined()` take the allocator for returned caller-owned provision/result buffers. `LoopbackJoinNetwork.registerDirectPeerWithAcceptHost` supports tests where Join reaches a host through one peer but the final Accept arrives on another direct peer; `AddressedJoinNetwork` requires an opaque application address, can resolve already-live registry entries, and can call an app-supplied connector for unknown addressed provisions. The compact Zig JoinKeyPart/JoinResult/provision payload conventions are not frozen and are not a production VatNetwork policy. |
| RPC Vat provisions + Vat facade (L3 hosting) | `src/rpc/vat/provisions.zig`, `src/rpc/vat/host.zig`, `src/rpc/peer/mod.zig` | Vat-wide `ProvisionIndex` + refcounted `Provision` objects let a multi-connection vat HOST three-party handoffs (Provide and Accept on different peers): cross-peer serving via pinned proxy exports, live `receiverHosted` targets via deferred-Release import pins, per-provision embargo slots, Accept-before-Provide parking/adoption, owner-side promised-target re-resolution, per-peer + vat-wide park budgets, transport-close holder cleanup, deadline-cache expiry, gauges/events, and the secure-default `rpc.peer.Vat` facade (30s TTL; explicit raw/null opt-out). The focused seven-binary `zig build test-rpc-l3` gate plus resource/OOM/ReleaseSafe/ReleaseFast lanes cover fairness, lifecycle, reentrancy, expiry and rollback. Single-threaded vats only. Cross-impl hosting is proven against vendored C++ by `just e2e-l3-vatc` across nine scenarios, including both receiverHosted shapes, park-adopt, expiry, per-peer fairness and recovery; no other implementation can drive the client roles. Not frozen. |
| RPC Vat Network | `src/rpc/vat/network.zig` | `VatNetwork` addressing seam + `LoopbackVatNetwork` for L3 origination. Experimental; the production addressing policy is application-defined. Duplicate-token, unknown-token, allocator-failure paths, and Zig↔C++ TCP token success/failure rendezvous cases are covered. |
| RPC `ServerSession` (as a type) | `src/rpc/transport/tcp/*` | The `ServerSession` struct/API *beyond* its `.accept` consumer entry point. `.accept` + its lifecycle are Stable (see above); the type itself is not frozen. |
| RPC Transport | `src/rpc/transport/tcp/stream_transport.zig` | Concurrent read/write I/O. |
| RPC Events | `src/rpc/events.zig` | Redacted transport-general observer events, plus the typed disconnect-cause enum (`DisconnectCause`, the QUIC death certificate). Event names may grow while payloads stay redacted. |
| Switchable Io Backend | `src/io_backend.zig` | Backend selection (`process_init`/`threaded`/`evented`). Selector shape may change. |
| RPC QUIC Transport | `src/rpc/transport/quic` | Optional QUIC baseline/native transport, gated by `-Dquic=true`. Windows uses a single-in-flight cancellable UDP receive bridge; fanout sessions are heap-stable before `Peer` attachment. Native Windows acceptance runs both evidence roots in CI (Debug + ReleaseSafe, skips rejected). |
| RPC Host Peer | `src/rpc/integration/host_peer.zig` | Host-neutral detached frame-pump for wasm environments. |
| RPC Payload Remap | `src/rpc/caps/payload_remap.zig` | Capability descriptor remapping for outbound messages. |
| RPC Persistence | `src/rpc/peer/persistence.zig` | Sturdy-ref save/restore (level 2): persistent-export hooks, restorer hook, and `Peer.sendSave`/`sendRestore`. Realm conventions and consumer flow are documented in `rpc_runtime_design.md` and `rpc-persistence.md`; reconnect, malformed-frame, send-failure, callback-failure, hook lifecycle, and OOM rollback paths are regression covered. Not frozen. |
| Forwarded / 3-party internals | `src/rpc/peer/forward/*`, `src/rpc/peer/third_party/*` | Proxy/intermediary forwarding and third-party transfer internals. Not part of the frozen surface. |

The L4 row includes lease hardening but remains Experimental. Raw peers keep
`PeerTimeouts.join_timeout_ms = null`; TCP connect/serve and `WorkerPool`
configure a secure 30-second default unless the caller explicitly supplies
null. `PeerLimits` adds 64 parts per Join and 4096 aggregate lifecycle records,
and `PeerStats`/events expose only redacted record, part, provision-byte, and
answer-id data. One origin-owned `HostedJoin` holds the provision, captured
network, deadline, result count, and Accept-host backlink. Attachment is
fallible while dependent state exists. These safeguards do not add a
production dialer, address/authentication policy, stable wire convention, or
cross-implementation L4 claim.

### Internal

These modules are implementation details and should not be imported directly by
consumers. The project test suite uses the deliberately unstable `rpc.testing`
facade for narrow white-box coverage.

| Module | Path | Notes |
|---|---|---|
| RPC testing facade | `src/rpc/testing.zig` | Test-only access to the current white-box RPC helpers. |
| Peer dispatch | `src/rpc/peer/dispatch.zig` | Inbound message dispatch logic. |
| Peer cleanup | `src/rpc/peer/peer_cleanup.zig` | Resource cleanup on peer teardown. |
| Peer state | `src/rpc/peer/state.zig` | Peer limits, small state record types, and debug thread-affinity helpers. |
| Peer errors | `src/rpc/peer/errors.zig` | Named error groups for future peer API narrowing. |
| Peer transport facade | `src/rpc/peer/transport.zig` | Peer-facing transport binding aliases plus callback/state helper facades. |
| Peer dispatch facade | `src/rpc/peer/dispatch.zig` | Inbound message routing facade. |
| Peer bootstrap facade | `src/rpc/peer/bootstrap.zig` | Bootstrap, abort, and unimplemented-message control facade. |
| Peer finish facade | `src/rpc/peer/finish.zig` | Finish cleanup and resolved-answer cleanup facade. |
| Peer resolve facade | `src/rpc/peer/resolve.zig` | Resolve-message and pending-embargo facade. |
| Peer disembargo facade | `src/rpc/peer/disembargo.zig` | Disembargo handling facade. |
| Peer transport callbacks | `src/rpc/peer/peer_transport_callbacks.zig` | Transport event wiring. |
| Peer transport state | `src/rpc/peer/peer_transport_state.zig` | Transport-level state tracking. |
| Peer call targets | `src/rpc/peer/call/peer_call_targets.zig` | Call target resolution. |
| Peer call sender | `src/rpc/peer/call/peer_call_sender.zig` | Outbound call construction. |
| Peer call orchestration | `src/rpc/peer/call/peer_call_orchestration.zig` | Call lifecycle orchestration. |
| Peer promises | `src/rpc/promises/peer_promises.zig` | Promise pipeline tracking. |
| Peer inbound release | `src/rpc/peer/peer_inbound_release.zig` | Inbound release message handling. |
| Peer embargo accepts | `src/rpc/peer/peer_embargo_accepts.zig` | Embargo/accept flow. |
| Peer cap lifecycle | `src/rpc/peer/peer_cap_lifecycle.zig` | Capability reference counting. |
| Peer outbound control | `src/rpc/peer/peer_outbound_control.zig` | Outbound message control. |
| Peer return frames | `src/rpc/peer/return/peer_return_frames.zig` | Return message framing. |
| Peer return orchestration | `src/rpc/peer/return/peer_return_orchestration.zig` | Return lifecycle. |
| Peer return dispatch | `src/rpc/peer/return/peer_return_dispatch.zig` | Return dispatch logic. |
| Peer return send helpers | `src/rpc/promises/return_send_helpers.zig` | Return send utilities. |
| Peer forward orchestration | `src/rpc/peer/forward/peer_forward_orchestration.zig` | Forwarded-call management. |
| Peer forward return callbacks | `src/rpc/peer/forward/peer_forward_return_callbacks.zig` | Forwarded return handling. |
| Peer forwarded return logic | `src/rpc/peer/forward/peer_forwarded_return_logic.zig` | Forwarded return processing. |
| Peer provide/join | `src/rpc/peer/provide/peer_join_state.zig`, `peer_provides_state.zig`, `peer_provide_join_orchestration.zig` | Three-party handoff (provide/accept/join). |
| Peer third-party | `src/rpc/peer/third_party/peer_third_party_adoption.zig`, `peer_third_party_pending.zig`, `peer_third_party_returns.zig` | Third-party capability transfer. |
| Promised answer copy | `src/rpc/promises/promised_answer_copy.zig` | Deep-copy utility for promised answers. |
| Promise pipeline | `src/rpc/promises/pipeline.zig` | Owned promised-answer state and transform traversal utilities. |
| RPC mod (core) | `src/rpc/mod_core.zig` | Core RPC re-exports for hostless targets. |
| List readers impl | `src/serialization/message/list_readers.zig` | List reader type definitions (re-exported by `message.zig`). |
| List builders impl | `src/serialization/message/list_builders.zig` | List builder type definitions (re-exported by `message.zig`). |
| Any pointer impl | `src/serialization/message/any_pointer_reader.zig`, `src/serialization/message/any_pointer_builder.zig` | AnyPointer impl (re-exported by `message.zig`). |
| Struct builder impl | `src/serialization/message/struct_builder.zig` | StructBuilder impl (re-exported by `message.zig`). |
| Clone any pointer | `src/serialization/message/clone_any_pointer.zig` | Deep-copy impl (re-exported by `message.zig`). |

## Semver Guidance

capnpc-zig follows [Semantic Versioning 2.0.0](https://semver.org/).

- The current version is **0.17.0**. The two-party RPC core and serialization
  have been **Stable** on a frozen, CI-gated public surface
  (`docs/api-snapshot.txt`) since 0.3.0; the project is still early (0.x)
  development. See [`supported-surface.md`](supported-surface.md) for the
  authoritative consumer contract. Experimental APIs may still change between
  0.x releases per the tiers above.
- Within the 0.x series, **minor** bumps may include breaking changes to
  experimental modules. Stable modules will remain compatible within a minor
  version where possible, with breaking changes clearly documented in the
  changelog.
- Once the project reaches **1.0.0**, the stability levels above will be
  enforced strictly:
  - Breaking changes to **Stable** modules require a major version bump.
  - **Experimental** modules may break in minor releases but will be documented.
  - **Internal** modules carry no compatibility guarantees.

## Recommendations for Consumers

1. **Pin your dependency** to an exact version or commit hash until 1.0.0.
2. **Only import through `src/lib.zig`** (i.e., `@import("capnpc-zig")`),
   and prefer the domain-shaped RPC facade under `capnpc.rpc`.
   Direct imports of internal modules may break at any time.
3. **Expect RPC API churn.** The RPC runtime is under active development.
   If you depend on it, watch the changelog closely.
4. **Wire format and codegen are safe to depend on.** These layers are
   well-tested, interop-validated, and unlikely to see breaking changes.
