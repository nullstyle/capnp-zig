# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`Transport.readTimeout` — deadline reads without sockopt games
  (Stable-adjacent TCP transport; Experimental method).** A read with an
  `Io.Timeout`, built on `io.operateTimeout(.net_read)`: the deadline
  belongs to the OPERATION, so expiry cancels the read and returns
  `error.Timeout`. The alternative a downstream reached for — arming
  `SO_RCVTIMEO` on the raw fd — makes a timed-out `recv` return EAGAIN,
  which `Io.Threaded` classifies as a programmer bug (`errnoBug`) and
  turns a routine deadline into a debug-build panic; that workaround is
  now unnecessary. Proven both ways: a silent peer yields `error.Timeout`
  after the deadline (never a panic), and data arriving inside the
  deadline is delivered normally.
- **Published framing conformance fixtures**
  (`tests/fixtures/framing/framing_fixtures.json` + README). Byte
  streams to expected frames/errors for `rpc.wire.framing.Framer`,
  including reassembly across arbitrary chunk boundaries, truncation,
  the segment-count limit and its breach, count overflow, the
  `max_frame_words` breach, and a `max_buffered_bytes` breach at push
  time. Meant to be VENDORED by downstreams; capnp-zig executes the same
  file (`test-rpc-wire`) so the published bytes cannot drift from the
  implementation, and the runner asserts the fixtures' recorded limits
  against the live constants so a limit change invalidates stale
  vendored copies loudly.
- **Nightly forward-compat lane (zig dev.1786, Linux).** Builds and runs
  the transport + wire suites against a newer zig dev snapshot than
  `mise.toml` pins, so std drift lands on our gate instead of in a
  downstream tracking master more closely.

### Fixed

- **The TCP transport compiles across the std.Io net move.** Zig deleted
  the `netWrite`/`netRead` `Io.VTable` entries in favor of
  `Operation.net_write` / `.net_read` (around 0.17.0-dev.1786), which
  broke every downstream referencing `rpc.transport.tcp` on a current
  toolchain. The transport (and the test fakes that intercept socket
  writes) now select on the OPERATION's presence rather than a version
  number, so one source tree builds on both sides of the move; the read
  path already preferred `net.Stream.read`. Verified green on the pinned
  dev.1683 AND on dev.1786.

## [0.15.0] - 2026-08-27

### Added

- **Auto warm redial (`rpc.transport.quic.WarmRedialClient`;
  Experimental) — the durable-caps ladder's integration rung.** When a
  QUIC client's transport dies with the crash-restart proof
  (`Peer.lastDisconnectCause() == .stateless_reset`), the client dials a
  fresh Connection+Peer generation resumed via the latest captured
  session ticket and re-restores its saved sturdy ref, delivering the
  healed capability through an `on_rebind` callback (import ids die
  with their peer, so healing happens at the sturdy-ref layer — by
  design, not limitation). New `Peer.sendRestorePipelined` aims the
  restore call at the promised bootstrap answer so bootstrap + restore
  enqueue before the loop starts and both ride 0-RTT on the resumed
  dial — restore's idempotence is what makes the replay window
  acceptable, and the layer sends nothing else in early data. Redial
  policy: bounded attempts + fixed backoff; only `.stateless_reset`
  redials by default (`.idle_timeout` opt-in — it proves nothing about
  liveness). Proven by a crash-restart heal e2e (two generations, one
  redial, the healed capability answers, the restarted server's reset
  counter advances) and a zero-budget ablation (the reset is detected
  but NOT healed; give-up carries the certified cause).
- **QUIC half-open handshake guard (`handshake_timeout_ms` on both
  Client- and ServerOptions; new certified cause
  `DisconnectCause.handshake_timeout`; Experimental).** Half-open
  connections were IMMORTAL in the whole stack: no QUIC timer fires on
  a connection that never completes its handshake and goes quiet. On a
  server they accumulate under churn, loss, or attack until
  `max_concurrent_connections` pins and every new dial is silently
  refused — the QUIC analog of a SYN flood, observed in-house (the
  whole table `.open`, hundreds of silent `table_full` drops, dial
  workers hung forever). Servers now sweep half-opens at the deadline
  (default 10s; counter via `Server.handshakeTimeouts()`), clients
  abandon dead dials (default 30s) — both with the certified cause,
  both null-disable-able. Proven by an e2e pair plus an immortality
  ablation (guard off → the half-open provably never dies).
- **QUIC fanout server: batched datagram receive (up to 32 per pass) +
  per-`FeedOutcome` counters (`Server.feedOutcomeCounts()`).** The loop
  received ONE datagram per pass and then swept EVERY session twice, so
  per-packet cost grew linearly with session count — the measured
  saturation mechanism. Batching amortizes the sweeps; with the guard
  clearing zombie slots the old plateau is gone: 60s steady soak
  throughput went from 13.9k to 119k calls at 32 workers (8.6x) and
  from 13.8k to 128k at 128 workers (9.3x, p50 111ms -> 22.6ms). The
  outcome counters make silent refusals (`table_full`, drops, rate
  limits) a one-line report instead of an unexplainable stall; the
  soak prints them and sizes its table for post-batching churn.
- **Warm restore's server half, part 1: the 0-RTT idempotency gate
  (`ServerOptions.early_dispatch`; Experimental).** With
  `early_data = .without_replay_protection` the server previously held
  EVERY early-data frame until the handshake (replay-safe, nothing
  executes early). `.restore_only` now executes the idempotent PREFIX of
  the early flight — Bootstrap frames and Calls on the pinned Restorer
  interface — before the handshake, so a warm restore answers without
  waiting; the first non-qualifying frame parks (order preserved) until
  the handshake lands, keeping the replay window closed for everything
  non-idempotent. `.hold_until_handshake` stays the default and the
  hardened posture; baseline mode only (native always holds). Proven by
  network-free engine tests: prefix dispatch, in-order drain of the
  parked frame, hold-mode inertness, no-leak parking, and the
  classifier's exact vocabulary (with the mirrored Restorer id asserted
  equal to the pinned persistence constant).
- **Warm restore's server half, part 2: the warm sturdy-ref envelope
  (`quic.warm_state`; Experimental).** `{session ticket, NEW_TOKEN}`
  encode/decode as ONE persistable blob — upstream delivers them through
  separate channels, and only together do they buy the full
  one-round-trip, address-validated resume. `WarmRedialClient` now
  captures NEW_TOKEN alongside tickets (latest-wins, tear-free), sends
  both on every redial, and gains `exportWarmState`/`seedWarmState` so
  an application can persist the warm half beside its sturdy-ref bytes
  and resume warm across a PROCESS RESTART. The codec is
  dependency-free, exported in the non-QUIC build too, and registered
  as a coverage-guided fuzz target (decode must reject or round-trip
  canonically).
- **Soak: healing workers (`--heal-workers K`, QUIC only) — the
  churn-scale self-healing proof.** K workers each run one persistent
  `WarmRedialClient` (restore-backed, echo-chaining) for the whole run
  while the rest churn; the soak server peers now serve the vat restore
  convention. New gates: zero give-ups, echo traffic flowed, and — when
  paired with abrupt-death mode — EVERY healing client healed across
  EVERY death. First run (8 healers + 8 churn workers, kill every 2s,
  14s): 7 deaths × 8 clients = exactly 56 redials and 64 rebinds — a
  perfect heal lattice — 19,750 healed-path echo round trips, zero
  give-ups, zero unexpected exceptions.
- **Nightly: the self-healing arc gated end to end** — the extended
  gates run a 20s QUIC soak with abrupt deaths every 2s and half the
  workers healing, failing unless every death is reset-certified and
  every healing client heals.
- **Soak: abrupt-death chaos mode (`--abrupt-death-every-ms N`, QUIC
  only).** Every N ms the harness kills the echo server with NO close
  ceremony and restarts it on the same port with the same
  `stateless_reset_key` — the crash-restart shape the death certificate
  exists for. Surviving clients' next datagrams draw stateless resets
  and their sessions close `.stateless_reset` instead of stalling to
  idle timeout. New pass gates: deaths executed, at least one
  reset-certified client close, and reset traffic observed at the
  server; reset counters now accumulate across server incarnations.
  First field data (12s, kill every 2s, 8 workers): 6 deaths, 8 resets
  sent, 8 `.stateless_reset` session closes, zero unexpected
  exceptions. This is the instrument the auto-warm-redial rung's soak
  proof will run against.

### Changed

- Docs/process hygiene: the RELEASING.md semver table gains an explicit
  "new functionality: additive declarations on any tier → minor" row
  (previously argued from precedent each cut); stale point-in-time QUIC
  evidence counts in quic-transport.md/stability.md replaced with
  gate-anchored wording (`tools/quic_test_evidence.zig` floors);
  stability rows refreshed (DisconnectCause named in the Events row;
  Windows QUIC acceptance no longer described as pending); lib_quic's
  `canonical` doc comment now says Stable.

## [0.14.0] - 2026-08-26

### Added

- **`Message.initFlat` / `initFlatUnvalidated` (Stable).** Validating
  (and shape-only) decode of bare, table-less single-segment bytes — the
  exact form `canonical.canonicalizeFlat` and `capnp convert
  binary:canonical` emit — without fabricating a synthetic segment
  table. Zero-copy: the single segment aliases the caller's bytes; same
  validation walk and `ValidationOptions` limits as `init`. Registered
  as a coverage-guided fuzz target, since flat bytes arrive from
  untrusted peers in downstream consensus protocols.
- **`canonical.canonicalizeFlatFromBuilder` / `canonicalizeFromBuilder`
  (Stable).** Canonicalize a `MessageBuilder`'s current contents
  directly — byte-identical to the serialize → re-parse → canonicalize
  round trip it replaces (differentially tested, including
  multi-segment builders with far pointers), with zero allocation
  overhead beyond `canonicalizeFlat` itself and no redundant validation
  walk over bytes the local builder just wrote.
  `CanonicalizeFromBuilderError` adds the round trip's
  `TruncatedMessage` verdict for a builder whose root segment holds no
  root word.

### Changed

- **The schema-free `canonical` module is Stable and FROZEN** (was
  Experimental): `canonicalize`, `canonicalizeFlat`, `isCanonical`,
  both error sets, and the new FromBuilder pair now live in the
  CI-gated `docs/api-snapshot.txt` contract, and `Message.initFlat` /
  `initFlatUnvalidated` freeze under the existing `message` prefix.
  Deliberate promotion: downstream consensus consumers pin their
  signing preimages to `canonicalizeFlat`'s bytes, where drift is a
  permanent network fork rather than an API break. Byte behavior stays
  pinned by the C++ acceptance-suite ports and the
  `capnp convert binary:canonical` differential suite.

### Fixed

- **TCP soak abort at >=32 workers (macOS/Linux).** `setTcpNoDelay`
  routed through `std.posix.setsockopt`, whose EINVAL/EBADF errno arms
  are `unreachable` — so a peer resetting its connection between
  `accept()` and the option call aborted the whole process. Now issued
  as the raw syscall with every failure treated as a skipped
  optimization. Regression test drives the same std arm via a dead fd.
- **Windows teardown abort under parked accepts.** `WorkerPool` closed
  the listen socket while workers were parked in `accept()`; Windows
  completes the parked AFD wait with STATUS_CANCELLED, which std's
  `netAcceptWindows` treats as unreachable (process abort). Teardown
  now counts parked acceptors, retries the self-connect wake-ups
  (previously one failed dial abandoned ALL remaining wake-ups), and
  closes only once nobody is parked (bounded by a 2s valve). The
  Nightly 64-worker ReleaseSafe soak lane now runs on BOTH matrix OSes
  — 20s on Windows: a 60s run measurably exhausts the ~16k ephemeral
  ports (TIME_WAIT=16,224 observed), and under port exhaustion no
  user-space teardown can dodge the std defect (upstream report
  candidate: `.CANCELLED => unreachable` in netAcceptWindows).

## [0.13.0] - 2026-08-26

### Breaking

- **QUIC `Server.init` now refuses a hand-set
  `transport_params.stateless_reset_token` without a
  `stateless_reset_key` (`error.InvalidConfig`, via the quic v0.16.1
  pin).** Only opt-in QUIC consumers are affected. At v0.12.0 that
  configuration initialized and ran — and was a live security footgun:
  the one fixed token was advertised unchanged to every peer, letting
  any peer that completed a handshake reset any other peer's
  connection, and RFC 9000 §18.2 means a static token in transport
  params could never be honored per-CID anyway. No first-party code
  ever set the field. **Migration:** delete the hand-set
  `transport_params.stateless_reset_token` and set
  `ServerOptions.stateless_reset_key` instead; quic-zig then derives a
  correct per-connection-ID token for every CID it issues.

### Added

- **Typed disconnect causes over QUIC (the "death certificate";
  Experimental).** quic-zig has carried a full typed close certificate
  since v0.15 (sticky `closeEvent()`, `CloseSource` incl.
  `stateless_reset`); this transport never read it — every remote death
  reached the peer layer as one indistinguishable disconnect, and the
  fanout server even recorded `.normal` for remote-caused session deaths.
  Now: `rpc.events.DisconnectCause` (unknown / local_close / peer_close /
  idle_timeout / stateless_reset / transport_error); QUIC `Connection`
  and `ServerSession` capture the cause on their terminal paths and
  expose `closeCause()` (+ raw `quicCloseEvent()`); `Peer` picks it up
  through the same capability-detection seam as `on_tick` and exposes
  `lastDisconnectCause()` — already set inside the question callbacks the
  disconnect cancels and inside `on_close`. The synthetic exception's
  reason text stays exactly "disconnected" for every cause, so existing
  matchers (and the TCP transport, which stays `.unknown`) see zero
  change. `.stateless_reset` is the crash-restart proof the durable-caps
  restore ladder keys on. New server surfaces: `ServerOptions
  .stateless_reset_key` (forwarded; enables the §10.3 emitter and the
  §18.2 handshake-CID token clients need for that proof) and
  `Server.statelessResetsSent()`. Proven by three e2e tests, including a
  full crash-restart: server destroyed with no close ceremony, restarted
  on the same port with the same key, and the client's next call converts
  the stateless reset into `DisconnectCause.stateless_reset` at every
  promised observation point (ablation-verified). The QUIC soak now runs
  with a reset key and reports per-cause session close distributions plus
  `unroutable_dcid`/reset counts. First measurement (60s churn, 8
  workers): every session closes `local_close`, resets and unroutable
  DCIDs both zero — the soak's churn is GRACEFUL, so it exercises the
  counters' plumbing but not yet the reset path itself. Producing real
  reset field data needs an abrupt-death chaos mode; the instrument is
  now in place for it.

### Changed

- **quic pin → v0.16.1** (no wire behavior changes, and both API
  snapshots are byte-identical across the bump; the one behavioral
  tightening is the init-time refusal filed under **Breaking**
  above). This release exists
  because of a finding from this repo: without
  `Server.Config.stateless_reset_key` there is no client-side
  crash-restart DETECTION, not merely no emission. Upstream's audit of
  that found the same null key also silently disables auto CID
  replenishment, client-initiated migration, and peer CID rotation on NAT
  rebinding — and turned up a live footgun, a hand-set
  `transport_params.stateless_reset_token` advertised unchanged to every
  peer, letting any peer that handshook reset any other peer's
  connection. v0.16.1 refuses that keyless combination with
  `error.InvalidConfig` at `Server.init`. Our `ServerOptions
  .transport_params` guard, which was the only protection at v0.16.0, now
  documents the enforced behavior instead of warning about it.

## [0.12.0] - 2026-08-21

### Added

- **QuicVatNetwork (durable-caps ladder, first out-of-process VatNetwork;
  Experimental).** `rpc.vat.quic_network` mints Level-3 introductions whose
  `ThirdPartyToContact` is a provision ticket `{version, dictated initial
  DCID, nonce, vat key, reset token (reserved), address hints}` and redeems
  them against a pre-established pool of live peers (`addVat` /
  `registerPeer`; redemption inside frame dispatch never dials). One
  deterministic encoder produces both `to_await` and the Accept completion,
  upholding the byte-identity invariant VatC's provide table keys on.
  Nonce and DCID come from a fail-closed CSPRNG (explicit seed or
  `io.randomSecure`) — verified against quic-zig HEAD that upstream
  validates only the 8..20 DCID length, so unpredictability is the
  minter's job. Adapter plumbing landed with it: `ClientOptions
  .initial_dcid` (validated, forwarded) and `ServerSession.initialDcid()`
  (the rendezvous matching hook). Proven by a transport-free unit suite
  (codec, foreign-token rejection, OOM-clean) and two QUIC e2e tests,
  including a FULL three-vat L3 handoff over three real QUIC connections
  (fanout VatC with a shared ProvisionIndex; ticketed auto-pickup; direct
  call on the handed-off cap). v1 boundaries recorded in
  docs/quic-durable-caps-plan.md: pool-only redemption, empty reset_token,
  no Retry on provision dials, VatC admission left to the embedder.

- **Warm restore over QUIC 0-RTT (durable-caps prototype #2, both halves).**
  Client half: `ClientOptions` gains the resumption surface:
  `resumption_state`, `new_session_callback`, `new_token`,
  `new_token_callback` (+user_data). A resumed dial opens its RPC stream
  before the handshake completes, so frames enqueued before the loop
  starts ride early data; on 0-RTT rejection quic-zig requeues the staged
  bytes verbatim at 1-RTT, so a stale ticket costs a round trip, never
  data. Proven by two e2e tests (accepted-0-RTT round trip; stale-ticket
  1-RTT fallback). Server half: with `early_data =
  .without_replay_protection`, both stream engines now HOLD dispatch of
  RPC frames that arrived in early data until the handshake completes — a
  replayed first flight can never complete a handshake, so the
  replay-execution window closes at the transport while the round trip
  stays banked (the frames are already buffered when the handshake
  lands). `.with_anti_replay` dispatches immediately (the TLS tracker
  already guarantees single use). Ablation-proven e2e: gate off, a
  replayed flight executes; gate on, red turns green. Method-aware
  restore-inside-0-RTT (RPC-layer idempotency marks) remains future work.
- **QUIC soak variant** (`zig build -Dquic=true soak -- --transport quic`):
  the previously TCP-only soak now measures QUIC steady-state heap and
  per-connection footprint — the instrument the last two upstream bumps
  lacked. Its first run found that the QUIC transport could not drive
  Peer call-deadlines at all (no `on_tick` plumbing); fixed by the
  deadline-parity entry below, and the soak's deadline sessions now run
  over QUIC too.
- **QUIC transport drives Peer call-deadlines (`on_tick` parity with
  TCP).** Client `Connection` and fanout `ServerSession` both carry an
  `on_tick` hook (1ms-floored, callback-depth-accounted, invoked after
  each service pass), which `Peer.attachConnection` binds to its deadline
  sweep — before this, a call deadline over QUIC silently never fired.
  Proven by a deadline-cancellation e2e over real QUIC. Sharp edge worth
  knowing: a small `default_call_timeout_ms` armed before `sendBootstrap`
  can expire the bootstrap question during the QUIC handshake itself —
  arm per-call deadlines, or arm defaults after bootstrap returns.

### Changed

- **quic pin → v0.16.0** (via v0.15.1, both cold-verified). v0.15.1
  retired the two latent transport wedges (pacer refill freeze;
  flow-control credit starved behind the pacing gate). v0.16.0 flips
  the congestion default to BBRv3 — and this transport FOLLOWS the
  flip per its recorded defaults policy: `ClientOptions
  .congestion_control` default is now `.bbr`, and `ServerOptions`
  gains the same knob (previously absent, which silently split
  posture: servers followed upstream's default while clients used our
  field default). `.cubic` remains the one-line rollback on either
  side. The soak gained `--cc default|cubic|bbr|newreno` for A/B runs;
  measured on identical v0.16 code (60s, 8 workers, churn + deadline
  traffic, loopback caveats apply): BBR vs CUBIC is a strict
  no-regression — every metric (calls, p50, p99, steady heap ~30.3MB)
  within the cubic configuration's own run-to-run spread, with p50
  ~0.7ms higher as the only consistent visible shift. An earlier
  version of this entry attributed a +10% cubic-to-cubic throughput
  gain across the bump to upstream's delivery-rate sampler fix; that
  was wrong twice over — the sampler feeds only BBR's model, and
  repeat runs (v0.15.1: 6101/7220/6823 calls; v0.16: 6723/6736/6586)
  show the original baseline was simply the low outlier. Cubic
  throughput across the bump is flat within variance.
- **quic pin: v0.12.0 → v0.14.0 → v0.15.0** (each validated by building
  against a pristine cache; full suite, QUIC suite, api-snapshot, and
  docs-snippet gates green). v0.14 brings `Client.Config.initial_dcid`
  (the rendezvous dial), `quic.app`/`quic.testing`, and per-stream
  early-data flagging; behavior note: `streamRead` on a locally-
  initiated uni stream now returns `error.StreamNotReadable` instead of
  0-forever — no capnp-zig path trips it. v0.15 brings the stateless-
  reset emitter (`FeedOutcome.stateless_reset_sent`,
  `LogEvent.unroutable_dcid`, §18.2 primary-CID reset-token advertise)
  and `earlyDataSendWindow()` — the observation surfaces the death-
  certificate ladder rung builds on.

  Soak measurements under churn (60s, 8 workers, ~16 live connections),
  all from COLD caches after the stale-cache lesson below: v0.12.0
  61.16MB → v0.14.0 20.94MB → v0.15.0 21.26MB steady-state live heap.
  The upstream per-connection reduction REPRODUCES under this workload
  (−66%, ≈−2.5MB per live connection, landing with their v0.13 tracker
  shrink), and v0.15 is flat over v0.14 as upstream predicted. An
  earlier measurement showing v0.14 flat at 61.9MB was an artifact: the
  soak binary had been built from a stale local `.zig-cache` module
  graph still resolving v0.12 sources after the pin bump. The same
  staleness made the api-snapshot tool verify its own outdated render.
  **After any dependency pin bump, purge `.zig-cache` (or build from a
  fresh checkout) before trusting locally built evidence.**

### Fixed

- **Nightly was red for nine consecutive nights (2026-08-12..08-20), two
  independent causes, both fixed and CI-verified green:** the
  extended-gates job never installed the `capnp` CLI that the schema
  fidelity suite hard-requires (nightly.yml gained the setup step), and
  the Windows soak lane's "leak" was the harness measuring its own
  latency-sample buffers (~6 B/call) — Windows merely completes 2-4x the
  calls of the Nagle-capped Linux client. Soak telemetry now allocates
  outside the counted allocator; no actual leak existed (every red run's
  terminal leak check passed).
- **Baseline QUIC engine could strand parsed frames forever**: frames
  buffered while callbacks were not yet bound (e.g. 0-RTT data arriving
  during the handshake) were only dispatched when NEW bytes arrived;
  service now attempts dispatch of buffered frames every pass.
- **Windows: a departed or off-path peer could kill the whole QUIC
  endpoint.** The fanout server and listener receive paths propagated
  ICMP-class datagram faults (`PortUnreachable`,
  `ConnectionResetByPeer` — surfaced eagerly by Windows sockets) as
  endpoint-fatal errors. The per-datagram transient-fault classifier
  already existed in `datagram_io`; four receive sites lacked it. Such
  faults are now dropped per-datagram with telemetry, matching the
  single-connection path.
- **Linux ReleaseSafe builds with QUIC failed to link** after the quic
  pin began compiling BoringSSL under Zig's default C sanitizers
  (undefined `__ubsan_handle_*` symbols). Both `dependencyLazy("quic")`
  sites now forward `sanitize-c=trap`, restoring the release-build lane
  without linking a UBSan runtime.

## [0.11.0] - 2026-08-13

### Breaking

- **The frozen API contract for `codegen` narrowed from 54 declarations to
  20.** Only the plugin contract stays frozen: `Generator.init` / `deinit` /
  `generateFile`, the five `set*` configurators, and `ApiProfile` /
  `CodegenBudget` with their shapes. **Migration:** none expected — what left
  the frozen tier is internal state that was swept in by prefix breadth rather
  than by decision (`Generator`'s own fields, `TypeGenerator`'s helpers, and
  `ArrayListWriter`, which appears in no frozen signature). Anything reaching
  into those is now explicitly outside the stability promise and free to move.
  See the Changed entry below for why this was worth doing.

### Added

- **A QUIC throughput benchmark (`bench-quic`, gated by `bench-check-quic`).**
  Nothing in this repo measured the QUIC transport, so a green `bench-check`
  said nothing about it — all benchmarks were TCP. Three modes: `sequential`
  (round-trip latency), `pipelined` (call throughput), and `bulk`, which pushes
  large payloads with several transfers outstanding and reports bytes/sec.
  Separately baselined in `bench/baselines-quic.json`, because the binary only
  exists under `-Dquic=true`.

  **Scope, measured rather than assumed.** It exercises our QUIC stack end to
  end — TLS handshake, framing, stream scheduling, the Peer call path — and
  will catch a regression in any of them. It does **not** separate
  congestion-control variants. The `--no-pacing` flag exists to demonstrate
  that rather than to hide it; on loopback, 300×64KiB:

      pacing on   11.44  11.73  12.52  MiB/s
      pacing off  11.30  11.37  11.74  MiB/s

  A ~4% difference in means inside a ~9% run-to-run spread is not signal. That
  is structural: loopback has no bottleneck, so cwnd grows unbounded, the
  pacer's rate ceiling never binds, and the limiting factor is our own
  serialization rather than the network. Validating CUBIC/pacing/HyStart++
  needs a constrained link — upstream's QUIC Network Simulator and quic-go
  interop gates. So quic-zig's congestion defaults remain unvalidated *here*,
  and this benchmark does not pretend otherwise; what changed is that our
  transport is now measured at all.

### Changed

- **`generator.zig` decomposed: 4,517 → 3,251 lines, with generated output
  byte-identical.** Two tranches, both comptime-generic siblings over the
  Generator type with thunks left behind, so every call site is unchanged:
  `interface_gen.zig` (868 lines — interface/RPC emission, the generated
  `Client`/`Server`/`VTable`, per-method structs, streaming and pipelined call
  machinery) and `name_validation.zig` (625 lines — every check that runs
  before emission, so a bad schema becomes a clear error rather than code that
  will not compile).

  The `Generator` type itself does not move: the API snapshot renders literal
  declaration paths and its entry points are still frozen. What did move is
  internal, and what widened to `pub` is Experimental — which is precisely what
  the freeze narrowing below bought, and why this was impossible before it.

  Verified by regenerating every committed artifact (RPC schemas, e2e
  bindings, goldens, all four examples, WASM) and diffing: identical. The
  decomposition stopped here on measurement — `Generator` now ends around line
  1956, the rest of the file is inline tests, and the largest remaining cluster
  is 103 lines.

- **Codegen internals are no longer part of the frozen API contract.** The
  Stable tier claimed 54 declarations under `codegen`; it now claims 20. What
  stays frozen is the plugin contract a consumer actually depends on —
  `Generator.init` / `deinit` / `generateFile`, the five `set*` configurators,
  and the shapes of `ApiProfile` and `CodegenBudget`, which callers construct.

  What moved to Experimental is internal state that was frozen by accident of
  prefix breadth rather than by decision: `Generator`'s own fields
  (`node_map`, `shape_share_map`, `allocator`, …), `TypeGenerator`'s helpers,
  and `ArrayListWriter` in its entirety — a writer named in no frozen
  signature. **Migration:** none expected, but anything reaching into those
  is now explicitly outside the stability promise and free to move.

  This was a blocking decision, not housekeeping. Because Zig's privacy is
  file-scoped, a blanket freeze over `codegen` made `generator.zig` and
  `struct_gen.zig` — 9,029 lines, the largest un-decomposed units in the tree —
  effectively unsplittable: extracting any cluster of private methods forces
  the helpers it calls back into to become `pub`, and each one would have
  landed in the frozen tier permanently. The rule is now an explicit list, so
  freezing a new entry point is a deliberate act rather than a side effect of
  how wide a prefix happens to be.

### Fixed

- **The QUIC library root ran none of its own tests.** `src/rpc/mod.zig` forces
  its subtree through semantic analysis so `test` blocks join the compilation,
  but deliberately skips the `quic` declaration — correct there, since in a
  default build it is the disabled stub whose every declaration is an
  intentional `@compileError`. The bug was that `src/rpc/mod_quic.zig`, where
  the module is real, had no walk at all, so under `-Dquic=true` nothing under
  `src/rpc` was analysed, the QUIC transport included. The walk is now shared
  with a `skip_quic` parameter.

  That surfaced 19 previously-dead tests (325 → 344) and three defects in them,
  all invisible while they never ran: a framer test declaring a 4096-byte frame
  against a 1024-byte ceiling, so decode rejected it before any assertion ran;
  a malformed-frame case reusing an 8-byte framer, so `push` tripped the buffer
  budget and the reserved-byte validation it exists to check never executed;
  and a leak from reassigning a variable that already carried a deferred
  `deinit`. In all three the code was correct and the tests were stale — the
  bounds guard attacker-declared lengths and were added after those tests were
  written, with nothing re-running them to notice.

- **QUIC congestion knobs were unreachable through this transport.**
  `ClientOptions` now carries `congestion_control`, `enable_pacing` and
  `enable_hystart`, forwarded verbatim to quic-zig. The v0.10.0 notes told
  consumers each default flip had a one-line opt-out — true of quic-zig, but
  our client factory forwarded only TLS/ALPN/transport-params, so the
  documented lever was not reachable from here. Defaults mirror upstream's
  rather than pinning the old behaviour: this transport follows its backend,
  and pinning silently would hide the very change the fields exist to expose.

## [0.10.0] - 2026-08-13

### Breaking

- **Minimum Zig is now `0.17.0-dev.1683`.** Earlier dev builds will not
  compile this release. **Migration:** upgrade your toolchain; `mise.toml`
  carries the exact pin this repo builds and tests against. Note that
  `ziglang.org/builds/` garbage-collects dev tarballs, so a pin much older
  than the current master eventually stops resolving.

- **`rpc.transport.tcp.runtime.Listener.init` gained `AccessDenied` in its
  error set.** This is a *frozen-tier* change, inherited from std's `listen`
  rather than invented here. **Migration:** consumers switching exhaustively
  on that error set must add a case. Narrowing it back was rejected — it would
  mean swallowing a genuine bind-permission failure on privileged ports.

- **The QUIC dependency is renamed `quic_zig` → `quic` and bumped to
  v0.12.0.** Only opt-in QUIC consumers are affected. **Migration:** the
  `build.zig.zon` dependency key and the `dep.module(...)` name both become
  `"quic"`; the repository URL is unchanged (still `quic-zig`). The manifest
  fingerprint changed with the rename, so there is no hash continuity — expect
  a fresh fetch. Crossing this bump also adopts quic-zig v0.11.0's wire
  defaults (CUBIC, pacing, HyStart++ all default-on), each with a one-line
  opt-out: `congestion_control = .new_reno`, `enable_pacing = false`,
  `enable_hystart = false`.

- **`error.DatagramTooLarge` no longer appears in the QUIC `Server` /
  `Listener` `receiveOne` error sets.** An oversized datagram is now dropped
  and counted rather than failing the step, so the error became unreachable.
  **Migration:** remove the arm; observe drops through
  `droppedDatagramCount()`, `StepResult.dropped_datagram`, or the
  `udp_datagram_bytes` resource event.


### Fixed

- **One spoofed UDP datagram could take down a QUIC fanout server and every
  session on it.** The single-connection receive loop already treated an
  oversized (truncated) datagram as a per-datagram fault, and said why: "UDP
  is unauthenticated and spoofable, so a single oversized datagram from any
  host must not tear down the endpoint (and, for a fanout server, every
  session on it)." The fanout paths did exactly that, on every platform.
  `Server.receiveOneFor` and `Listener.receiveOne` both returned
  `error.DatagramTooLarge`; it propagated out of `Server.stepOnce` through
  `try`, and `Server.run` responds to a failed step by closing the server. Any
  host able to reach the UDP port had a one-packet kill switch for the whole
  endpoint. (The 64 KiB default `udp_rx_buffer_size` is above the 65507-byte
  IPv4 UDP payload ceiling, so this only ever reached endpoints tuned closer
  to their path MTU — a normal thing to do.)

  All receive paths now share one policy in
  `src/rpc/transport/quic/datagram_drop.zig`: drop the datagram, count it,
  `warn`, and keep serving. The divergence existed because the policy was
  written twice; it is now written once, so a future divergence has to be a
  deliberate edit rather than an omission.

  Because a dropped datagram is otherwise invisible to the application — and a
  legitimate peer exceeding `udp_rx_buffer_size` is indistinguishable from an
  attacker — the drop is also published as a redacted observer event:
  `resource_rejection` with the new `events.Resource.udp_datagram_bytes`,
  `limit` set to the rx buffer size, `attempted` deliberately `null` (neither
  platform can report the true size), and `err = error.DatagramTooLarge`. New
  `Server.droppedDatagramCount()` / `Listener.droppedDatagramCount()` expose
  the tally per UDP endpoint, and `StepResult.dropped_datagram` reports it per
  step — including on the single-connection path, which previously logged the
  drop and told no one. `receiveOne` now returns `null` for a drop, the same
  as a timeout or wake, since none of the three is actionable by the caller;
  `DatagramTooLarge` is correspondingly gone from both `receiveOne` error sets
  in the experimental API snapshot.

  Two regressions cover it: a live fanout session that round-trips, absorbs a
  spoofed oversized datagram from an unrelated socket, and round-trips again
  with its session intact; and a bare `Listener` drop. Both were
  ablation-checked — restoring `return error.DatagramTooLarge` in either
  component fails its own test with exactly that error, and only that test.

- **The cross-target compile matrix builds again, on every target.** Two
  independent breakages, both long-standing:

  `tools/soak_rpc.zig` declared its twenty counters as
  `std.atomic.Value(u64)`. Zig's atomic builtins reject operands wider than
  the pointer width, so `zig build check-compile -Dtarget=x86-linux-gnu`
  failed with nine errors inside `std/atomic.zig` — the `Cross-target compile
  (x86-linux-gnu)` CI job had been red on this. The counters are now `usize`,
  which is the same u64 on every 64-bit machine the soak harness actually runs
  on; the 32-bit build exists purely as compile-only rot detection.

  A thread-affinity assertion still compared `@as(?std.Thread.Id, ...)`
  against a field widened to `?u64` (see the platform-stable snapshot work
  below). `std.Thread.Id` is u32 on Linux and Windows but u64 on macOS, so
  this compiled on a macOS host while `check-test-compile -Dtarget=
  x86_64-windows` failed.

- **QUIC's cross-thread wake door could write to a recycled file descriptor.**
  `request()` (callable from any thread) read `Handle.fds` and wrote the
  socketpair with no synchronization against `deinit()` (owner thread) nulling
  and closing them — the exact write-to-recycled-fd shape `tcp/connection.zig`
  already guarded with `wake_mu`. A spin mutex inside the `Handle` is now held
  across the fds read+write in `request()` and across the null+close in
  `deinit()`, covering the compat `Connection`, `Server`, and `ServerSession`
  at once. Loop-thread paths stay unlocked: only the owner closes, and it
  cannot poll and deinit simultaneously. A new 4-thread `request()` storm
  racing owner `deinit()` over 200 handle lifecycles — plus a TCP `wake()`
  storm racing `Connection.deinit` — exercises the interleavings the mutexes
  exist for.

- **A hostile or duplicated answer id could leak a results frame.**
  `completeSelfLoopbackReturn` stashed the frame under the answer id with a
  plain `HashMap.put`, so a second self-loopback Return on the same id before a
  `takeFromOtherQuestion` consumed the first silently overwrote and leaked it.
  Now `fetchPut` + free of the displaced frame. Found by the new
  structure-aware fuzzing below within a two-minute coverage-guided run, and
  pinned by a focused regression that drives the exact two-call sequence
  (fails pre-fix under the testing allocator).

- **Windows CI failed with every test passing, because of a listening-socket
  `shutdown()`.** `Listener.close` called `shutdownFd` before `closeFd` since
  on POSIX a bare close does not reliably wake a thread parked in `accept()`;
  the comment claimed this was "harmless on Windows". It is not — `shutdown()`
  on a *listening* socket is invalid there, fails with `INVALID_PARAMETER`, and
  std routes that through `unexpectedStatus`, whose debug diagnostic prints a
  stack trace and leaves the process exiting non-zero even though our
  `catch {}` discards the error value. The result was 1858/1861 passing, zero
  failing tests, and a red job. Windows `closesocket` already unblocks a
  pending accept, so the call is now POSIX-only. Long-standing; it surfaced
  only once a Windows leg survived long enough to finish.

- **Five CI jobs could not pass, and had been red since before this branch.**
  Only the primary `test` job installed `capnp` or checked out submodules, yet
  the hardening, QUIC-transport and ReleaseSafe jobs all reach the
  schema-fidelity and codegen roots, which **fail rather than skip** without
  the tool (deliberate policy, so codegen parity cannot regress into silent
  skips). The install and its presence assertion now live in a
  `.github/actions/setup-capnp` composite action wired into all four
  consumers, and the hardening job gained `submodules: recursive`.

- **The experimental API-snapshot gate rendered differently per platform.**
  `SockAddrStorage.any` names `std.posix.sockaddr` everywhere, but `@typeName`
  reports the *resolved* declaration — translate-c on macOS,
  `os.linux.sockaddr` on Linux — so the strict gate went red on Linux while
  passing on macOS. Fixed at the renderer, where it belongs: the snapshot
  exists to catch drift in *our* surface, not to freeze one OS's spelling of a
  std type. A grep for the remaining platform-variant token families finds no
  other cases, so the experimental surface is now platform-stable by
  construction rather than by assumption.

- **The ThreadSanitizer lane had never once finished, and the reason was the
  hang above, not the timeout.** It carried `timeout-minutes: 20` and was
  killed at exactly 20 minutes on every run since it was added, then at 45
  once raised — so it reported no result while looking configured. It runs the
  cross-thread transport suites, which is exactly where the wake-drain loop
  lived. With that fixed it completes in about four minutes: 11/11 steps,
  26/26 tests, **no races reported**. `docs/stability.md` now states that as
  measured coverage instead of a configured job.

### Fixed

- **A Linux-only infinite loop made four CI legs hang; `std.posix.system`
  changes its return type between platforms.** `Test (ubuntu-latest)` was green
  through `bf70eae` and had not passed since. The signature was a hang, not
  slowness: last line printed, then total silence until the step cap — 16.7
  minutes of silence at a 20-minute cap, 32 at a 35-minute cap — leaving
  orphaned `maker` (the build process) and `test` (a test binary) behind. Two
  runs with two different `--seed` values stalled identically. macOS and
  Windows passed the same suites.

  `std.posix.system` is the *platform's* syscall layer, and the halves disagree
  about the return type:

      linux  std.os.linux.read -> usize   raw syscall; failure is -errno
                                          reinterpreted as a huge POSITIVE
      macOS  std.c.read        -> isize   libc shim; failure is -1

  The wake-pipe `drain()` classified on the sign before consulting errno, so on
  Linux every error read as "bytes received". `EAGAIN` is the ordinary way a
  non-blocking drain finishes — the pipe is empty — so the loop never
  terminated and the errno switch was unreachable. Measured in a Linux
  container: **795,914 `read()` calls in 3 seconds, every one an error**,
  confirmed by strace plus a stack trace showing `Handle.drain` → `read` with
  four waker threads spinning.

  All three instances in the file are fixed, because the same ordering appeared
  three times with three different consequences: `drain()` looped forever,
  `writeByte()` returned on the `rc > 0` arm and so reported every failed wake
  as delivered, and `waitForSocket()` classified a *failed* `poll()` as
  "descriptors ready" and read `revents` the kernel never populated. The fix is
  the idiom `tcp.connection.pollRetryIntr` already used — its comment says
  "classified via errno rather than the sign of the return value".

  Also corrects the record: raising the `Test` job's caps (30→45 job, 20→35
  step) was committed on the theory that the leg needed more time. The silence
  disproves that — it hung longer.

- **A TCP wake-door deadlock hung the Linux CI legs.** Distinct from the
  wake-drain infinite loop above: that one was QUIC and deterministic, this is
  TCP and a race — which is why one Linux leg could hang while another ran the
  identical suite clean on the same commit. Captured with all three threads
  live:

      TID 3626  state=S  sendmsg <- netWritePosix <- Connection.wake  [HOLDS wake_mu]
      TID 2820  state=R  lockWake <- deinitNow <- Connection.deinit   [spins for it]
      TID 3625  state=R  lockWake <- Connection.wake                  [spins for it]

  `wake()` holds `wake_mu` across the write so teardown cannot close the
  descriptor mid-write — but the write could BLOCK, and the only thread that
  drains the channel is the one in `deinit`, waiting for that same lock. Once
  the pipe filled, the writer slept holding the lock and the drainer spun
  forever. Structural: the buffer size decides when, not whether.

  QUIC never had this, because its wake door differed in TWO coupled ways —
  non-blocking descriptors AND a raw `write` rather than `io.vtable.netWrite`.
  Both had to move: `Io.Threaded.netWritePosix` treats `EAGAIN` as `errnoBug`
  and panics on it, since it assumes a blocking descriptor. So TCP now does
  what QUIC always did, and both doors share one `setNonBlocking` and one
  `writeByte` — two implementations drifting apart being the whole defect.

  Proven on Linux: the suite that deadlocked at 1x now passes, and passes at
  20x reps.

- **Receive faults a remote peer can provoke no longer tear down a QUIC
  endpoint.** `PortUnreachable` and `ConnectionResetByPeer` are ICMP feedback —
  std's own docs describe them as queued against the bound socket and reported
  at the next receive — so they describe a datagram that did not arrive, not a
  broken endpoint, and an off-path packet can provoke them. They were fatal.
  Now they are per-datagram faults on both the POSIX and Windows arms. Local
  faults (`SystemResources`, fd exhaustion, `SocketUnconnected`, `NetworkDown`,
  `Unexpected`) deliberately still propagate; the test enumerates both halves,
  because a classifier widened to swallow everything would turn a broken
  endpoint into a spin.

- **The source-module tests ran nowhere.** `test-lib` existed but nothing
  depended on it — not `test`, not any domain step, and it appeared in no CI
  job or Justfile recipe. That is 322 tests by default and 325 with
  `-Dquic=true` that had never executed. Tests in `src/` are only collected
  when `src/lib.zig` is the ROOT module, which is exactly what that step does;
  the `tests/` roots import `capnpc-zig` as a separate module, and Zig does not
  collect tests from non-root modules. Now wired into `test`. This does not
  reach files past the `refAllRecursive` depth (verified by ablation), which is
  tracked separately.

- **`std.debug.panic` bypassed the hardening gate.** The scanner matched only
  the literal `@panic`, leaving a second, unreviewed way to abort the process.
  Both spellings are scanned now. Exactly one allowlist entry was required
  repo-wide, so nothing had been slipping through historically.

- **Windows QUIC runtime acceptance is earned.** `QUIC targeted transport
  (windows-latest)` now runs both native evidence roots, Debug and ReleaseSafe,
  with `SkipZigTest` rejected, and passes — see the transport entry above for
  the defect that was blocking it. Scope is the job's own step list: only the
  evidence roots run on Windows; the full QUIC-root suite, build-graph check,
  strict QUIC snapshot and doc-snippet fixtures remain Linux-only.

### Changed

### Changed

- **The QUIC dependency is renamed and bumped: `quic_zig` v0.10.1 → `quic`
  v0.12.0.** Upstream renamed the package (the *repository* is still
  `quic-zig`, so only the manifest key, the `dep.module(...)` name and our
  imports move). The manifest fingerprint changed with the name, so this is a
  new package identity with no hash continuity. Pinned by annotated tag,
  verified before pinning: `refs/tags/v0.12.0` → tag object `716fcb45…` →
  commit `0a0dbed878…`.

  **Wire defaults changed, but not in 0.12.0 — in 0.11.0, which this jump
  crosses.** CUBIC congestion control, packet pacing and HyStart++ are now
  default-on; 0.12.0 keeps those and adds BBRv3 strictly opt-in. Each flip has
  a one-line lever for attributing a behavioural change:
  `congestion_control = .new_reno`, `enable_pacing = false`,
  `enable_hystart = false`.

  Scope of what our gates actually prove here, since the obvious reading
  overstates it: `bench-check` passes 19/19, but it exercises **TCP only**
  (`bench/rpc_round_trip.zig` uses `rpc.transport.tcp.Connection`, and no
  benchmark references QUIC), so it is silent on the new congestion defaults.
  The real evidence is the QUIC suites — `test-rpc-quic` and the four-root
  evidence gate at 64 tests, zero skips, including receive-timeout cases that
  pacing would plausibly disturb — plus the full suite against the QUIC library
  root. We have no QUIC *throughput* benchmark, so the defaults are unmeasured
  rather than measured-and-unchanged.

  Upstream API breakage in this range does not reach us: `initClient` /
  `initServer` / `bind()` were removed from the raw sans-IO `Connection`, but
  every one of our 36 references is a borrowed `*quic_zig.Connection`
  parameter. Our only entry points are `Client.connect` and `Server.init`,
  whose wrapper APIs are unchanged.

- **The toolchain moved to Zig `0.17.0-dev.1683+5ceec001b`, and one frozen
  declaration changed with it.** `.minimum_zig_version` moves to
  `0.17.0-dev.1683`; consumers on an older dev build must upgrade. One std API
  break reached the source: `netClose` now takes `[]const Io.net.Socket`
  instead of raw handles, so `tcp.runtime.closeFd` and
  `stream_transport.ioClose` construct a `Socket` (the close path never reads
  `address`).

  The frozen-surface consequence, reviewed rather than regenerated blindly:

      tcp.runtime.Listener.init: error set gains AccessDenied

  That is inherited from std's `listen`, not invented here, and it is the
  complete frozen diff. Narrowing it back would mean swallowing a genuine
  bind-permission failure on privileged ports, so it is accepted and recorded.
  **Consumers switching exhaustively on `Listener.init`'s error set must add a
  case.** Cold-cache `zig build test` is 1862/1862, bit-identical to the
  pre-upgrade cold baseline.

  Operational note for anyone pinning a Zig dev build: `ziglang.org/builds/`
  garbage-collects them. Measured at the time of writing, the current pin and
  the previous one (`dev.1509`) still resolve while `dev.1252` returns 404 —
  retention is finite but deeper than one bump. This repo therefore keeps
  exactly one version specifier, in `mise.toml`; the nightly fuzz lane's former
  private `dev.1252` pin would be un-installable today had it survived.

- **`quic-zig` is pinned to the `v0.10.1` tag instead of a raw commit.**
  Dependency pins are now immutable, auditable release points. The tag
  contains exactly the previously pinned tree plus one release commit that
  declares the version (no source change), so the Windows socket-link and
  BoringSSL fixes it carries are the same ones already under test.

- **`rpc.peer` decomposed: `peer/mod.zig` shrank from 14,059 to 5,807 lines
  (-59%) with zero behavior change.** The full P0-P12 ladder extracted every
  major subsystem into comptime-generic sibling modules (the JoinCoordinator
  extraction contract — `Namespace(comptime Peer)` files one directory level
  deep): L3 provision hosting + the canonical drain/teardown procedure
  (`provision/`), the outbound Return send family (`return/`), cap
  refcount/release/frame-send, the sendCall family + inbound call path
  (`call/`), L4 join accept/completion + cross-peer relay (`join/`), L3
  origination + inbound Provide/Accept/Join arms (`provide/`), the
  cross-peer proxy + automatic third-party routes (`third_party/`),
  promise-export resolution + inbound Resolve, persistence hooks, question
  allocation, the 26 Peer-parameterized context/record structs
  (`peer_context_types.zig`), and — moved as one unit because its teardown
  order is load-bearing — the deinit/shutdown/cancel/deadline lifecycle
  (`peer_lifecycle.zig`). Every frozen Stable declaration
  (`docs/api-snapshot.txt`, 1548 lines) stayed byte-identical throughout;
  cold-cache `zig build test` totals are bit-equal before and after the
  ladder (1862/1862). Consumers of Experimental internals will see many
  previously-private `Peer` helpers now `pub` (they back the extracted
  namespaces); the experimental snapshots track all of it.

- **`build.zig` is now a 14-line driver; the build graph lives in
  `build/build_impl.zig`.** Step names, registration order, and option
  handling are unchanged (`zig build -l`, with and without `-Dquic=true`, is
  byte-identical). `build/` joined the `build.zig.zon` `.paths` whitelist and
  `package-preflight`'s REQUIRED package roots — without it a consumer's
  `zig build` cannot parse the graph — and the docs-smoke documented-step
  check now scans the real registration site.

- **The `.closing` observer event now always fires on the connection's
  owner/run-loop thread (Experimental events surface).** Cross-thread
  `requestClose()` — TCP and QUIC, including QUIC server sessions — no longer
  invokes the observer on the requesting thread; the run loop emits the
  deferred `.closing` when it observes the request, immediately before the
  terminal `.close`/`.closed` pair. Owner-thread `close()` keeps its
  synchronous emit, and the event fires at most once per connection either
  way. Timing consequence: a deferred `.closing` needs a running loop to
  observe the request — if the loop never runs, or has already exited, the
  event is dropped. `events.Observer` now documents the full observer
  threading contract, and `QuicServer.deinit`/`ServerSession.wake` document
  the teardown contract for external waker threads (quiesce before
  deinit/reap).

### Fixed

- **Explicitly initialized zero-sized structs are no longer encoded as null.**
  Root, nested, AnyPointer, and segment-targeted construction now use Cap'n
  Proto's reference-compatible offset -1 struct pointer when both the data and
  pointer sections are empty. Generated `hasXxx()` therefore distinguishes an
  initialized empty struct from an absent field.

- **Windows now runs the native `capnp`-driven serialization and codegen
  suites instead of carrying a partial-coverage exception.** The upstream
  prebuilt archive supplies `capnp.exe` but omits the standard schemas, so a
  shared test helper injects `-Ivendor/ext/capnproto/c++/src` for `compile`,
  `convert`, and `eval` on every platform. CI downloads the checksum-pinned
  archive, verifies the tool before tests, and then runs the ordinary suite;
  missing-tool skips can no longer make the Windows lane look green. The
  platform matrix now records codegen as full on Linux, macOS, and Windows.

- **Windows TCP soak and stream-transport evidence can no longer succeed by
  doing no work.** Soak now requires positive session, call, chaos-close, and
  applicable deadline-cancellation counters. The thirteen portable
  stream-transport skips are removed; the sole retained exception is the
  documented `TCP_NODELAY`-dependent timing case blocked by std's AFD socket
  API on Windows.

- **`rpc.vat` was missing from the core RPC surface, and the guard that should
  have caught it was too coarse.** `src/rpc/mod_core.zig` — the surface behind
  `capnpc-zig-core` and the wasm build — never exported `vat`, so
  `rpc.vat.*` (provision index, host, join/vat networks) was unreachable there
  despite nothing under `src/rpc/vat/` importing a transport, socket or thread.

  The library-root parity guards added for `canonical` exempted `rpc` wholesale,
  which was the wrong granularity: the rpc surface's VALUE legitimately differs
  per root (`mod_core` swaps in a stubbed transport, `mod_quic` a real one) but
  its NAME SET must not. Both roots now compare rpc submodule names too.
  Ablation-proven on each: removing a submodule stops the build naming it, with
  the check compiled in isolation so no ordinary test can mask the guard.

  This was the third module-root divergence in two days, and the first two were
  only found by a consumer build against a published tag.

### Added

- **The wake doors are instrumented, so a stuck lock names itself.** Two hangs
  this cycle each cost hours for one reason: a hang carries no information. A
  failing test names itself; a spinning thread names nothing, so the failure
  reaches CI as a job that prints its last line and dies at the step cap with
  no test named and no stack.

  `rpc.transport.wake_lock.Lock` replaces the bare
  `while (!tryLock()) spinLoopHint()` that both transports had written
  separately. Same fast path; in Debug (or release with the owner's
  `runtime_thread_checks`) it records the holder and bounds the spin, panicking
  with site, holder and waiter. It is bounded by iterations rather than a
  deadline because no portable monotonic clock is reachable from there —
  `std.time` carries only constants and `std.Io.Timestamp` needs the `Io` this
  lock deliberately lacks.

  Alongside it: `CAPNP_ZIG_STRESS_MULTIPLIER` scales the storm suites'
  repetitions (thread counts stay comptime — they size arrays) so a race can be
  soaked without slowing the default build; and `tools/stall_watchdog.sh`, wired
  into the four jobs that run test binaries, dumps every live test binary's
  threads, backtraces and embedded test names on output silence *before* the cap
  kills the step. The lock diagnostics found the TCP deadlock above on their
  first Linux run.

- **A ThreadSanitizer lane.** Nothing exercised the runtime under a race
  detector, while recent work added threaded code (the Windows QUIC receive
  bridge, the TCP reader bridge, `WorkerPool`) and the wake doors are
  explicitly any-thread. `build.zig` gains an instrumented module
  (`sanitize_thread` + `link_libc` — without libc `std.Thread` uses raw
  `clone()`, which TSan cannot see) and three steps: `test-tsan` / `soak-tsan`
  (which hard-fail off-Linux rather than pass vacuously) and `check-tsan`
  (compile-only, usable from any host via `-Dtarget=*-linux-gnu`). The lane is
  Linux-only by construction: a probe confirmed `sanitize_thread` reports a
  seeded race on Linux but that libtsan SIGSEGVs at startup on darwin — re-
  verified, not assumed, at the current toolchain.

- **Structure-aware fuzzing of the L3/L4 and QUIC-framer surfaces.** The single
  structured peer target emitted only Call and Return, leaving the ~4,900-line
  three-party/Join surface (Provide, Accept, Join, ThirdPartyAnswer, Resolve,
  Disembargo, Release, Finish, Bootstrap, Abort) reachable only through random
  bytes that essentially never decode, and the QUIC framers coverage-fuzzed not
  at all. `fuzzPeerL3Frame` seeds the peer with the state those handlers gate
  on — a live export, imports, an attached provision index with small limits so
  hostile embargo/park bytes stay bounded, a loopback Join network, and a
  well-formed Provide preamble with a reachability self-check so the target
  cannot silently rot into "bytes that never provide" — then feeds a weighted
  mix of all twelve builders. `fuzzQuicLengthFramer` and
  `fuzzQuicNativeControlFramer` are std-only and re-exported under both roots,
  so they fuzz under the default `test-fuzz` with no `-Dquic` and no extra
  build wiring. This is what found the self-loopback frame leak above.

- **A persistence restore / sturdy-ref decode fuzz target.** Sturdy-ref bytes
  are attacker-controlled by definition — a remote peer names whatever it likes
  in `restore` — yet the restore path (payload decode, the restorer hook, and
  `RestoreOutcome` handling for unknown/existing/host) was reachable only
  through random bytes that essentially never carry the restorer interface id.
  The target seeds a bootstrap export plus a restorer whose outcome is driven
  off the fuzzed ref, then feeds restore Calls with well-formed Data payloads
  of fuzzed length and content plus two malformed shapes (null `sturdyRef`
  pointer, absent content). Property: no crash, hang, or leak, and full state
  drain on deinit.

- **QUIC thread-affinity checks now match TCP's, and both contracts are
  documented.** QUIC `Connection` gains a `runtime_thread_checks` field so its
  `assertThreadAffinity` runs Debug-always / release-opt-in like TCP's
  (previously the whole check compiled out of release with no opt-in), plus the
  `adoptOwnerThread` method it lacked. QUIC `Server` gains a read-side
  `assertLoopThread` guarding `sessionCount`/`sessionAt`/`sessionById`.

  Two asymmetries with TCP were flagged in review; both are resolved as
  deliberate rather than by copying TCP. `Connection.sendFrame` stays
  un-checked because it enqueues into a mutex-guarded outbound queue the loop
  flushes, so it is thread-safe from any thread — TCP made the opposite choice
  (owner-thread-only send, cross-thread work via `wake()`). And `deinit()`'s
  callback-deferred branch intentionally does *not* assert affinity: it can be
  re-entered from a callback on the run thread, which need not be the owner;
  the real teardown still asserts. The sanctioned pattern for running a
  connection off its constructing thread remains adopt-before-run, now covered
  by a stress test.

- **Schema parsing and generated code now preserve pointer-kind and brand
  fidelity without changing erased APIs.** The frozen `schema.Type` union keeps
  its existing tags and payloads. Additive `TypeMetadata` / `TypeExpression`,
  node and method parameters, brand scopes/bindings, superclass/method and
  annotation-use brands, and AnyPointer sub-kinds retain the full
  `CodeGeneratorRequest` expression beside it. Generated annotation constants
  remain the legacy id/value projection; inspect the parsed request when an
  annotation's lexical brand matters.

  Structs and groups with constrained `AnyStruct`, `AnyList`, or bare
  `Capability` slots gain Reader/Builder `pointerKinds()` views in full and
  compact profiles. Finite concrete branded data-struct fields gain parallel
  `brands()` views covering arbitrary-depth lists, enum/Text/Data/struct/
  interface terminals, concretely branded nested structs, and inherited
  lexical bindings, including generic struct applications as list terminals
  and cross-file imported applications/terminals. They preserve groups,
  unions, recursively materialized Builder defaults, enum forwarding, null
  structs/lists, and near/far reopening. All historical erased and `raw()`
  accessors remain.

  One allocation-free resolver is shared by validation and generation. It
  composes `.bind` / `.inherit`, validates lexical scope, exact arity and
  parameter indexes, and enforces a 64-level cycle/depth bound. Additive Stable
  `validateMessageWithBrand`, `canonicalizeMessageWithBrand`, and
  `canonicalizeMessageFlatWithBrand` entry points apply a concrete root brand;
  existing entry points use an empty root brand while honoring concrete nested
  metadata. Valid unbound values remain erased, while malformed brand graphs
  and scalar generic bindings return `InvalidSchema`.

  This remains finite code generation, not general generic specialization:
  generic RPC clients and implicit generic methods stay erased. A new
  `CodegenBudget.max_brand_specializations = 4096` is configurable through
  `max-codegen-brand-specializations=` or
  `CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS`. Full and compact generation
  of vendored `capnp/test.capnp` now compile recursively, closing the bounded
  void-setter and nested `WhichTag`/`Reader`/`Builder` qualification failures.
  Cap'n Proto's layout-A double-far empty struct/list representation remains
  inherently ambiguous, while non-empty list-as-AnyStruct is rejected; wire
  schemas and schema-free `canonical.*` are unchanged. Current focused evidence
  is 83/83 codegen, 17/17 executable schema fidelity (including the dedicated
  branching-budget proof), 32/32 validation, and 150/150 message tests in
  Debug, with the focused fidelity/codegen matrix also green in ReleaseSafe.
  The hardening scan reports 63 reviewed findings across 148 checked files.

- **Windows QUIC now has a bounded native receive path and executable,
  transport-truthful evidence.** Published boringssl-zig commit `292c70a`
  links `ws2_32` with package-config lookup disabled; published quic-zig commit
  `e00d449` pins that archive, and capnp-zig pins the resulting quic-zig
  archive. Native shells and Git Bash therefore avoid the prior
  `pkg-config.BAT` failure.

  QUIC's Windows UDP path keeps exactly one blocking receive in an
  `io.concurrent` future. The owner thread alone advances QUIC and invokes
  callbacks; an `Io.Condition` wakes it for completion, timer, explicit wake,
  or close. Timer ticks retain the valid receive, while teardown cancels and
  reaps it exactly once before socket/callback destruction. Buffer-retention,
  poll, wake, timer, completion, truncation, start-failure, cancellation, and
  repeated-close tests cover the bridge, and a compile-time tripwire prevents
  Windows QUIC from calling `receiveTimeout()`.

  A native Zig evidence scanner rejects `SkipZigTest` and requires exactly
  four runnable roots with floors 26 + 1 + 17 + 8 = 52; there is no output
  parser, Bash dependency, or CI-only package. Eight of the current 61 tests
  are real `Peer` flows: verified-CA baseline; native
  Bootstrap/Call/Return/Finish; returned-cap pipelining; native large frames;
  graceful and abrupt close; two-session fanout; and fanout close isolation.
  Fanout sessions live at stable heap addresses before `Peer` attachment.
  macOS passes 61/61 in Debug and ReleaseSafe. Windows full-tree test
  cross-compilation passes 113/113, but native runtime acceptance remains a
  hosted gate after capnp-zig is pushed; no Windows parity claim is made yet.

- **Experimental L4 Join state is now quota-bounded, leased, and observable
  without exposing addressing data.** `PeerLimits` adds a 64-part per-Join
  ceiling and a 4096-record aggregate limit over buckets, parts, relays,
  hosted provisions, result answers, and direct Accepts. One origin-owned
  `HostedJoin` owns the provision, captured network, byte charge, deadline,
  result counts, and reciprocal Accept-host backlink. TCP connect/serve and
  `WorkerPool` apply a secure 30-second lease by default; raw peers remain
  opt-in and explicit null opts out.

  The first part stamps a partial bucket and later parts cannot extend it;
  relays and hosted Accept phases receive fresh local-clock deadlines. A cached
  next deadline drives sweeps before frame decode, before Accept lookup, and
  from deadline maintenance, with `Peer.sweepExpiredJoins()` available to
  manual pumps. `attachJoinNetwork()` / `detachJoinNetwork()` are fallible
  while dependent state exists, with identical reattachment a no-op.
  `PeerStats`, resource events, and `TimeoutKind.join` expose only aggregate
  record/part/provision-byte counts and an inbound answer ID. Quota and timeout
  Returns say only `"join unavailable"`.

  Cleanup detaches maps, counters, and backlinks before sends, observers,
  network callbacks, or user close callbacks. A committed direct pickup
  survives result-path transport close when a distinct Accept host remains
  live, and is cancelled by Accept-host close, TTL, explicit cleanup, or owner
  teardown; transport detach remains non-terminal. The focused L4 gate passes
  79/79 in Debug, ReleaseSafe, and ReleaseFast, and the full peer suite passes
  499/499. The nine-case Zig TCP e2e first exhausts a short lease/small quota
  attacker and then completes JoinResult→Accept→call; it runs in the Linux,
  macOS, and Windows Test matrix. This remains a Zig-only Experimental pilot,
  not a production dialer, address/authentication policy, stable wire
  convention, or cross-implementation L4 claim.

- **Experimental redirected RPC results can now be routed automatically
  through an attached `VatNetwork`.** The additive
  `ThirdPartyResultPolicy.vat_network` mode resolves an inbound
  `sendResultsTo.thirdParty` contact, registers a synthetic callee-range answer
  on the introduced peer, sends `ThirdPartyAnswer`, and lets the application
  handler use its ordinary results/exception Return API. Capability-bearing
  payloads are remapped through pinned cross-peer proxies; calls pipelined on
  the synthetic answer wait for and replay from its result, and direct calls on
  a returned proxy reach the source until Release. The runtime commits the
  recipient Return before the original caller receives
  `resultsSentElsewhere`; Finish then drains the synthetic answer, including
  the early/reentrant case.

  `.reject` remains the default, while `.application` keeps the manual
  `sendReturnResultsSentElsewhere()` contract.
  The network is borrowed and app-supplied; this does not add a production
  dialer, authentication/identity policy, L4 Join integration, or a Stable API.
  Coverage is Zig↔Zig only and proves one handler invocation, early recipient
  Finish, missing-network refusal, source/target deinit reentered during
  synchronous delivery, source/target transport close without deinit, and
  route drain. ThirdPartyAnswer-send rollback, failed target-result fallback to
  one exception, a proof-backed trailing target-send commit, and a
  post-delivery source-marker error without a second Return cover the send
  boundaries. Pipelined children also drain when the recipient Finishes first,
  including legal question ID zero and retained parameter caps. An
  allocation-index sweep drains every partial
  route/synthetic-answer/proxy/pin state, and a separate case proves distinct
  network/source/target allocator ownership. The focused binary passes 22/22
  with 19 automatic cases; the full peer suite passes 475/475 and hardening is
  green. The
  reference lanes do not accept inbound redirected-result calls.

- **Experimental caller-controlled RPC answer lifetimes and Level-3 transfer.**
  `rpc.peer.CallOptions` adds `.result_lifetime = .automatic | .retained` to
  additive `sendCall*WithOptions` methods. Generated `Client`,
  `PipelinedClient`, and `callXxxPipelined` methods gain matching `WithOptions`
  forms while existing calls remain automatic; streaming fire-and-forget calls
  are unchanged. A retained terminal Return withholds Finish until
  `finishRetainedQuestion(question_id, release_result_caps)` succeeds. Finish
  send failure is retryable, callbacks stay at-most-once, and post-visibility
  callback/OOM errors are reported non-fatally so synchronous callers still
  receive the question id. Synchronous Return is registration-safe,
  `noFinishNeeded` retires locally, and cancellation/close
  drain ownership without double cleanup.

  Retained questions have an independent default limit of 1024, caller-owned
  and transferred `PeerStats` gauges, and redacted `retained_questions`
  pressure/rejection events. `sendProvideFromRetainedAnswer` and
  `resolvePromiseExportToThirdPartyFromRetainedAnswer` transfer a completed
  answer plus promised-answer ops into the vine/Provide lifecycle, preserving
  the exact target for fallback forwarding and direct pickup. Failure before
  the Provide commit restores caller ownership; after that potentially-delivered
  boundary, the coupling or its cleanup owns both Finishes and retries failed
  protocol-owned sends through deadline maintenance. The new
  retained-target handoff proof is Zig↔Zig; no additional C++ scenario is
  claimed.

- **Experimental L3 parked-Accept admission is now fair, time-bounded, and
  observable.** `ProvisionIndexLimits` adds per-peer defaults of 64 entries and
  16 KiB, enforced before the larger vat-wide ceilings. Each Accept is charged
  for its normalized recipient token plus embargo bytes even when tokens are
  shared. Adoption, Finish, expiry, rollback, transport close, and teardown use
  centralized exactly-once refunds; terminal close detaches only the closing
  peer's holder records, preserving active provider provisions for
  disconnect-after-Provide pickup.

  Raw `ProvisionIndex` expiry remains opt-in, while `Vat` now defaults to a
  30-second TTL and requires either a custom clock or its value-stored
  `Options.io` fallback (`error.ParkClockUnavailable` otherwise). A cached next
  deadline makes the common inbound-frame check O(1); due sweeps are
  reentrancy-safe, run from every inbound path and deadline maintenance, and
  are also exposed by `sweepExpiredParkedAccepts()`. `ProvisionIndex.stats()` /
  `Vat.stats()`, per-peer park gauges, redacted park pressure/rejection events,
  and `TimeoutKind.parked_accept` add operability without exposing tokens or
  frame contents. Manual frame pumps can call idempotent
  `HostPeer.notifyTransportClosed()` on EOF/reset.

  `zig build test-rpc-l3` groups the seven L3 suites; VatC now participates in
  resource-budget, OOM, ReleaseSafe, and ReleaseFast hardening. The vendored
  C++→Zig VatC lane has nine scenarios, adding a one-entry attacker fairness
  case that refuses a second park while a sibling completes a legitimate
  reverse-direction handoff, then expires the first from ordinary traffic.

- **Generated schema-evolution APIs can preserve values unknown to an older
  schema without weakening typed access.** Structs and groups with enum fields
  now expose `Reader.EnumOrdinals` / `Builder.EnumOrdinals` through
  `enumOrdinals()`; enum lists add `getOrdinal()` / `setOrdinal()`; and union
  Readers add infallible `whichOrdinal()`. These APIs use logical `u16` values
  and apply enum-default XOR. Existing exhaustive enums, typed getters,
  `which()`, and strict schema-aware validation still reject unknown ordinals.

  Generated Readers and Builders also add structural `hasXxx()` checks for
  Text, Data, struct, list, AnyPointer, and interface fields. Null and
  old-layout-missing slots are absent even with pointer defaults, explicitly
  encoded empty values are present, and inactive union arms are absent before
  shared pointer storage is inspected. Stable `StructBuilder.isPointerNull()`
  and `readUnionDiscriminant()` provide the matching low-level behavior.

  A checked-in V1/V2 fixture with shared schema/type IDs exercises unknown enum
  forwarding, enum defaults, new union arms, pointer presence, and old-layout
  reads as ordinary Zig tests on every OS. `just check-generated` now also
  covers those bindings plus addressbook, ping-pong, kvstore, and WASM output.

- **Generated nested-list fields now have recursive typed views without
  removing their raw API.** In full and compact profiles, structs and groups
  containing `List(List(T))` expose `Reader.nestedLists()` and
  `Builder.nestedLists()`. The views recurse through deeper lists, support
  scalar, Text, Data, enum, struct, and interface/capability terminals, with
  AnyPointer retaining the same raw pointer-list terminal as flat lists. They
  carry pointer defaults and union guards, preserve enum ordinal
  forwarding, distinguish null inner pointers from explicit empty lists, and
  support segment-targeted initialization. Every recursive wrapper exposes
  `raw()`, and existing `PointerListReader` / `PointerListBuilder` field
  accessors are unchanged. Unknown struct layouts and unresolved enum IDs keep
  explicit raw/ordinal fallbacks.

- **A hermetic manifest-filtered package preflight now exercises real consumer
  shape before any publishing workflow.** `zig build package-preflight` / `just
  package-preflight` snapshots the checkout, applies `build.zig.zon` `.paths`,
  asserts the five allowed roots, archives and re-fetches that filtered tree,
  and builds/runs default, core, and opt-in QUIC consumers in Debug and
  ReleaseSafe with isolated caches. It also proves lazy QUIC fetch behavior,
  installs and runs the packaged compiler plugin against a checked schema,
  compares normalized generated output, and asserts the checkout is unchanged.
  The gate runs in the three-platform test matrix and in the local preflight.

- **The QUIC-enabled API surface is now snapshotted.** `check-api` runs without
  `-Dquic=true`, so it saw `rpc.transport.quic` as the disabled stub and the
  real `ServerOptions` fields not at all: quic-zig v0.10.0's breaking
  `Server.Config` rename moved ZERO snapshot lines, in either file. A new
  `docs/api-snapshot-experimental-quic.txt` (2464 declarations, vs 1980
  without QUIC) records that surface, regenerated by
  `zig build -Dquic=true api-snapshot-quic` and checked by
  `zig build -Dquic=true check-api-quic`, both wired into `ci-quic` and CI.

  Two distinct properties, worth separating because only one is a gate:
    * GATED — the frozen `docs/api-snapshot.txt` must be **byte-identical**
      whether or not QUIC is enabled. An Experimental transport that alters the
      frozen contract is a bug by definition, and that is now asserted rather
      than assumed. (It holds today: 1313 stable declarations either way.)
    * TRACKED, not gated — the QUIC experimental surface, matching how every
      other Experimental surface is treated. A field rename now appears as a
      reviewable diff in a committed file, verified by ablation; previously it
      produced no artifact anywhere. It gets no staleness check, deliberately:
      the experimental surface is NOT byte-stable across platforms
      (`std.Thread.Id` is `u64` on macOS and `u32` on Linux, so every thread-id
      field renders differently), which is exactly why
      `docs/api-snapshot-experimental.txt` has never had one either. Only the
      Stable file can be a contract. An earlier revision of this entry added
      such a check and CI rejected it on the first run.

### Added

- **Cross-impl coverage for the park-then-adopt rendezvous** — a new
  `park-adopt` scenario in the `e2e-l3-vatc` lane. rpc.h:483-492 makes the
  third-party rendezvous order-independent: "The two calls can happen in any
  order; `completeThirdParty()` will wait for a corresponding
  `awaitThirdParty()` if it hasn't happened already." `unknown-token` proved the
  waiting half (an Accept naming no provision parks); this proves the other —
  that the park is ADOPTED and served once the Provide naming its token arrives.

  Driven by arithmetic rather than timing, which is what made it cheap: the
  driver's completion-token rewrite is now an explicit mode, and `park-adopt`
  selects `next_provision` (`+1`), which is exactly the token the driver's NEXT
  introduction registers. The Accept therefore necessarily reaches the host
  before its Provide. No delay hook is involved, so the ordering is
  deterministic rather than raced — this cell was previously deferred for
  wanting exactly such a hook.

  Both sides assert, and the pair is the point: parking without serving is
  `unknown-token`, and serving without parking would prove nothing about
  ordering. The adopted capability is then held to the same lifecycle as a
  normally-served one — the driver runs the `happy` release ceremony and the
  host requires the proxy export to die and all transient state to drain.

  Ablation-proven and discriminating: removing the Provide's adoption drain
  (`drainAdoptedParkedAccepts`) leaves the PARK assertions green on both sides
  while the driver's call times out and the host reports no serve, no Carol
  call, the park still outstanding, and no drain.

### Fixed

- **Every QUIC gate could pass while compiling zero QUIC code.** `build.zig`
  resolved quic-zig with the deprecated
  `b.lazyDependency("quic_zig", ...) orelse break :blk null` and `build`
  returned `void`. `std.Build.runPackageScript` only fetches unresolved lazy
  dependencies and re-runs the configure phase when `build` returns an ERROR —
  a `build` that returns normally proceeds straight to the make phase with
  whatever graph it managed to build. So an unresolved quic-zig produced a null
  module, `addQuicImport` no-opped, all four QUIC test steps were omitted, and
  `-Dquic=true check`, `test-rpc-quic` and `test` each exited 0 having built
  nothing. `just ci-quic`, the CI QUIC job and `release-preflight` were vacuous
  together, and the only visible difference was the step count: `1/1 steps
  succeeded` against a healthy `13/13`.

  `build` now returns `!void` and uses `try b.dependencyLazy(...)`, the
  documented idiom — the error propagates, the toolchain fetches and retries,
  and "QUIC enabled but no QUIC steps" becomes unrepresentable rather than
  merely unlikely. Both resolution sites (debug and the ReleaseSafe lane) are
  converted.

  Belt-and-braces for the day someone reintroduces an `orelse null`: a new
  `just check-quic-not-noop` asserts the lane's step count, wired into
  `ci-quic` and run as its own CI step. Ablation-proven — restoring the old
  swallowing shape leaves `zig build -Dquic=true test-rpc-quic` exiting 0 while
  the assertion fails naming the cause.

  This also corrects the record: commit 6dd903d attributed the false-green
  after the quic-zig bump to a stale `.zig-cache`. That was never established;
  this is the mechanism, and it was a standing property of the build rather
  than a cache artifact.

## [0.9.0] - 2026-07-31

Reachability and defaults. v0.8.0 shipped the schema-free canonicalizer
exported from only one of the library's three roots, so neither a
serialization-only consumer (`capnpc-zig-core`) nor a QUIC-enabled one
(`-Dquic=true`) could reach it — each root compiles fine alone, which is why
nothing caught it. Both are fixed, and both are now held by comptime parity
guards plus a CI job that builds the whole suite against the QUIC root.

The quic-zig bump to v0.10.0 brings its `Server.Config` normalization through
to capnp-zig's own options, and with it a correction that matters on its own:
capnp-zig's QUIC server ran with the **Initial-flood DoS mitigation disabled by
default**, because the knob's `null` default meant "explicitly off" rather than
"unset". That is exactly the confusion the new three-state `RateLimit` exists to
make unspellable.

### Breaking

- **quic-zig bumped v0.7.0 -> v0.10.0, and the QUIC server options adopt its
  three-state `RateLimit` / `EarlyData` types.** (Experimental tier.) quic-zig
  0.10.0 normalized `Server.Config` and froze the names to 1.0; capnp-zig's
  `ServerOptions` mirrored the old shape 1:1, so the rename lands here too.

  | old `ServerOptions` field | new field |
  | --- | --- |
  | `max_initials_per_source_per_window: ?u32` | `initial_source_rate_limit: RateLimit` |
  | `max_vn_per_source_per_window: ?u32` | `vn_source_rate_limit: RateLimit` |
  | `max_log_events_per_source_per_window: ?u32` | `log_source_rate_limit: RateLimit` |
  | `max_datagrams_per_window: ?u32` | `listener_datagram_rate_limit: RateLimit` |
  | `max_bytes_per_window: ?u64` | `listener_byte_rate_limit: RateLimit` |
  | `max_bytes_per_source_per_second: ?u64` | `source_byte_rate_limit: RateLimit` |
  | `enable_0rtt: bool` + `early_data_anti_replay: ?*T` | `early_data: EarlyData` |

  **Migration:** `null` -> `.disabled`; `n` -> `.{ .limit = n }`; omit the field
  for `.default`. `.enable_0rtt = true` + a tracker ->
  `.early_data = .{ .with_anti_replay = &t }`; `enable_0rtt = true` alone ->
  `.early_data = .without_replay_protection`; omit to keep 0-RTT off. Read an
  effective cap with `.resolve(default_cap)` (null = limiter off). `RateLimit`
  and `EarlyData` are re-exported from `rpc.transport.quic`, so callers need no
  direct quic-zig import. Renamed constants:
  `default_quic_max_vn_per_source_per_window` ->
  `default_quic_vn_source_rate_cap`,
  `default_quic_max_log_events_per_source_per_window` ->
  `default_quic_log_source_rate_cap`, plus a new
  `default_quic_initial_source_rate_cap`.

### Fixed

- **`capnpc-zig-core` now exports `canonical`.** v0.8.0 added the schema-free
  canonicalizer to `src/lib.zig` only, so the module the docs tell a
  serialization-only consumer to import could not reach it — despite
  `canonical.zig` importing nothing but `message.zig`. Caught by a clean-room
  consumer build against the published tag, *after* it was cut: `zig fetch`
  compiles nothing, so the release validation step could not see it.

  Two guards so it cannot recur. `src/lib_core.zig` now carries a comptime
  parity test that fails the build, naming the declaration, if `lib.zig`
  exports a non-RPC module it does not (`rpc` and `io_backend` are exempt by
  design — the two roots expose deliberately different RPC surfaces). And
  `lib_core.zig`'s test block is now compiled at all: a new `core_tests` target
  roots there, because `lib_tests` roots at `src/lib.zig` and nothing in
  `lib_core.zig` had ever been compiled as a test. A deliberately failing probe
  produced no output before that target existed, and fails as expected after.
  RELEASING.md's post-tag step now requires a clean-room *build* against both
  published modules, not just a fetch.



- **`canonical` was missing from the QUIC library root.** `-Dquic=true` selects
  `src/lib_quic.zig`, which never gained the export v0.8.0 added to
  `src/lib.zig` — so a QUIC-enabled consumer could not reach the canonicalizer
  at all. Found by the parity guard added for `lib_core.zig`, once the full
  suite was finally built against the QUIC root. `lib_quic.zig` now carries the
  same comptime guard, and CI runs `-Dquic=true test` (the whole suite, not just
  the transport lanes) so a third root cannot silently diverge again: the
  targeted `test-rpc-quic` lane never compiled the serialization suites against
  the root a QUIC consumer actually gets.

- **The QUIC server's Initial-flood DoS mitigation was off by default.**
  `max_initials_per_source_per_window` defaulted to `null`, and under quic-zig
  >= 0.3.0 `null` on that knob means *explicitly disable the limiter* — not
  "unset". capnp-zig mirrored quic-zig's recommended caps for the VN (8) and
  log-event (16) limiters but left this one at `null`, so every server that did
  not opt in accepted unbounded Initial packets per source address. quic-zig's
  0.10.0 notes describe a downstream consumer shipping exactly this
  misconfiguration "with no compile error and no failing test"; that consumer
  was capnp-zig. `initial_source_rate_limit` now defaults to `.default`, which
  applies the recommended 32-per-window cap. Servers that genuinely want it off
  must now say `.disabled`.

## [0.8.0] - 2026-07-31

Two-party interop and the frozen contract, hardened. The headline is a spec
violation on the *Stable* surface that no lane could see: every Return that
answered a call with param capabilities told the caller to release its exports
twice — fatal against the C++ reference, a silent export destruction Zig-to-Zig.
The freeze gate itself got its last two holes closed (no fallible `anytype`
signature remains on the frozen surface — every frozen error set is now either
concretely pinned or an honest `anyerror`), the list-upgrade rule is now
complete in both directions
including `List(Void)`, and canonicalization gained the spec's actual
schema-free form, byte-identical to `capnp convert binary:canonical` and
appropriate as a signing input.

### Breaking

- **`reader.Reader.readMessage` / `readPackedMessage` take a concrete
  `*std.Io.Reader`; `reader.SliceReader` is removed.** The last two fallible
  `anytype` signatures on the frozen Stable surface (the infallible
  `CapDescriptor.writeSenderHosted`-family keeps `anytype` harmlessly: no
  error set exists to pin). A generic parameter leaves a
  function's error set unpinnable: the snapshot renderer emits an opaque marker
  that is identical no matter what the set contains, so adding, removing or
  renaming an error passed `check-api` unchanged while breaking every
  consumer's `catch |err| switch (err)` — demonstrated live before the change
  (an error rename inside `readMessage`, gate green) and again after (the
  identical rename, gate red naming the drifted line). Both error sets now
  render concretely in `docs/api-snapshot.txt`, and `api-closure` checks the
  signatures it previously had to skip. `SliceReader` existed only to feed the
  duck-typed parameters; use `std.Io.Reader.fixed(bytes)`. Short-body behavior
  is unchanged: truncated framing is still this function's own
  `error.UnexpectedEof`, and a truncated packed stream still surfaces
  `error.EndOfStream`. Frozen Stable declarations: 1320 -> 1313.

  **Migration:** wrap your stream in `*std.Io.Reader` — for a byte slice,
  `var r = std.Io.Reader.fixed(bytes);` then pass `&r`; a socket/file reader
  from `std.Io` passes through unchanged. A custom duck-typed reader must be
  adapted to the `std.Io.Reader` interface. `SliceReader{ .data = bytes }`
  call sites become `std.Io.Reader.fixed(bytes)` one-for-one (that is the
  exact conversion this repo's own tests underwent). Error handling: the sets
  are now concrete and pinned; `error.ReadFailed` replaces whatever your
  custom reader's failure was, and truncation diagnoses are unchanged
  (`UnexpectedEof` framed, `EndOfStream` packed).

### Fixed

- **Inbound param capabilities are no longer released twice.** rpc.capnp on
  `Return.releaseParamCaps`, which DEFAULTS to true: "If true, all capabilities
  that were in the params should be considered released. The sender must not
  send separate `Release` messages for them." The sender there is the callee —
  and capnpc-zig was doing both: it left the flag at its wire default while also
  emitting explicit `Release` frames from the post-dispatch auto-release (and,
  for a handler that kept a param cap the way the generated accessors do, from
  the application's own later `releaseImport`). The caller then retired the same
  export twice. Against the vendored C++ reference that is fatal:
  `capnp/rpc.c++:4263: failed: Tried to release invalid export ID.` followed by
  `RPC connection broken for non-DISCONNECTED reason`. Zig↔Zig it silently
  destroyed the caller's export at ref-count 2 → 0.

  The peer now decides the flag from what the answer actually owes.
  `Peer.returnReleasesParamCaps` reads a per-answer `owes_param_cap_releases`
  bit recorded in `active_inbound_questions` when the Call arrives, computed
  from the params cap table: a `senderHosted` / `senderPromise` /
  `thirdPartyHosted` descriptor is a reference this vat takes and settles
  explicitly, so those Returns carry `releaseParamCaps = false` — exactly what
  the C++ reference emits on every Return it sends ("no version of the C++ RPC
  system has ever" sent true, `rpc.c++`). Every peer-built Return threads it:
  results, exception, `canceled`, `resultsSentElsewhere`,
  `takeFromOtherQuestion`, `awaitFromThirdParty`, and the reflected-loopback
  results. Answers that took no param refs — Bootstrap/Provide/Accept/Join,
  cap-free params, the `sendResultsTo = thirdParty` refusal issued before any
  import is noted — keep the schema default `true`, where the flag is a no-op.
  Cross-peer relay Returns still compute their own value and override it.

  `HostPeer` had the same defect inverted in its own settle step: it sent wire
  `Release` frames for `releaseParamCaps = true` and stayed silent for `false`.
  A host-built Return frame now settles silently either way (the flag already
  released the caps, or the host claimed retention and owes its own
  `sendReleaseForHost`), while the two peer-built paths — which stamp `false` —
  keep emitting the explicit Releases that are then the only signal.

  Proven cross-implementation. Ablating the fix and re-running
  `just e2e-l3-vatc` reproduces the C++ abort verbatim; with the fix the lane is
  green **and** `tests/e2e/zig/l3_vatc_host.zig` no longer needs its
  `setReleaseParamCaps(false)` override — the runtime states the retention. Five
  new frame-capturing tests in
  `tests/rpc/peer/rpc_return_release_param_caps_test.zig` assert the pairing
  (never one half of it) for the ignoring handler, the retaining handler, the
  exception Return, the cap-free call, and a two-peer end-to-end grant/settle;
  ablating `returnReleasesParamCaps` to the old constant turns four of them red,
  the end-to-end one with the caller's export already destroyed.

- **The inverse list-upgrade rule now reaches the nested and type-erased list
  readers.** v0.7.0 taught `StructReader.read*List` to decode a correctly
  encoded struct list (element size C = 7) as the `List(UInt32)` / `List(Text)`
  an older schema declares, but stopped at a struct's own fields; one level down
  (`PointerListReader.getU8/16/32/64List`, `getI*`, `getF*`, `getPointerList` —
  the accessors that reach an element of a `List(List(T))`) and through
  `AnyPointerReader.getPointerList`, the same wire bytes still failed with
  `error.InvalidPointer`. All three surfaces now share one resolver
  (`src/serialization/message/element_list.zig`), which is the point: a
  downgrade implemented per-surface is how one of them ends up silently wrong.

  Preconditions are unchanged and now apply identically everywhere, straight
  from the C++ reference (`layout.c++`, `readListPointer`): a primitive list
  needs a non-empty data section, a pointer list needs at least one pointer, a
  struct list is never readable as `List(Bool)`, and Text and Data still require
  `ElementSize::BYTE`. The pointer arm adds the element's data-section length to
  the base so it reads the element's *pointer* section — the `+ dataSize`
  go-capnp's `TextList.At` omits while its own `PointerList.At` applies it, so
  the C++ reference is again the only cross-check for that arm.

  Both silent-wrong-data traps were live on the new paths and are closed by
  routing through the inline-composite resolution rather than
  `resolveListPointer`: for C = 7 the latter reports the pointer's D field (a
  WORD count, not an element count) and a content offset addressing the TAG
  word. Ablation, on a fixture with two data words per element so the two counts
  differ: nested `getU32List` reports `expected 3, found 6` and reads the tag as
  data (`expected 100, found 12`). The stride ablation (natural element width
  instead of the whole struct) turns 9 tests red, including
  `expected 572662306, found 0` on the nested reader's second element. The
  precondition ablation turns a pointer-only struct list into readable `u8` data
  on both the struct-field and nested surfaces.

  **No `docs/api-snapshot.txt` drift.** The shared resolver is an internal
  module imported directly, not a new `pub` declaration and not a seventh
  `define()` parameter — the latter renders inside every generated reader's type
  name and would have rewritten ~80 lines of the frozen surface for an internal
  refactor. `error.InvalidInlineCompositePointer` stays folded into
  `error.InvalidPointer`, and `listContentBytes` is threaded to the resolver by
  each caller so `PointerListReader`'s eleven `get*List` signatures keep their
  existing `anyerror!` rendering rather than silently tightening to a named set.

  Still open, and now the only bullet on this rule in
  `docs/supported-surface.md`: `readVoidList` resolves through the plain list
  pointer, so a struct list read as `List(Void)` reports the word count as its
  element count. C++ accepts `ElementSize::VOID` against an inline-composite
  list. `test-message` 96 → 100.

- **`StructReader.readVoidList` accepts every well-formed list encoding, at
  the honest element count.** The last arm of the list-upgrade rule. A void
  element needs zero data bits and zero pointers, so both of the C++
  reference's element-size checks are vacuous for an expected
  `ElementSize::VOID` (`layout.c++`, `readListPointer`: the ordinary-list
  "at least as large as expected" comparison and the `case ElementSize::VOID:
  break;` struct-list arm). Previously anything but a plain void list was
  rejected with `error.InvalidPointer` — including the struct list a peer
  evolved a `List(Void)` field into. (An earlier docs revision misdescribed
  the symptom as misreporting the word count; the probe shows it rejected.)
  The inline-composite arm reads the element count from the TAG word — the
  pointer's D field is a word count for C = 7, so the plain-path count would
  be silent wrong data. The frozen error set is unchanged: a classification
  failure is swallowed and the plain path reproduces the pre-existing
  diagnosis. Ablation: restoring the strict element-size check turns exactly
  the two new tests red.

- **e2e L3 driver: the unmatched-token corruption could collide with a real
  token.** `unknown-token` corrupted the completion token by `+= 1`, and
  `newToken()` hands out small sequential values — so with two introductions in
  flight the second Provide registers exactly the token the first Accept was
  corrupted to, adopts that parked Accept and SERVES it. One introduction hid
  this; `park-expiry` has two and observed its pipelined call resolving instead
  of failing. Corruption is now a high-bit flip, which no minted token can
  reach.

### Added

- **`canonical` — spec-faithful, schema-FREE canonicalization (Experimental
  top-level module).** `canonical.canonicalize` (framed) /
  `canonical.canonicalizeFlat` (bare segment, byte-identical to
  `capnp convert binary:canonical`) walk the raw pointer graph the way the
  reference implementation's `canonicalize()` does — single segment, preorder
  layout, trailing-zero data-word and trailing-null pointer truncation,
  uniform max-truncated inline-composite element sizes, far pointers
  collapsed, upgraded lists preserved as written, capabilities rejected —
  each rule cited to the vendored `layout.c++`/`message.c++` at its
  enforcement site. `canonical.isCanonical` ports
  `MessageReader::isCanonical`. Unlike the schema-driven
  `schema_validation.canonicalizeMessage` (unchanged, still the home of
  schema-aware equality and `omit_default_pointers`), the schema-free form
  preserves fields the local schema does not know about, making it
  appropriate as a signing input. Differentially tested byte-for-byte
  against the reference CLI (multi-segment collapse, truncation,
  heterogeneous struct lists, text/data/bit/nested lists, null pointers,
  empty structs) plus ports of the reference's `canonicalize-test.c++`
  acceptance suite; idempotence, determinism, full-validation and
  per-allocation OOM invariants; ablation-proven (each canonical rule has a
  test that goes red without it).

- **Cross-impl coverage for the failed-answer directions of the broken-pipeline
  rule** — a new `park-expiry` scenario in the `e2e-l3-vatc` lane (Zig VatC
  host, C++ recipient+introducer driver). The ANSWERED direction already had
  cross-impl teeth in `pipelined-provide`; the failed direction had only unit
  tests, because the `receiverHosted` lift turned the scenario that used to
  refuse into one that succeeds.

  The host now runs with a clock and `park_ttl_ms`, so an Accept whose
  completion token matches no Provide parks and is then evicted by the L9 TTL
  with an exception Return. Three Returns must carry that exception: the
  evicted Accept itself, a call the driver pipelined on it BEFORE it failed
  (queued against the pending answer, settled by the drain), and a call
  arriving AFTER the Return was built (settled from the recorded
  `failed_answers` entry). The last is the path a conformant C++ client never
  exercises by accident — once it has processed the exception its promise is
  broken and it fails such calls locally — so the driver queues it in the same
  turn as the triggering Accept and never turns its event loop in between,
  making it deterministic on frame order rather than on timing.

  The eviction is driven by that second Accept, not by a timer: the TTL sweep
  runs lazily from the Accept path and nowhere else, so "a later Accept
  reclaims an expired parked one" is proven here too.

  Ablation-proven and arm-discriminating: forcing the recorded-exception lookup
  to miss leaves the drain arm green while the driver's late call times out and
  the host's expired-Return count drops 3 -> 2.

## [0.7.0] - 2026-07-31

Level-3 three-party hosting is complete: a vat can now host a handoff whose
provided capability it merely *imports* from the introducer, proven against the
C++ reference. Alongside it, a deliberate freeze ceremony on the Stable surface,
the missing half of the list-upgrade rule, and a run of gate work that found
three real defects — including two the gates could not previously see at all.

The recurring theme is worth stating: several of these were found by *building
the gate*, not by suspecting the bug. A use-after-free in peer teardown, an
unsound OOM harness, and 179 tests that had never once been compiled were all
invisible until a lane existed that could see them.

### Breaking

- **Five frozen Stable signatures lost their `anytype` holes.** The API snapshot
  cannot pin a *generic* function's error set: Zig will not resolve an inferred
  error set through an `anytype` parameter until instantiation, so nine Stable
  lines rendered an opaque `@typeInfo(...).error_union.error_set` marker that is
  **identical no matter what the set contains**. Adding, removing or renaming an
  error on any of them passed `zig build check-api` unchanged while breaking
  every consumer's `catch |err| switch (err)`. `api-closure` skipped them too.

  Two techniques, applied together. **De-genericized** (this is the breaking
  part — the parameter type is now concrete):

  - `message.MessageBuilder.writeTo` / `writePackedTo` take `*std.Io.Writer`
    instead of `anytype`.
  - `rpc.wire.protocol.CapDescriptor.writeReceiverAnswer` /
    `writeThirdPartyHosted` / `writeThirdPartyHostedNull` take
    `message.StructBuilder` instead of `anytype`. Callers holding a generated
    `rpc_capnp.CapDescriptor.Builder` pass its `._builder` field.
    `StructBuilder` was chosen over the generated builder deliberately: the
    latter renders under `rpc.generated.*`, which is Experimental, and would
    have tripped `api-closure`. The infallible `writeSenderHosted` /
    `writeSenderPromise` / `writeReceiverHosted` keep `anytype`; they return
    plain `void`, so neither hole applies.

  **Annotated** (signature otherwise unchanged): `codegen.ArrayListWriter.print`
  declares `error{CodegenBudgetExceeded, OutOfMemory}` — an explicit return set
  renders concretely even while `args: anytype` keeps the function generic.

  Opaque markers in `docs/api-snapshot.txt`: **9 → 2**. Genuinely tightened:
  **5 of 9**. Two of the remaining four —
  `CapDescriptor.writeThirdPartyHosted` and
  `rpc.caps.table.payload_remap.clonePayloadWithRemappedCaps` — resolve to
  `anyerror` at *every* instantiation (`message.cloneAnyPointer` is declared
  `anyerror!void`; `map_inbound_cap` is an `anyerror`-typed callback), so they
  now render an honest `anyerror!void` rather than a misleading marker, with no
  tightening claimed. `reader.Reader.readMessage` / `readPackedMessage` stay
  unpinned: they are duck-typed over any stream reader, so the set belongs to
  the caller's reader type. See `docs/supported-surface.md`.

  Ablation-proven in both directions. Before: `return error.Sentinel` added to
  the generic `writeReceiverAnswer` left `check-api` **green** — the hole. After:
  the same edit turns it **red** with `Sentinel` visible in the diffed set.

- **`CanonicalizeOptions.omit_default_pointers` now defaults to `false`.** The
  spec's canonical form is **schema-free**: the reference canonicalizer cannot
  know a written pointer equals its field's schema default, so it keeps the
  pointer. Ours defaulted to *nulling* it — a capnp-zig extension — which made
  canonicalize-and-compare against any other implementation's output diverge
  precisely on default-valued fields. Proven differentially: a `TestDefaults`
  with `textField` explicitly set to `"foo"` (its schema default) canonicalizes
  under `capnp convert binary:canonical` to 24 bytes keeping the text; ours
  dropped it. The default output is now byte-exact with `capnp`; the omission
  stays available as an explicit opt-in for schema-aware equality.

  **Migration:** pass `.{ .omit_default_pointers = true }` to keep the old
  behaviour. No caller inside the repo relied on it — the canonicalizer had no
  callers at all until this release series added them.

### Fixed

- **A Call pipelined on an already-failed answer never received a Return.**
  Found on the wire by the cross-impl L3 lane (`just e2e-l3-vatc`,
  pipelined-provide scenarios): the C++ recipient pipelines its Call on the
  Accept question without waiting, the host refuses the Accept
  (`CrossPeerReceiverHostedTargetUnsupported`), and the Call — arriving after
  the refusal already went out — parked in `pending_promises` forever. The
  queued-call drain in `sendReturnException` only reaches calls queued
  *before* the exception Return; results Returns are recorded in
  `resolved_answers` precisely so late pipelined calls can resolve, but
  exception Returns were recorded nowhere, so a late call on a failed answer
  was indistinguishable from one on a still-pending answer. The C++ recipient
  hung on the spec's exactly-one-Return-per-call guarantee. Not Accept-specific:
  any answer failed synchronously during dispatch had the same window.

  Exception Returns are now recorded in a `failed_answers` map (reason + type,
  kept until Finish, the exact lifecycle of `resolved_answers`), and a
  promised-target call that would otherwise queue is answered immediately with
  a **copy of the recorded exception**, preserving the retryability signal —
  the broken-pipeline behavior of the C++ reference. The record is written in
  the one funnel every exception Return passes through, so transitive
  pipelines (a call pipelined on a failed pipelined call) are covered, and the
  drain path is unchanged. Ablation-proven unit tests pin both the refused-
  Accept shape and the plain two-party shape; the e2e driver now observes the
  refusal *through* a pipelined call instead of working around the gap with
  `whenResolved()`.

- **`MessageBuilder.writeTo` and `writePackedTo` were neither type-checked nor
  tested.** Both are frozen Stable API with **zero call sites in the tree**, and
  Zig does not analyse an uninstantiated generic body — so making them concrete
  would only have swapped an unchecked generic for an unchecked concrete
  function. `tests/serialization/message_test.zig` now instantiates and executes
  both: a multi-segment message (which forces the multi-entry segment table and
  the even-count padding word) streamed through `std.Io.Writer.Allocating`,
  asserted byte-for-byte against `toBytes`/`toPackedBytes` and then read back
  and compared field by field, plus the zero-segment frame. Both functions turn
  out to be correct — the defect was the absence of any check, not the code.

- **179 `test` blocks under `src/rpc` were never compiled, let alone run.**
  `src/rpc/mod.zig` re-exports through namespace *structs*; Zig analyses a
  container's declarations lazily, so nothing forced the files behind them to be
  analysed, and a file that is never analysed contributes no tests. The library's
  own test root collected 148 tests out of 349 present in `src/`.

  Proven with a canary rather than by inspection: a
  `test { try std.testing.expect(false); }` added to `src/rpc/peer/mod.zig` left
  `zig build test` exiting **0** without ever mentioning it. With the fix in
  place the same canary fails, which is what says the mechanism works.
  `std.testing.refAllDecls` is not sufficient on its own — it stops at those
  namespace structs, and the recursive variant it used to pair with no longer
  exists in std — so `src/rpc/mod.zig` now carries a bounded declaration walk.
  The library test root goes from **148 to 310** collected tests.

  Compiling them for the first time surfaced 24 compile errors across 9 files,
  all API drift in the tests rather than defects in shipping code, plus four
  genuine test failures that had been dormant:

  - Two `connection handleRead` tests built a `Connection` with
    `.transport = undefined` and segfaulted dereferencing the io vtable.
    `invokeTerminalError` gained a `transport.shutdown()` call after those tests
    were written, and nothing ever compiled them again.
  - Two `peer_return_dispatch` tests stored a `[]const u8` borrowed from a
    `DecodedMessage` their own hook destroyed on return, then asserted on it
    afterwards. They read back as `UUUUUUUUUU` — `SafeAllocator`'s `0x55`
    free-fill — which is to say these tests had a use-after-free of their own.

  The checked-in generated code under `src/rpc/gen/` also could not be analysed
  by the library test root, because it imports the library by module name the
  way a consumer does; the test module now self-imports as `capnpc-zig`, matching
  what `lib_module` already did.

- **Use-after-free in `HostPeer` teardown.** `Peer.deinit` is send-bearing —
  `forceCancelAllQuestions` emits `Finish` frames and `releaseAllImports` emits
  `Release` frames whenever a send path is available — and `HostPeer` installs
  exactly such a path, a `setSendFrameOverride` callback that appends into its
  own `outgoing` queue. But `HostPeer.deinit` freed that queue *first* and tore
  the peer down last, so every teardown-time frame was appended to freed
  storage. The peer is now destroyed before the queues its override writes into.

  It survived this long because no lane could see it. In Debug and ReleaseSafe
  `ArrayList.deinit` poisons the handle, and these suites happened never to trip
  a safety check on the poisoned value, so all 981 tests passed; ReleaseFast
  leaves the freed pointer intact, the append lands in still-mapped freed
  memory, and only `SafeAllocator`'s free-fill check notices. Ablation-verified
  in both directions: restoring the old order reproduces exactly 8 crashing
  tests with `panic: write after free`, all on the same 144-byte allocation.

### Added

- **The receiverHosted lift (L17): cross-peer Level-3 accepts of an IMPORTED
  provide target are now SERVED instead of failing closed.** Before this, a
  vat hosting a handoff whose provided capability it merely *imports* from the
  introducer answered the recipient's Accept with a pinned
  `CrossPeerReceiverHostedTargetUnsupported` exception at two sites: the
  stored `.local{receiverHosted}` target, and the stored `.promised` target
  whose owner-side re-resolution lands on `.imported`. Both now serve through
  the same cross-peer proxy machinery as exported targets — the proxy minted
  on the accept peer forwards to the owner's import via the forwarded-vine
  call path.

  Mechanism (deferred-Release import pinning, the reviewed design's five
  rules): `ImportEntry` gains `handoff_pin_count` and `deferred_release`.
  (1) RETENTION — `CapTable.releaseImport` keeps a pinned entry alive when a
  decrement would drop `ref_count` to 0 and returns *false* (resolved-import
  cleanup is deferred to the unpin instead of firing eagerly and recursing
  through `releaseResolvedCap` mid-handoff). (2) WITHHOLD — the peer's
  `send_release` seam (`sendReleaseDeferringHandoffPin`, bound at both
  generic import-release walks) accumulates released counts into
  `deferred_release` while pinned instead of emitting a Release frame; a
  `receiverHosted` descriptor grants no transferable wire reference, so the
  withheld count is the only thing keeping the introducer-side export alive
  across the [Provide, serve) window. Imports alive on `promise_ref_count`
  alone still release eagerly, and the raw senders (`sendReleaseForHost`, the
  deinit sweep) stay eager. (3) DEFERRED EMISSION — the last unpin emits the
  accumulated count as one exact Release via the raw sender, zeroing the
  tally regardless, so a second pin/unpin cycle starts from 0. (4) REMOVAL —
  the unpin then performs the removal + resolved-import cleanup retention
  deferred. (5) POSTURE — the unpin is fallible; wire-driven Finish re-raises
  OOM, teardown catch-logs (the documented deinit best-effort exception).
  Pins: site 1 takes a Provide-time pin at registration (`target_import_pinned`
  — a flag deliberately SEPARATE from `target_export_pinned`: import and
  export ids share one numeric space per connection, and one flag would unpin
  the wrong table by a colliding bare id) plus a serve-time pin owned by the
  proxy ctx (`release_source_import_pin_id`, abandoned when the source peer
  dies); site 2 takes the serve-time pin only, its [Provide, serve) window
  covered by the stored answer's own liveness (a vanished target still fails
  closed with `CrossPeerProvisionTargetUnavailable`). Embargoed accepts of
  receiverHosted targets complete at Disembargo time through the same serve.

  Ablation-proven teeth: with the retention branch disabled, the V2-M6
  integration test dies at the probe (`ImportDiedDespitePin` — the entry is
  destroyed by wire refs draining *between* Provide and Accept); with the
  withhold branch disabled, it dies at the deferred-tally probe (the Release
  goes out eagerly and the introducer-side export is destroyed before the
  serve). Wire totals are pinned by a dedicated unit suite: a pinned drain
  emits ZERO Release frames, the unpin emits exactly one `Release(id, N)`,
  and total granted == total released across re-grant cycles. Cross-impl: the
  `pipelined-provide` / `pipelined-provide-chain` e2e scenarios (previously
  fail-closed witnesses) are rewritten to assert SUCCESS against the C++
  reference over real TCP — the accepted cap reaches the driver's own local
  capability (43, not host-Carol's 42) for both stored forms, and the
  driver's Release ceremony drains the host leak-free (18/18 lane).

  Also fixed in passing, exposed by the new OOM sweep: `addCrossPeerProxyExport`
  double-released its transferred source-peer leases when
  `registerCrossPeerProxy` failed (the destroy sweep's ctx deinit released
  them, then the errdefer's manual arm released them again — enough to steal
  a coexisting provision pin). The once-only discipline is now tracked
  explicitly (`leases_transferred`).

- **The inverse direction of the list-upgrade rule: a struct list now decodes
  back as the primitive or pointer list an older schema declares.** The forward
  direction landed earlier (any element size except one bit decodes as a struct
  list, so a peer that evolved `List(UInt32)` into `List(SomeStruct)` reads old
  data). The other half — the *old* binary reading the correctly-encoded struct
  list the evolved peer now writes back as `List(UInt32)` — was rejected with
  `error.InvalidPointer`. Both reference implementations accept it, so capnp-zig
  was rejecting messages every other implementation reads.

  `StructReader.readU8List` / `readI8List` / `readU16List` / `readI16List` /
  `readU32List` / `readI32List` / `readF32List` / `readU64List` / `readI64List` /
  `readF64List` / `readTextList` / `readPointerList` now accept element size
  C = 7, resolving it through the inline-composite path. That routing is the
  whole difficulty: a C = 7 list pointer's D field is a **word** count, not an
  element count, and its content offset addresses the **tag** word, so merely
  relaxing the element-size check yields a reader with the wrong length anchored
  one word early — no error, just wrong data.

  Preconditions follow the C++ reference (`layout.c++`, `readListPointer`) and
  are the amplification mitigation, not politeness: a primitive list requires a
  non-empty data section, a pointer list requires at least one pointer, and a
  struct list is never readable as `List(Bool)`. A struct list carrying no data
  at all would otherwise synthesize readable elements for free, and a
  pointer-only one would hand pointer words back as data. Text, Data and
  `List(Bool)` are unchanged — C++ requires byte elements for the blobs and
  hard-fails composite-as-bit, so relaxing those would diverge from the
  reference rather than converge with it.

  Elements are strided by the whole struct, so the list readers carry a
  `stride_bytes` field (0 = the element type's natural stride; defaulted, so
  existing literals and checked-in generated code are unaffected).
  `U8ListReader.slice` now fails with `error.InvalidPointer` for such a list:
  its bytes are one struct apart, so no contiguous slice of the segment holds
  them and only them; `get` still works element by element. The pointer arm adds
  the element's data-section length before reading the pointer word — the offset
  go-capnp's `TextList.At` omits while its own `PointerList.At` applies it, so
  that arm was verified against the C++ reference only.

  Not covered: the same direction one level down. `PointerListReader.getU32List`
  and friends, and `AnyPointerReader.getPointerList`, still reject a struct list,
  which affects nested `List(List(UInt32))` and type-erased access rather than
  ordinary fields. Recorded in `docs/supported-surface.md`.

  Three ablations, each pinned by a different test: reading at the natural width
  instead of the struct stride turns five tests red with value mismatches from
  the second element on; taking the base and count from the plain list
  resolution turns six red, including `expected 3, found 6` on a
  two-data-word fixture where the element and word counts differ; and dropping
  the data-section precondition lets a pointer-only struct list be read as
  `List(UInt32)`.

  Stable surface: `stride_bytes` on twelve list readers, plus
  `U8ListReader.slice` gaining `InvalidPointer`. No `read*List` signature
  changed — the inline-composite resolver's `InvalidInlineCompositePointer` is
  folded into the plain path's `error.InvalidPointer` rather than widening a
  frozen error set.

- **`just test-release-safe-full`, wired into `release-preflight`.** The full
  suite under ReleaseSafe is what CI's per-OS job runs, but no local recipe did
  — `test-release-safe` is a ten-binary subset, and the gap let two defects
  reach `main` in this cycle: the OOM harness that reported a deterministic
  function as nondeterministic, and a dangling `ctx` pointer that segfaults on
  amd64 while a still-mapped stack page hides it on arm64. Neither Debug,
  ReleaseFast, nor the subset showed either. Being green locally in every other
  mode is not evidence.

- **A `test-release-fast` lane** (`zig build test-release-fast`, `just
  test-release-fast`), wired into `just ci` and the per-OS CI Test job. It is a
  memory-safety lane rather than a performance one: ReleaseFast is the only mode
  that does not poison freed memory, which makes it the only mode where a
  use-after-free reached from a *destructor* is observable. Scoped to the
  teardown-heavy RPC suites, where destructors do real work (cancel questions,
  release imports, drain provisions). Proven to have teeth — reverting the fix
  above turns this lane red.

  A caveat worth stating: `unreachable` is undefined behavior rather than a
  panic in this mode, so a failure here deserves reading before it is "fixed".

- **The cross-implementation Level-3 hosting lane is now gated.**
  `just e2e-l3-vatc` — in which the vendored Cap'n Proto C++ reference drives
  the recipient and introducer roles over real TCP against a capnp-zig two-peer
  VatC host — ran in **no workflow at all**, and in neither `just ci` nor
  `release-preflight`. It existed only as a recipe someone had to remember to
  type. v0.6.0 shipped citing it as the evidence for cross-implementation
  hosting while nothing executed it. It now runs in the `e2e-zig` CI job, which
  already pays for docker and the cpp-rpc image, so the marginal cost is close
  to zero, and in `just ci`.

  Ablation-verified rather than assumed, because "hollow gate" is exactly the
  failure this is meant to end: drawing reflected-loopback question ids from
  the outbound wire space again — the real defect that cross-implementation
  first contact originally found — turns the lane red. It is also the only gate
  that would catch a vendored-submodule bump breaking conformance.

- **A lane that actually executes an `std.Io` backend selector.** The
  `evented-check` job ran `zig build -Dio-backend=evented check`, which verified
  nothing that plain `zig build check` did not already: `-Dio-backend` is a
  `[]const u8` compared at *runtime* by `io_backend.parseKind`, so all three
  arms of `Backend.init` are semantically analysed in every configuration.
  Ablation-proven — breaking the `.evented` arm turns plain `zig build check`
  red with no flag passed at all.

  What was missing was any lane that *executes* a selector. `just
  check-selector` / `zig build -Dio-backend=threaded e2e-self` now runs the RPC
  e2e over an explicitly chosen backend, in CI and in `just ci`. Its own
  ablation: making `parseKind("threaded")` return null leaves the old compile
  check **green** and turns the new lane red. `.threaded` is the only selector
  that can carry RPC today — see the Evented note above. The compile check is
  kept as a cheap cross-check that the evented selector still builds.

- **CI runs the full suite under ReleaseSafe**, not the ten-binary
  `test-release-safe` subset. That subset covered message, codegen, framing,
  fuzz-smoke and two transport files — leaving most of the RPC runtime (peer,
  caps, promises, vat, integration) never executed with safety checks on in an
  optimized build. Ablation-verified both ways: a deliberately failing assertion
  in `tests/rpc/peer/rpc_peer_test.zig` leaves `zig build test-release-safe`
  **green** and turns `zig build test -Doptimize=ReleaseSafe` red. The narrow
  step remains for a quick local pass and for the nightly job.

- **The OOM-injection harness was unsound, and the ReleaseSafe lane found it on
  its first run.** `L4 Join proxy relay rolls back state under OOM injection`
  failed under ReleaseSafe on both amd64 tiers while passing on macOS and
  linux-aarch64. The rollback code was never at fault — all 34 injected failures
  rolled back cleanly, with no leak and no swallowed OOM.

  `std.testing.checkAllAllocationFailures` shares **one** backing allocator
  across every fail-index iteration, which couples the runs through residual
  heap state. `FailingAllocator` bumps `alloc_index` only in `alloc`, while
  `resize`/`remap` bump a separate counter it never fails — so a buffer that
  needs a fresh `alloc` on a cold heap can grow *in place* on a warm one. One
  allocation disappears, the last fail index is never reached, and a perfectly
  deterministic function is reported as `NondeterministicMemoryUsage`. Measured
  on ubuntu-latest: reference run 35 allocations; iteration 34 made 34
  allocations and 6 resizes with no failure induced — exactly one anomaly, at
  the last index only, because for every earlier index the injected failure
  fires first. It is platform-dependent because in-place growth is an
  allocator/heap-layout decision.

  `harness.checkAllAllocationFailuresIsolated` gives each iteration a pristine
  `DebugAllocator`, which removes the coupling and keeps per-iteration leak
  detection. With a fresh heap every time, an unreached fail index becomes a
  real signal instead of an artifact. Worth stating plainly: the previous
  arrangement could silently under-test rollback paths on any platform where
  growth happens in place.

- **The Level-3 index thread-affinity tripwire now covers the Accept path.**
  `ProvisionIndex.assertThreadAffinity()` pins the index to the first thread
  that touches it, and was called from `attachProvisionIndex` and the *Provide*
  path but not the *Accept* path. A vat driven from two threads therefore
  panicked loudly on a second-thread Provide and raced **silently** on a
  second-thread Accept. Multi-threaded vats are unsupported; both paths should
  fail the same way. One assert, ahead of the park sweep and the lookup.

  The test is the real work here, and worth describing: the L3 vat suite calls
  `index.disableThreadAffinity()` throughout, so anything written on that
  harness would silently no-op and prove nothing. The new test builds its own
  index with affinity enabled, attaches by hand (because `attachProvisionIndex`
  asserts, which would pin the index before the Accept ever arrives and pass
  vacuously), and is Debug-guarded since the tripwire is Debug-only and
  `thread_id` is literally `void` in release builds. It compares the pinned id
  as an *optional* so an ablation reports a mismatch rather than aborting on a
  null unwrap. Ablation-verified: deleting the assert yields
  `expected <id>, found null`.

- **Parked Level-3 accepts can now expire (opt-in TTL).** An inbound `Accept`
  whose recipient token matches no provision does not fail — it *parks*, by
  design, because the rendezvous is order-independent. But the token is
  arbitrary attacker bytes needing no authentication, no prior `Provide` and no
  bootstrap, the budgets were count/byte only, and the actual release point is
  **`Peer.deinit`, not connection close**. One stranger connection could hold
  vat-wide park slots for a peer's whole lifetime and starve unrelated
  legitimate tokens. (The previous limitation text said "until its connection
  dies", which understated it.)

  `ProvisionIndexLimits.park_ttl_ms` (default **null**, so behaviour is
  bit-identical until an embedder opts in) plus an **index-owned** clock —
  deliberately not sourced from a peer, since `Peer.setClock` is per-peer and
  two peers of one vat can sit in different time domains, which would
  mis-compare deadlines and leave clockless-peer parks immortal. The sweep is
  lazy, running at the next `Accept`, rather than on a tick: a tick only fires
  when poll times out, so an attacker keeping its own connection busy could
  suppress the very sweep meant to evict it.

  Three ablations back it, including the one a naive implementation fails:
  `failPendingAccept` decrements only the *queued* counters and does nothing for
  parked entries, so reusing it without adding the parked decrement leaks the
  budget upward forever — permanently shrinking the thing the TTL exists to
  protect. Stripping those decrements reddens two tests, on the denied legit
  accept and on a stale count.

- **`WorkerPool` peers now get a real entropy source.** `workerMain` did
  `Peer.init` + `setClockIo` and nothing else, so `entropy` stayed null on every
  pooled peer and `nextAcceptEmbargoId` fell back to a **counter** where the
  spec wants unguessable accept-embargo ids. One `DefaultCsprng` per worker
  *thread*, living on the worker frame: thread-confined by construction, so no
  lock and no thread-safety requirement on the CSPRNG. Handing each peer a
  pointer to it is sound because every accepted peer is destroyed inside the
  same loop iteration that created it.

  Fails closed the way `ServerSession` does — a worker that cannot obtain secure
  entropy refuses the connection rather than quietly downgrading, and re-seeds
  per accept so one transient `randomSecure` failure does not poison the worker
  for its whole life. Urgency is genuinely low: pool peers are never enrolled in
  a `Vat` today, so this was spec hygiene rather than a live weakness.

  Also corrects the record: an earlier revision of `docs/supported-surface.md`
  blamed "a shared CSPRNG across worker threads", which is not what the code
  did — the pool shared nothing and installed nothing.

- **A regression net was built under the `receiverHosted` arms before they were
  lifted.** Both sites that used to answer a cross-peer Level-3 `Accept` with
  `CrossPeerReceiverHostedTargetUnsupported` had **zero** test coverage
  anywhere in the repo, so the lift above would have been rewriting them blind.
  They were pinned first — two tests asserting the exception reason exactly per
  site, ablation-verified so neither rode on the other's arm — and the lift then
  flipped those same tests to assert the served path. Recorded because the
  sequencing is the point, not because the intermediate state shipped: what
  survives in this release is the served behaviour plus a fail-closed *witness*
  for the one input that legitimately still refuses, a `.promised` target whose
  re-resolved import has died.

- **The hardening gate now scans the compiler plugin.** `unsafe_dirs` covered
  `src/serialization`, `src/rpc` and `src/wasm` but not `src/capnpc-zig`, so
  every `catch unreachable`, `@panic` and unchecked optional unwrap in the code
  generator was **invisible rather than reviewed** — in a component that parses
  a `CodeGeneratorRequest` off stdin like everything else here. (The plugin was
  already scanned for *disclosure* patterns, which is what made the omission
  easy to miss.)

  Proven by probe before fixing: a fresh `catch unreachable` added to
  `generator.zig` left the gate reporting "Hardening gate passed". With the
  directory added it reports the probe plus six genuine pre-existing sites.

  `generator.typeNameForConst`'s `else => unreachable` on a schema-derived type
  is now `error.UnsupportedConstType` — a panic in Debug and undefined behaviour
  in ReleaseFast, on input this process does not control. Its sibling
  `types.typeToZig` has the identical arm but is **frozen Stable** as
  `error{OutOfMemory}![]const u8`, so converting it has to wait for a snapshot
  ceremony; it carries an allowlist entry saying exactly that rather than
  implying it was judged safe. The four remaining sites (one guarded
  `ancestor_name`, three callback trampolines whose `ctx` is optional only to
  satisfy the callback ABI) get reviewed entries with their reasoning.

### Documentation

- **Three claims corrected, each of which overstated support in the direction
  that could bite a user.**

  - `docs/supported-surface.md` listed canonicalization as plainly "supported".
    Ours is **schema-driven** where the spec's is schema-free, so it emits only
    what the loaded schema knows: data a newer peer wrote for an unknown field
    is dropped, and an upgraded list is re-encoded. The output can be a
    *different message*, which makes it unsafe as a signing primitive while
    remaining fine for canonicalize-and-compare between peers sharing a schema.
  - The `WorkerPool` entropy limitation gave the wrong **cause** — "a shared
    CSPRNG across worker threads is not thread-safe". The pool shares nothing;
    `src/rpc/integration/worker_pool.zig` never calls `setEntropySource` at all,
    which is why pool peers fall back to counter ids. The genuinely
    unsynchronised sharing is `Vat.enroll` handing every enrolled peer a pointer
    to one `&self.rng`. This matters because the stated cause implies a hard
    problem while the real one is small.
  - The Evented `std.Io` backend was described as supported wherever Zig
    exposes it. It **compiles and then fails at runtime on any socket**: at the
    pinned toolchain `std.Io.Evented` is `Dispatch` on macOS (only `netClose`
    implemented) and `Uring` on Linux (only `netBindIp` / `netClose` /
    `netShutdown`), with `netListenIp`, `netAccept` and `netConnectIp` all
    `...Unavailable` stubs. Every RPC path is socket-based. The platform matrix
    now reads "compile-checked only — no sockets", and README and
    `docs/stability.md` say why, upstream.

## [0.6.0] - 2026-07-30

capnp-zig can now act as **VatC — the host** — in a Cap'n Proto Level-3
three-party handoff, proven cross-implementation against the C++ reference. The
release also carries the three protocol fixes and the freeze-gate work that had
been sitting unreleased behind it.

Twenty-seven commits since v0.5.0, this cut included. It also repairs the
changelog itself: `6460319`
prepended a second `## [Unreleased]` heading over the file's title twelve
commits ago, stranding the preamble mid-file and hiding three protocol fixes
below a section boundary most readers would never scroll past.

### Breaking

- **The minimum Zig version moves from `0.17.0-dev.813` to
  `0.17.0-dev.1509`.** `mise.toml` is now the single version specifier for this
  repository; `build.zig.zon`'s `.minimum_zig_version` states the floor rather
  than acting as a second pin.

  **Migration:** install `0.17.0-dev.1509+bb296ab9b` or newer — `mise install`
  reads the pin directly. Two source-level changes arrive with the jump and may
  touch your own code: `std.builtin.OptimizeMode`'s tags are now lowercase
  (`.debug`, not `.Debug`), and `zig fmt` rewrites `@intFromEnum`/`@enumFromInt`
  on a packed struct's backing integer to `@backingInt`/`@fromBackingInt`. If
  you manage Zig with zvm, its PATH entry wins over mise's shims — use
  `mise exec -- zig ...` to match CI exactly.

### Added

- **A multi-connection vat can now HOST Level-3 three-party handoffs (the VatC
  role), Experimental.** The spec's canonical topology puts `Provide` on the
  introducer↔host connection and `Accept` on the recipient↔host connection —
  different `Peer`s of one vat — and every per-peer table previously answered
  such an Accept with `"unknown provision"` (a witness test keeps that failure
  pinned). The new vat-wide `rpc.vat.provisions.ProvisionIndex` holds
  refcounted, connection-independent provision objects (the C++ reference's
  ThirdPartyExchangeValue analogue); the accepted capability is served as a
  proxy export on the accept peer pinned to the owner's export by a new
  Release-immune `handoff_ref_count` ref class, so it survives the
  introducer's Finish, and hostile over-release stays a detected protocol
  error. Embargoed accepts queue in per-provision slots released by the
  spec-form accept-`Disembargo` (promisedAnswer target naming the Provide
  question — the introducer now emits that form, ordered after the
  parked-call replay so e-order holds under synchronous transports, where the
  old ordering both inverted e-order and stranded the parked calls).
  Accept-before-Provide parks and is adopted by the later Provide; stored
  promised targets re-resolve on the owner; accept-embargo ids are 16 random
  bytes from a fail-closed `randomSecure`-seeded source on tcp sessions; the
  `rpc.peer.Vat` facade owns the index + CSPRNG with one `enroll(peer)` per
  connection. Teardown is leak-free with either peer or the index dying
  first.

  **Cross-implementation hosting is now proven against the C++ reference**
  (`just e2e-l3-vatc`): the vendored Cap'n Proto 2.0 drives vats A and B over
  real TCP against a Zig two-peer VatC across four scenarios, with both sides
  asserting. That first contact also found a genuine host defect — reflected
  (loopback) call question ids were drawn from the outbound wire space, which
  reflection merges with the remote-owned inbound answer space, so our own
  reflected frame could collide with the remote's live answer id and be
  rejected as a duplicate. Fixed by drawing loopback ids from a separate
  descending space; no Zig↔Zig test had exposed it.

  **Deliberately not everything:** `receiverHosted` provide targets fail closed
  cross-peer (the wire-honest deferred-Release import pin is specified but
  unlanded); vats are single-threaded (`WorkerPool` excluded); and beyond the
  C++ reference, **cross-implementation hosting remains unproven** — no other
  implementation can currently drive the recipient/introducer roles against a
  capnp-zig host (go-capnp's 3PH is `TODO`; the Rust/Python adapters are
  two-party only). See `docs/supported-surface.md` for the full limitation
  list.

- Additive entries on the frozen Stable surface (`docs/api-snapshot.txt`,
  689 → 695 declarations, nothing removed or changed):
  `Message.resolveStructListPointer`, `StructListLayout`,
  `protocol.ExceptionType`, `protocol.Exception.kind`,
  `protocol.MessageBuilder.buildAbortTyped`, and
  `protocol.ReturnBuilder.setExceptionTyped`.

### Changed

- **Vendored Cap'n Proto bumped to the `v2` tip** (`ba3d1f6b` → `f8498184`,
  185 commits). The old pin was a six-month-old commit that upstream's `v2`
  branch had moved well past. `rpc.capnp` is unchanged by the bump, so the
  mirrored `src/rpc/capnp/rpc.capnp` stays byte-identical and no generated
  code moves. KJ made `kj::Exception` non-copyable in this range, so the two
  e2e C++ drivers now use `e.clone()` where they used `kj::cp(e)` — upstream's
  own prescribed migration. Both L3 interop lanes were re-run against a fully
  rebuilt image: `e2e-l3-vatc` 12/12 and `e2e-l3-cpp` PASS(60).

  Note this tracks an **unreleased** line: there is no 2.0 tag, and the 3PH
  surface every L3 interop lane depends on does not exist in the latest stable
  release (1.5.0 has none of the rendezvous types or `VatNetwork` hooks), so
  pinning to a stable tag would mean deleting that evidence.

- **One Zig version specifier, and CI installs from it.** Zig had been pinned
  in three places that drifted independently: `mlugg/setup-zig`'s `version:`
  input (repeated across fifteen call sites), `build.zig.zon`'s
  `.minimum_zig_version`, and the nightly fuzz lane's private override. CI now
  installs Zig with `mise`, and the local composite action *asserts* `PATH`
  against `mise current zig` instead of carrying a copy of the version — so a
  mismatch fails in seconds with both values printed, rather than twenty
  minutes later as a confusing compile error. It also retires the last Node 20
  action in the workflows.

  Two failure modes closed along the way, each of which had already cost a red
  CI run. `jdx/mise-action` must pin its own `version:`: unpinned, its cache
  served a different mise per platform, and older mise cannot resolve a
  dev-build tarball URL — which failed on Linux and Windows while passing on
  macOS. And nothing caches `.zig-cache` across runs any more, because
  `setup-zig`'s cross-run save/restore poisoned CI whenever a *cancelled* run
  captured that cache mid-build; since every poisoned run re-saved it, re-runs
  failed identically and the entries had to be deleted by hand.

  The nightly fuzz lane's private `dev.1252` pin went with it. That lane's real
  fix was moving to an arm64 runner — the Zig fuzzer's `maker` stage aborts on
  x86_64 on every toolchain tried, dev.813 and dev.1252 alike.

  `just check-toolchain` makes the same assertion locally, and
  `release-preflight` and `release-tag` now run it first. Cutting this release
  is what motivated it: zvm installs its shim at `~/.zvm/bin/zig`, which
  shadows mise's on `PATH`, so the local preflight was about to gate a release
  on a toolchain CI never runs.

- **The benchmark regression gate can now actually fire.** `bench/baselines.json`
  carried a global `max_regression_pct: 500.0` while the wall-clock baselines had
  gone unrefreshed since 2026-02-06 — CI numbers had since improved 6–15×, so the
  effective headroom was 65–150×: `packed_pack_default` would have had to become
  roughly 78× slower to fail. Bands are now sized per case from measured data
  (5 successive green CI runs on `main`) rather than one global guess:

  - wall-clock cases: baseline at the **worst observed CI run**, 40% band. The
    observed spread is 1.07×–1.25×, so this cannot flap while still catching a
    real regression — roughly 50× tighter than before.
  - allocation counts: 10% band, and two stale-conservative baselines corrected
    (`packed_unpack_default_allocs` 7→3, `packed_roundtrip_default_allocs` 13→9,
    which had been permitting 2.3× and 1.4× regressions). Variance across every
    observed run is exactly zero, so one extra allocation is now a failure.
  - **allocation bytes are gated for the first time** (5 new cases). The
    benchmarks already emitted `alloc_bytes_per_iter` and `bench_check` already
    reads arbitrary JSON keys, so a change that kept the call count at 6 while
    growing each buffer tenfold was simply invisible. 25% band for its first
    release, since these values have not previously been observed on CI.

  Correction to an earlier claim: the five allocation-count cases were **never**
  governed by the 500% global band — each already carried its own
  `max_regression_pct: 30.0`. Only the five wall-clock cases were dead.
  Ablation-verified: inflating a baseline past its band turns both the
  allocation-count and allocation-byte gates red.

- **The nightly coverage-guided fuzz job no longer reports green without
  fuzzing.** Zig master's fuzzer aborts in its `maker` stage before fuzzing
  begins on the pinned toolchain; the job's infrastructure-failure downgrade then
  fired and exited 0, so 30 consecutive nightlies passed having produced zero
  coverage-guided signal — "the fuzzer never started" was indistinguishable from
  "the window elapsed with no findings". The job now requires a positive signal:
  an infrastructure abort inside the first 120 seconds fails with the remediation
  spelled out, while one after real fuzzing stays a warning. `SECURITY.md` no
  longer lists coverage-guided fuzzing among the controls that actually run.

- **`capnp` is installed on the macOS CI tier, converting 26 silent skips into
  real coverage.** It was installed on Linux only, so ten serialization/codegen
  suites that shell out to it returned `SkipZigTest` — 26 tests silently skipped
  on macOS and 29 on Windows — while the platform matrix advertised codegen as
  "full | full | full". A `capnp --version` step now fails the job outright if
  the tool is missing, instead of letting the suites quietly disable themselves.

  **Windows is deliberately excluded, and that exclusion produced a finding.**
  Installing the upstream prebuilt Windows tools makes the suites run and then
  fail: they ship only the executables, not the standard schema include tree, so
  a schema using an absolute standard import (`import "/capnp/stream.capnp"`)
  cannot compile. So Windows codegen coverage is genuinely **absent**, not
  merely unmeasured — the platform matrix's "full" for that tier was never
  evidenced. `docs/stability.md` now says so and records what closing it needs
  (an `-I` include path threaded through the ~10 test files that hardcode their
  `capnp` argv).

### Fixed

- **`Exception.type` is now set on outbound exceptions and consulted on inbound
  ones.** Every exception capnp-zig sent carried type 0 (`failed`), including
  disconnect and shutdown exceptions, so a peer of any other implementation
  could not tell a retryable transport loss from an application error. Inbound,
  disconnection was detected by string-comparing the reason against capnp-zig's
  own `"disconnected"` literal — which no other implementation emits — so a
  C++/Go/Rust/Python peer signalling `type = disconnected` was reported as a
  plain remote exception. The standard cross-implementation retry signal was
  inert in both directions.

  Now: `protocol.ExceptionType` (deliberately **non-exhaustive** — the wire
  field is a remote-controlled `UInt16`, and future spec revisions may add
  values, so an unknown code decodes to an unnamed tag instead of being illegal
  to construct), `Exception.kind()`, and typed builders
  (`setExceptionTyped` / `buildAbortTyped`). Transport drains and shutdown stamp
  `disconnected`; deadline expiry stamps `overloaded` (the spec classes timeouts
  there); a remote that echoes our question as `Unimplemented` yields
  `unimplemented`; aborts derive their type from the failing error via
  `errors.exceptionTypeForError`. A cross-peer relay now forwards the origin's
  type verbatim instead of laundering it to `failed`.

  Generated `unwrap()` switches on the type rather than comparing reason text.
  This is a **behavior change in generated code with no source change**: the
  signature and `CallError` set are identical, so nothing needs editing, but the
  classification is stricter and more correct. A disconnect reported by any
  implementation is now recognized — and an application exception whose reason
  text merely reads `"disconnected"` is no longer promoted to
  `error.Disconnected`. Two tests that pinned the old text-matching behavior
  encoded exactly that spoof and were rewritten.

  Scope: this stamps the types that carry information for a remote peer. Many
  remaining internal exception sites still send `failed`, which is the spec's
  correct catch-all ("repeating the call would fail the same way") — refining
  those individually is follow-up work, not a defect.

- **An inbound `Call` with `sendResultsTo = thirdParty` no longer silently
  discards its results.** `sendReturnResults` took the third-party branch and
  answered `awaitFromThirdParty` *without ever invoking the `build` closure*. For
  a generated direct handler the whole method body lives inside that closure, so
  the application method never ran; for a deferred handler it ran and its results
  were dropped. Either way the caller was told to await results from a third vat
  that would never be contacted, and its question sat un-settled until the peer
  was torn down — a remote peer could make this vat accept a call, run nothing,
  and leave the question hanging, which is the one outcome the protocol never
  permits.

  Inbound `thirdParty` calls are now **refused by default**, before dispatch and
  before any answer bookkeeping, with a single exception `Return`. That is what
  both reference implementations do — go-capnp echoes `Unimplemented` and drops
  the call, the C++ stack fails the requirement and aborts — so no reference peer
  can observe a difference. `sendReturnResults` now returns
  `error.ThirdPartyResultsNotRedirected` rather than dropping results it cannot
  deliver.

  Applications that *do* perform the redirect themselves opt in with
  `Peer.setThirdPartyResultPolicy(.application)` and settle the answer with the
  new `Peer.sendReturnResultsSentElsewhere`. That emits
  `Return{resultsSentElsewhere}` — the tag the spec mandates for a Return
  answering a Call whose `sendResultsTo` was not `caller`. The previous code
  emitted `awaitFromThirdParty`, which is a *different* message: what an
  introducer sends the original caller on another connection, gated on that
  caller having set `allowThirdPartyTailCall`. Settling also fails any calls
  already pipelined on the redirected answer, since this vat never sees the
  results and so cannot resolve a promised-answer target against them — without
  that drain those children would never receive a Return at all.

  Two behavior changes worth naming: **capnp-zig acting as an introducer or
  proxy no longer propagates third-party result routing by default** (the
  `propagate_accept_from_third_party` forwarding mode requires `.application` on
  each capnp-zig hop), and the L3 redirected-return slice now puts
  `resultsSentElsewhere` on the wire where it previously put
  `awaitFromThirdParty`. Both are Experimental, Zig↔Zig-only paths — no
  reference implementation accepts an inbound `sendResultsTo = thirdParty` at
  all.

- **Struct-list "upgrade" decoding is implemented; capnp-zig no longer reports
  a legal message as corrupt.** Cap'n Proto requires a reader to decode a list
  of any element size except `C = 1` (one bit) as a struct list, synthesizing a
  struct whose data section is the element itself. That rule is the whole
  mechanism behind evolving a `List(UInt32)` field into a `List(SomeStruct)`:
  a peer that performed the evolution keeps reading data written under the old
  schema. capnp-zig rejected every such list outright
  (`error.InvalidInlineCompositePointer`), so a C++/Go/Rust peer that made a
  legal schema change and still had old data on the wire was treated as sending
  a corrupt message — and `docs/error-handling.md` told the consumer to reject
  it.

  `Message.resolveStructListPointer` now dispatches between the native
  inline-composite encoding and the upgrade table, and `readStructList` /
  `PointerListReader.getStructList` both route through it. Element widths are
  tracked in **bytes**, not words: rounding a 1/2/4-byte element up to a word
  would make element *i*'s data section overlap element *i+1*, so a field past
  the real width would return the neighbour's bytes instead of its default and
  the final element would read past the end of the list. An ablation that makes
  exactly that mistake turns four tests red. `List(Bool)` reports the new,
  distinct `error.CannotUpgradeBitList` — a schema mismatch, not corruption.

  Four honest limits on this:

  - **Decode-side only.** The encoder still writes the inline-composite form for
    every struct list, which is the only legal encoding to *write*.
  - **The inverse direction is not implemented.** Reading a `C = 7` list as a
    primitive list — what an old binary needs in order to read data from a new
    binary that correctly writes the evolved schema — is still unsupported.
    This closes one half of the compatibility guarantee, not both.
  - **A type mismatch now reads as defaults rather than erroring.** Because
    `readStructList` accepts `C = 0/2/3/4/5/6`, pointing it at the wrong field
    yields structs whose fields are all at their defaults instead of
    `InvalidInlineCompositePointer`. Every other implementation is equally
    permissive; the RPC capability-table reader inherits it.
  - **`require_struct_size` rejects upgraded elements by design.** That opt-in
    strictness knob is stricter than the spec and will report
    `StructSizeTooSmall` for a legally upgraded element.

- **A list of zero-width elements is charged one traversal word per element.**
  A `List(Void)` previously cost nothing at all, while a struct list of
  zero-width elements was capped by `inline_composite_element_limit`. Since a
  Void list is now readable as a struct list, that asymmetry would let a
  one-word list pointer synthesize 2^29 readable elements for free. Both
  validation paths now charge the element count, matching the reference
  implementation. **This is a validate-path mitigation only** — consumers using
  `Message.initUnvalidated`, which skips the walk entirely, are not protected by
  it. A message containing a very large `List(Void)` or zero-width struct list
  can now fail with `TraversalLimitExceeded` where it previously passed.

- The compiler plugin's zero-width struct-list guard now also tests
  `sub_word_data_bytes`. Without it, a legitimately upgraded `C = 2/3/4` list —
  which reports zero data *words* while carrying real 1/2/4-byte elements —
  would have been rejected as an amplification attempt. A `Void` upgrade still
  lands on all-zero and is still rejected, so the guard keeps its full strength.

- Schema-aware validation and canonicalization are upgrade-aware. All three
  sites keyed their "nothing to walk" early-out on whole words, which is exactly
  the condition an upgraded sub-word list satisfies — they would have reported
  success without validating a single element. They now key on the byte stride,
  and canonicalization re-emits an upgraded list in the inline-composite form,
  so the old primitive-list encoding and the evolved struct-list encoding
  canonicalize to identical bytes.

- **`schema_validation.canonicalizeMessage` did not compile.** It is `pub` and
  part of the **frozen Stable surface**, and it returned `[]const u8` from a
  `![]u8` signature — but nothing in the tree called it, so its body had never
  been type-checked and the mismatch sat there undetected. Any consumer calling
  it got a compile error inside the library. Found by closing the error-set hole
  below: expanding inferred error sets forces body analysis, which surfaced it
  immediately. The schema-validation suite now calls it, so it cannot rot again.

- **The frozen Stable surface is now closed under its own signatures**, gated by
  the new `zig build api-closure` on all three CI tiers. A Stable entry point
  whose signature mentions an Experimental type is only nominally frozen: the
  type can change shape under it at any 0.x bump while `check-api` stays green,
  because the Stable *line* never moved.

  The check's first run reported **14 violations**. Resolving them promoted the
  types a consumer cannot avoid: `ConnectOptions` (needed by the frozen
  `connect`/`connectHost`), `ServeOptions` and a narrowed `Listener` (`init` /
  `close` / `getAddress` — the path both the getting-started guide and the
  shipped example already use), and `Export` (needed by the frozen
  `Peer.addExport` / `setBootstrap`). **Before this there was no fully-Stable way
  to stand up a server**: `ServerSession.accept` required a `*Listener` that only
  Experimental API could construct.

  Two honest limits, recorded in `docs/supported-surface.md`. *Closure is not
  constructibility* — the gate verifies a type named in a Stable signature is
  frozen, not that a Stable constructor exists for it (found the hard way: my
  first ablation removed `Listener.init` and the gate stayed green, because it
  keys on the type's tier, not its members). And a method taking or returning its
  own enclosing type is exempt by design, which keeps `ServerSession` frozen at
  `.accept` plus its lifecycle rather than wholesale.

  Ablation-verified: removing the `Listener` type promotion fails the gate with
  `error.StableSurfaceNotClosed`.

- Optional field defaults now render their **value** rather than "present":
  `default_call_timeout_ms: ?u64 = 30000`, not `<non-null default>`. A 30s → 60s
  change to a documented timeout would otherwise have passed the gate.

- **`just check-generated` no longer diffs the experimental API snapshot.** That
  file is regenerated on every run by design and records target-dependent detail:
  `OwnerThreadId.value` is `std.Thread.Id`, which renders `u64` on macOS and
  `u32` on Linux, so a committed copy can never match on all three tiers. Once
  field rendering started capturing types, this turned the Linux drift check red
  for a difference that is not an API change. The **Stable** file stays in the
  diff — it must be target-stable, and `zig build check-api` proves that by
  running on all three OSes.

- **The API freeze gate now pins struct fields, defaults, enum ordinals, and
  error sets.** `tools/api_snapshot.zig` walked `declarations` only, so every
  frozen struct was pinned by *name alone*: changing
  `Connection.Options.read_buffer_size`'s default, removing a field from
  `PeerLimits`, or reordering a union passed `check-api` green. Separately, 325
  of the frozen lines (nearly half) rendered their inferred error set as the
  self-referential `@typeInfo(...).error_union.error_set` expression, which is
  **identical no matter what the set contains** — so adding, removing, or
  renaming an error on that half of the Stable surface was invisible while
  breaking every consumer's `catch |err| switch (err)`.

  Now: fields render with their default *values*, unions render their variants,
  enums render their ordinals (which matters for wire enums), and inferred error
  sets expand to sorted `error{...}` lists. Opaque error-set lines dropped from
  **325 to 9**. `PeerLimits` moved from an exact to a prefix rule so its fields
  join the contract — it is a config struct whose defaults consumers rely on,
  unlike `Peer` itself, which stays exact so its ~73 fields of internal state
  stay out.

  Ablation-verified, both dimensions: changing a field default produces
  `read_buffer_size: field usize = 65536` → `= 32768` drift, and adding an error
  to a Stable inferred set produces the expanded-set diff. Both were silent
  before.

  Stable declaration count goes 695 → **1282**. That is one large reviewed diff
  from newly-captured detail, not new API: no declaration was added or removed.

- **Three `stable_rules` entries named declarations that do not exist** —
  `Peer.run`, `Peer.close`, and `ClientSession.adoptOwnerThread` — so the freeze
  scope documented a `Peer` run/close lifecycle that was never implemented (the
  nearest real method is the Experimental `closeAttachedTransport`). A new
  comptime assertion requires every rule to match at least one real declaration;
  it found all three on its first run, and makes the rules list self-validating
  against typos in future promotions.

### Documentation

- `docs/supported-surface.md` records the freeze gate's residual holes rather
  than implying it is now complete: nine Stable signatures remain unpinned
  beyond their arity because they are *generic* (Zig cannot resolve an inferred
  error set through an `anytype` parameter until it is instantiated — asking is
  a compile error), and the closure check proves that a type named in a Stable
  signature is *frozen*, not that a Stable constructor for it exists.

- Corrected two `docs/stability.md` claims this work disproved, in opposite
  directions. The bench note called the pipelined throughput case "stable within
  ~3%" and the reliable signal; it is the **widest-variance** case of the
  fourteen at 1.75× across 5 runs. Conversely, both `stability.md` and
  `supported-surface.md` claimed the cross-implementation e2e matrix runs on a
  local Docker host only — it in fact runs **per push** on the Linux tier via the
  `e2e-zig` job, so the project was understating its strongest interop evidence.
  `tests/hardening/toolchain_gate_test.zig` now states that it covers committed
  fixtures only and points at the CI step that asserts the tool.

- The struct-list limitation in `docs/supported-surface.md` contradicted its own
  body: it was titled "Reading a `List(Struct)` from data encoded as a struct
  list is not supported" — which describes the ordinary, fully supported case —
  while the paragraph beneath correctly described the *inverse* direction
  (reading a struct list back as a primitive list). Retitled. The closing
  sentence of that section also claimed no active limitation remained, directly
  under a list of four.

## [0.5.0] - 2026-07-29

This release exists primarily to make the shipped artifact match the verified
one. `v0.4.0` was tagged three minutes before its own push CI went red — the
static hardening gate failed on all three operating systems and the benchmark
regression gate failed with it — and both gates were only repaired twelve days
later. Everything in that repair, plus a remote-triggerable validation-CPU
amplification fix and a pipelined-call `Return` liveness fix, lands here on a
commit whose CI is green in all 21 jobs. `v0.4.0` should be considered
superseded; consumers on it should move to `v0.5.0`.

The minor bump (rather than a patch) is forced by the codegen change under
**Breaking** below: it alters the shape of generated code, which is part of the
Stable tier.

### Breaking

- **Codegen: group-typed union member getters are now fallible.** A union
  member whose type is a group generated a getter with no `which()` check —
  reading it while a sibling variant was selected silently reinterpreted the
  sibling's bits. Group union-member getters now return `!Group.Reader` and
  yield `error.WrongUnionMember` when a different variant is selected, matching
  the guard slot union-members already had. Plain (non-union) groups keep their
  infallible getter.

  **Migration:** regenerate your schemas and add `try` at call sites that read a
  group-typed union member. Only schemas with group members inside a union are
  affected; if `capnp compile` on your schemas produces no `!Group.Reader`
  getters, there is nothing to change.

### Added

- **The release ceremony is written down and mechanically enforced.**
  `RELEASING.md` records the checklist that previously lived only in maintainer
  habit — semver classification (including the rule that a change to the *shape
  of generated code* is a Stable-tier break the `check-api` gate structurally
  cannot see), the green-CI precondition, the version sweep, the CHANGELOG cut,
  and the post-tag `zig fetch` validation. Three gates back it:

  - `zig build docs-smoke` now requires every consumer-facing version stamp to
    match `build.zig.zon` and rejects any `zig fetch` pin naming a different
    version. Ablation-checked: bumping the manifest alone produces 14 failures
    across README, `build-integration.md`, `supported-surface.md`,
    `stability.md`, and the CHANGELOG.
  - `just release-tag X.Y.Z` refuses to create a tag when the worktree is
    dirty, when the manifest version disagrees, or when the commit being tagged
    has no green CI run.
  - `.github/workflows/release.yml` runs on the tag itself and fails when the
    tagged commit's CI was not green, or when tag, manifest, and CHANGELOG
    disagree — so bypassing the recipe leaves a permanent red mark on the
    release instead of being discovered twelve days later.

### Documentation

- Added a **Schema-language support** section to `docs/supported-surface.md`: a
  verified per-feature table (structs/groups/unions, enums, defaults, lists,
  `AnyPointer` variants, generics, annotations, constants, JSON/serde,
  canonicalization, imports) plus the sharp edges — exhaustive enums are not
  forward-compatible, there is no `has<Field>()` accessor, generics are erased
  to `AnyPointer` silently, nested-list writes are limited to primitive inner
  elements, and the JSON manifest is a descriptor, not a serializer.
- Fixed uncompilable serialization snippets in the README and the serialization
  getting-started guide: `Message.init` / `initPacked` take a
  `ValidationOptions` argument (`.{}` for defaults), and a scalar setter
  returns an error union (`try person.setAge(30)`).
- Corrected the QUIC row of the `stability.md` platform matrix: it advertised
  the transport as "CI-gated per push" on macOS, but the only job that passes
  `-Dquic=true` runs on Linux. macOS QUIC coverage is zero.
- Corrected the `build-integration.md` generated-file path: `-ozig:gen` writes
  under `gen/` preserving the schema path (`schema/addressbook.capnp` ->
  `gen/schema/addressbook.zig`); the module now imports the right file, and its
  snippet test asserts the output-dir-prefixed convention.

### Fixed

- **The RPC benchmark gate no longer flaps on hosted CI.** The sequential
  round-trip latency cases were enforced against a baseline measured on a
  developer machine, but a serialized round-trip on a shared CI runner is
  dominated by hypervisor/neighbor scheduling: re-running the *same commit*
  produced p50 94us then 68us, and p99 105us then 210us. `bench_check` gains an
  `advisory` per-case flag (report as `[WARN]`, never gate) and an
  `--enforce-advisory` override for quiet machines; the three
  `rpc_round_trip_seq_*` cases are marked advisory. The pipelined throughput
  case - stable within ~3% across the same runs - stays enforced and remains
  the real code-regression signal. Harness errors still fail regardless.

- **`-Dquic=true` builds expose `rpc.vat` again.** `mod_quic.zig` re-exported
  every `mod_base` namespace except `vat`, so QUIC-enabled builds silently lost
  the Experimental L3/L4 addressing seams (`rpc.vat.network`, `rpc.vat.join`)
  entirely — any consumer building with `-Dquic=true` could not reach them, and
  the L3/L4 e2e drivers would not compile in that config. A QUIC build is the
  same RPC runtime with a different transport, so the namespace lists must
  mirror. With this fixed, `e2e-l4-zig` rejoins the `check-compile` gate (it is
  now verified in native, `-Dquic=true`, and cross-target Windows builds).

- **Nightly fuzz no longer masks genuine findings.** The crash classifier
  downgraded any log matching the bare token `fuzzer.zig` to a warning — but a
  real finding (the fuzz target dying) prints a stack trace that also passes
  through `fuzzer.zig`, so genuine memory-safety findings were silently
  swallowed. The downgrade now matches only the specific zig-fuzzer
  infrastructure-failure markers.

- **Repository hygiene:** the unanchored `.gitignore` entry `test_*` shadowed
  the whole `tests/test_schemas/` directory, silently dropping any new fixture
  added via `git add .`; it is now anchored to the repo root (`/test_*`).
  Removed a committed agent scratchpad under `private/` and now ignore
  `private/`.

- **The per-connection validation-work budget now charges frames that fail to
  decode.** `handleFrame` ran the full validating pointer walk before charging
  the budget, and both decode-failure arms — an unknown message tag (which
  echoes `Unimplemented`, re-walking and cloning the same payload) and a
  malformed frame — returned before any charge. A hostile peer could therefore
  spend unbounded validation CPU, and amplify through the `Unimplemented` echo,
  with frames that never dispatch. Decoding now accounts the traversal-word
  cost even on failure (new `Message.initCounting` / `validateCountedInto` and
  `DecodedMessage.initCounting`, which report the walk's cost including when it
  aborts partway), and `handleFrame` charges that cost before the echo and
  before returning. `SECURITY.md`'s amplification class is closed on this path.

- **`Finish`-cancelling a queued pipelined call now drains calls pipelined on
  it.** Cancelling a queued call sent its mandated `Return(canceled)` but never
  touched the calls pipelined on that call's own (now-cancelled) answer. Those
  grandchildren received no `Return` at all — hanging a compliant caller — and
  their orphaned queue bucket could later replay against an unrelated answer if
  the remote reused the question id. The cancel path now fail-drains those
  pipelined calls (each with its own exception `Return`) through the same
  worklist `sendReturnException` uses, preserving the exactly-one-Return-per-call
  invariant.

- **The `bench-rpc` client now sets `TCP_NODELAY`, matching the real client
  transport.** The benchmark's hand-rolled loopback client never disabled
  Nagle, so on Linux the sequential mode's small frame writes hit the
  Nagle/delayed-ACK interaction and measured the kernel's ~40ms coalescing
  timer instead of the RPC stack — the CI benchmark gate had never passed on a
  hosted runner (the pipelined mode, which batches writes, always passed).
  Real clients were unaffected: `ClientSession` has set `TCP_NODELAY` on
  connect all along.

- **The hardening gate covers the host-call param-cap retention loop.** The
  two `catch unreachable` sites in `HostPeer.onHostCall`'s retention pass are
  reviewed and registered: both `InboundCapTable.get` and `retainIndex` fail
  only on an out-of-bounds index, the loop is bounded by `inbound_caps.len()`,
  and the first param-scan loop already try-walked the same range. The gate
  (red since the retention change landed) passes again.

- **RPC resolved-answer cleanup now preserves pipelined calls across
  reentrant Finish.** A synchronous transport can deliver a caller's `Finish`
  while the callee is still sending the results `Return`, before the callee has
  committed the answer into `resolved_answers`. The peer now marks answers that
  are in that post-send commit window, records the caller's early-Finish flag,
  drains any queued promised-answer calls by committing the reserved answer, and
  then immediately applies the normal Finish cleanup. This avoids stale
  `resolved_answers` entries without dropping parked pipelined calls — the same
  commit window embargoed Level-3 Accept pickup relies on to deliver parked
  promised-answer calls to the host before post-pickup direct calls (regression
  coverage lives in the two-party answer-lifecycle suite). Bootstrap answers now
  commit through the same reserve → send → commit-or-cleanup discipline:
  recording a Bootstrap Return can no longer fail after the frame is on the
  wire, and a Finish that re-enters during the Bootstrap send no longer strands
  a recorded answer (plus its answer-held export reference) that no later
  Finish could clear. The commit-then-cleanup discipline applies in BOTH
  orderings: a late `Return` for a call the caller already cancelled with an
  early `Finish` also transiently commits, so calls pipelined on the cancelled
  answer replay with their own Returns instead of being stranded without any
  Return (previously they hung a compliant caller forever and their frames
  leaked until teardown). The late Return honors the Finish's
  `releaseResultCaps` flag by releasing the wire references its results
  descriptors took (previously they leaked for the connection lifetime), and
  reusing a question id whose early-Finish tombstone is still undischarged is
  rejected as `DuplicateQuestionId` — a compliant caller never reuses an id
  before receiving its Return, and a violator could otherwise force the stale
  tombstone's release flag onto the new answer's result caps (a premature
  export release). Resolved-answer reservations are also counted while they
  are held open across a send, so a nested inbound Call or Bootstrap answered
  synchronously (a reentrant transport delivering frames mid-send) can no
  longer consume the map slot an outer reservation's infallible post-send
  commit depends on at a hash-map load-factor boundary.

## [0.4.0] - 2026-07-11

### Fixed

- **Host-call param caps live until the host answers, and the host can retain
  them.** The host-call bridge (`HostPeer`) used to release a relayed call's
  parameter capabilities as soon as the call was queued for the host, sending
  the remote a `Release` before the host even saw the call — a host handler
  could never legally keep a param capability past its dispatch. Queued host
  calls now retain their param-cap imports; the retained references are settled
  when the host answers, following the Return's `releaseParamCaps` flag:
  `true` (the rpc.capnp default, and the legacy results/exception paths)
  releases each reference back to the remote with explicit `Release` frames
  once the Return is on the wire, while `false` transfers ownership to the
  host — the peer forgets its import bookkeeping silently (new
  `Peer.forgetImportRefsForHost`) and the host sends its own Release when
  done. Queued-but-unanswered calls drop their records without wire traffic on
  `clearHostCalls`/`deinit`. The wasm ABI advertises the behavior via the new
  `FEATURE_HOST_CALL_PARAM_CAP_RETENTION` feature-flag bit (1 << 9).

- **Experimental RPC Level-3 auto-pickup cleanup is callback-failure safe.**
  Automatic `thirdPartyHosted` pickup now sends its internal Accept question
  with no restore-on-return-error from creation, avoiding restored questions
  that point at a freed pickup context when synchronous delivery runs the
  callback before `sendAccept` returns. Failed pickup callbacks now release the
  handoff vine and defer unretained accepted-cap releases until the host has
  committed the Return's export refs in synchronous loopback. Auto-pickup also
  suppresses the generic internal Accept auto-Finish, records the Accept answer
  id in the pickup context, and sends Finish only after synchronous `sendAccept`
  unwinds so the host-side resolved answer has been committed; a bounded retry
  covers transient Finish send failure. Regressions cover callback failure after
  observing the direct cap and a first Accept Finish OOM, verifying the Accept
  question, vine, Provide state, accepted import, and host resolved answer all
  drain.

- **Experimental RPC Level-4 JoinCoordinator direct Accept cleanup is
  Finish-failure safe.** The coordinator now sends its internal direct
  `Accept` with restore-on-return-error disabled and auto-Finish suppressed
  while synchronous loopback delivery is in progress, then Finishes the Accept
  answer after the callback unwinds. It records the Accept answer until that
  Finish actually succeeds, so `takeAccepted`, `releaseAccepted`, cancel, and
  deinit cleanup can retry a still-held host answer later. Regressions cover both
  an initial OOM that succeeds on the bounded immediate retry and repeated OOM
  that drains during accepted-cap release/transfer, including the case where
  `releaseAccepted()` itself returns `AcceptFinishFailed` after releasing the cap
  and a later call drains the same Accept answer: the accepted cap remains
  retained and callable, no question is restored with coordinator-owned callback
  state, and the JoinResult, pending Accept, resolved-answer, and JoinNetwork
  registry state all drain. `takeAccepted()` is also covered while the Accept
  Finish is still failing: it transfers the retained cap to the caller, leaves
  the unfinished Accept answer retryable, and coordinator deinit drains that
  host answer without releasing the caller-owned cap. The direct Accept peer now
  also records a back-link to live
  coordinators; if that peer deinits first, it Finishes any unfinished Accept
  answer, neutralizes the coordinator's borrowed peer/cap pointers, and leaves
  later coordinator cleanup as a safe no-op.

- **Experimental RPC Level-4 JoinCoordinator now tracks split-peer Join
  lifetimes correctly.** The coordinator records the peer for every originated
  Join part and sends each post-pickup Finish or cancel on that same peer, so
  joins split across transparent proxy paths such as A→B and A→C no longer rely
  on a single origin connection. It also rejects duplicate local part numbers
  before sending, rejects new parts once Accept/cancel has begun, and cancels a
  pending direct Accept question if its Return is lost. Dropping the coordinator
  now best-effort cancels pending Join questions and pending direct Accept
  questions before freeing the callback context. Regressions now cover
  duplicate-part rejection without a second wire Join, lost-Accept-Return
  cancellation via explicit cancel and deinit, pending-Join deinit cancellation,
  partial Finish retry without resending already-finished JoinResult questions,
  malformed/exception JoinResult cleanup that Finishes the affected question
  without restoring it, mixed retained-result/malformed cleanup that cancels the
  aggregate Join and releases retained `Joined` leases immediately, mismatched
  successful JoinResults that release retained `Joined` leases and keep failed
  Finish sends retryable before returning
  `JoinResultMismatch`,
  terminal direct-Accept cleanup (exception or malformed Return) that still
  Finishes JoinResult lifetimes and releases retained `Joined` inputs,
  proxy-relay pickup through the real `JoinCoordinator`, and
  `releaseResultCaps` propagation when upstream Finish drains a relayed
  JoinResult lifetime after Return, including downstream Finish send retry.
  Host-side JoinResult rollback now also clears the pending direct-Accept
  provision if both the JoinResult Return and its fallback exception Return fail
  before any JoinResult was delivered.
- **Experimental RPC Level-4 Join allocator ownership tightened.**
  `JoinNetwork.hostJoinResult()` and `JoinNetwork.connectJoined()` now take an
  explicit allocator for returned caller-owned buffers. Host-side JoinResult
  completion clones the pending direct-Accept provision into the Accept peer's
  allocator before transferring ownership, and regressions now cover distinct
  Join-host / Accept-host allocators with promised targets.

- **RPC Level-4 Join state insertion is rollback-safe.** Fresh Join buckets now
  roll back if allocation fails before the first part is fully indexed, avoiding
  stale empty `pending_joins` entries. New regressions cover matching Join
  completion, target mismatch, duplicate parts, Finish-before-completion cleanup,
  Return send-failure fallback, and OOM rollback for the insertion path. L4
  remains Experimental; the public surface is still the raw helper plus
  receive-side readiness.

- **RPC Level-2 persistence rollback and callback ownership are hardened.**
  Restore handlers that host a fresh export now roll that export back if the
  Restore Return cannot be built or sent, so failed restores do not leave
  unreachable exports behind. `Peer.sendRestore` also releases the retained
  restored import when the user callback fails after receiving it, including the
  `OutOfMemory` path. Focused regressions now cover malformed Save/Restore
  results, malformed restore params, Return send failures, independent
  save/restorer hook clearing, and allocator rollback for `setPersistentExport`,
  `setRestorer`, `sendSave`, and `sendRestore`.

- **RPC Level-3 loopback VatNetwork duplicate registration is ownership-safe.**
  `LoopbackVatNetwork.register` no longer double-frees the copied nonce when a
  duplicate registration is rejected. Focused allocator-failure regressions now
  cover loopback introduction mint/connect, `sendProvide` rollback, and
  `sendAccept` rollback so failed origination setup does not leave stale
  questions, vines, provide couplings, or token allocations behind.

- **QUIC transport updated to `quic_zig` 0.7.0**: the dependency is repinned to
  upstream `28f1d960`, and the transport loop now drives `Connection.advance()`
  before polling datagrams and during post-handshake operation to match
  upstream's current embedder contract. `ClientOptions` also exposes
  `insecure_skip_verify` for loopback/self-signed test clients; the default
  remains certificate verification via `quic_zig`'s system-trust client context.

- **RPC forwarded-return intermediary translation now handles
  `takeFromOtherQuestion` / `resultsSentElsewhere` instead of degrading to a clean
  exception.** A peer acting as a forwarding intermediary now rewrites nested
  `takeFromOtherQuestion` IDs through the forwarded-question map in the
  caller-translation, `sendResultsTo.yourself`, and `sendResultsTo.thirdParty`
  propagation modes, and preserves `resultsSentElsewhere` markers for upstream
  callers. This closes the remaining v0.3.0 frozen two-party known limitation in
  proxy topologies; ordinary two-party client/server calls were already
  unaffected. Proven by focused peer regressions covering all forwarding modes.

- **RPC Level-3 forwarded parked calls now remap cap-bearing params/results
  instead of clean-exceptioning.** The introducer-side relay now clones forwarded
  payloads across the independent caller↔introducer and introducer↔host cap-table
  id spaces by minting sender-hosted proxy exports on the destination connection.
  Each proxy retains the source-side import ref until the destination releases the
  proxy, forwards calls back to the original peer, and neutralizes its borrowed
  source-peer pointer if that source connection tears down first. This removes the
  former issue #56 follow-up limitation: forwarded parked L3 calls are no longer
  cap-free-only. Proven by three-peer regressions where the host calls a
  caller-supplied param capability through the relay and where the caller invokes
  a capability returned by the host through the relayed results.

### Added

- **Experimental RPC Level-4 addressed Join pilot over real TCP.** The L4
  `JoinNetwork` seam now has an Experimental `AddressedJoinNetwork` registry
  that requires callers to associate each host peer with an opaque application
  address and direct-peer policy. Joiners first resolve already-live registry
  entries and can now install an application connector that parses unknown
  addressed provisions, dials/resolves the address out of band, caches the
  direct peer for returned `Joined` handles, and drains that cache when the
  handles are released. The generated provision token carries the app address
  plus a nonce, letting the Zig pilot exercise a non-loopback addressing policy
  without claiming a bundled production dialer or stable address format. New
  regressions cover unknown/stale provisions, duplicate provision rollback,
  direct-peer removal, connector malformed-token/no-dial handling, shared-cache
  lease cleanup, network teardown before returned `Joined` release, and OOM
  rollback before connector dialing. `just e2e-l4-zig` now runs a standalone
  Zig↔Zig loopback TCP scenario that bootstraps a hosted Number cap, originates
  two Join parts, consumes JoinResult payloads, sends the direct Accept, invokes
  the accepted cap, and verifies the addressed registry drains.

- **Experimental RPC Level-4 Go recon expanded.** `just e2e-l3-go` now also
  checks the vendored Go L4 source surface: generated `rpc.capnp` bindings expose
  `Message.join`, the vendored twoparty schema carries `JoinKeyPart` and
  `JoinResult`, but the Go receive loop still has no runtime dispatch for
  `Message_Which_join`. This keeps the Go L4 status source-backed without
  claiming runtime interop.

- **Experimental RPC Level-4 transparent proxy Join relay.** Inbound `Join`
  requests that target transparent cross-peer proxy exports now relay through the
  proxy to the underlying source peer, relay downstream JoinResult/exception
  Returns back upstream, and keep the downstream Join question alive until the
  upstream caller sends Finish. Relay state has explicit source-peer back-links
  and direct-Accept-host back-links so owner-peer-first teardown,
  source-peer-first teardown before or after downstream Return,
  upstream Finish-before-Return, failed downstream Finish retry, source
  unavailable, unsupported source-target rejection, downstream Join send
  failure, downstream results/exception Return relay failure, unexpected
  downstream Return cleanup, owner teardown after downstream Finish send
  failure, target mismatch through the relay, and OOM during relay setup all
  drain without stale pointers or duplicate Returns.
  The Zig-only happy path now proves A can join caps through B/C proxy exports,
  Accept directly on A↔D, and invoke the accepted cap without routing through
  the proxy owners after pickup. L4 remains Experimental: no Stable
  `Peer.sendJoin`, no production Join addressing policy, no multi-hop relay
  beyond transparent proxy exports, and no cross-implementation L4 runtime claim.

- **Experimental RPC Level-4 JoinResult runtime expansion.** Peers can now
  attach an Experimental `rpc.vat.join.JoinNetwork`; when present, completed
  inbound Join parts return compact Zig `JoinResult` payloads instead of the
  legacy direct provided-cap shortcut. The joiner resolves those results to a
  direct peer, sends `Accept`, receives the final cap, and invokes it on the
  direct connection. The new Experimental `rpc.peer.JoinCoordinator` wraps the
  compact Zig flow above the raw sender: it emits key parts, collects matching
  JoinResults, sends direct Accept, retains/releases the accepted cap, and
  Finishes each JoinResult question on its originating peer after pickup; it can
  also reject duplicate local part numbers and cancel after JoinResults arrive
  or after a direct Accept question is pending. New regressions cover the
  Zig↔Zig JoinResult→Accept happy path, coordinator accepted-cap release,
  duplicate-send rejection, post-JoinResult and post-Accept-send cancel cleanup,
  malformed/exception JoinResult terminal cleanup, coordinator sendPart OOM
  rollback, pending direct-Accept cleanup, and JoinResult Return send-failure
  rollback. `just e2e-l4-zig` runs the focused Zig runtime gate. L4 remains
  Experimental with no Stable `sendJoin` and no cross-implementation L4 runtime
  claim.

- **Experimental RPC Level-4 Join origination pilot.** `Peer.sendJoinExperimental`
  can now send raw `Join` messages with caller-supplied key parts and ordinary
  Return callbacks. New Zig↔Zig regressions originate a two-part Join, import
  the returned cap, invoke it successfully, verify mismatch exceptions drain
  state, and inject allocation failures through the sender rollback path. This
  is a manual Experimental helper only: there is still no Stable
  `Peer.sendJoin`, production addressing policy, multi-hop relay, or
  cross-implementation L4 runtime claim. Use the Experimental
  `JoinCoordinator` entry above for the compact Zig JoinResult connection flow.

- **Experimental RPC Level-4 legacy Join coordinator coverage.** The older
  direct-cap pilot still has reusable test-local coverage on top of
  `Peer.sendJoinExperimental`: it selects the joined cap, invokes it, releases
  retained result imports, verifies target mismatch does not retain caps, proves
  canceling a partial Join drains remote state, and covers callback failure after
  joined-cap retention. The public helper is the Zig-shape `JoinCoordinator`
  above; the legacy direct-cap coordinator remains regression scaffolding only.

- **Experimental RPC Level-4 C++ runtime blocker probe.** `just e2e-l3-cpp`
  now enforces the source-backed C++ L4 recon result in addition to the
  JoinKeyPart/JoinResult shape checks: vendored C++ exposes the generic L3
  `VatNetworkBase` hooks used by the lane, exposes no callable generic L4 Join
  hook, keeps typed `VatNetwork` Level-4 as TODO-only, and still has only
  Capability client/server Join TODO comments. This keeps the cross-impl L4
  status checked without claiming runtime interop.

- **Experimental RPC Level-3 Go handoff recon gate.** A new `just e2e-l3-go`
  source-backed probe checks vendored go-capnp's current three-party status:
  `Network3PH` hook names are present, but inbound `Accept`/`Provide`,
  `thirdPartyHosted` pickup with a network, `awaitFromThirdParty`,
  accept-context `Disembargo`, and same-network embargo/locality handling still
  hit `TODO: 3PH` guards. This records why there is no Zig↔Go L3 runtime
  interop claim yet and gives future Go 3PH support a failing gate to revisit.

- **C++-first cross-implementation RPC Level-3 handoff e2e.** A new
  `l3_l4_interop` e2e lane (`just e2e-l3-cpp`) runs a real TCP three-party
  handoff with vats A/B in capnp-zig and the hosted Number capability in the
  C++ reference stack built from the vendored Cap'n Proto 2.0 source. Vat B
  receives the C++ `Number`, returns an unresolved promise to Vat A, resolves it
  with `resolvePromiseExportToThirdParty`, and Vat A auto-picks up the
  `thirdPartyHosted` descriptor by sending `Accept` on its direct A↔C++
  connection. The accepted cap is then invoked directly over A↔C++ and returns
  the C++ value. The lane now also runs a C++ failure matrix: bad contact data
  falls back to the vine proxy, invalid/unknown completion tokens and await-side
  C++ rejection produce deterministic pickup exceptions, a C++ disconnect after
  `Provide` still permits direct pickup, duplicate/late `Accept` is rejected
  without yielding a second cap, hosted-cap exceptions report over the direct
  A↔C++ path, and every case checks local Provide/Accept/vine/embargo state
  drains. The lane also records the current L4 recon result: capnp-zig can
  consume the C++-shape `JoinKeyPart` and `JoinResult` structs used by this
  probe, but the C++ reference stack exposes no callable generic `VatNetwork`
  Join hook for the TCP harness. L3/L4 remain Experimental; Stable API
  unchanged.

- **RPC L4 Join readiness note and shared L3/L4 invariants.** The new
  `docs/rpc-l4-join-readiness.md` documents the current receive-side Join slice,
  invariants, evidence, and non-goals. The three-party handoff test harness now
  includes reusable assertions for drained Join, embargoed Accept, third-party,
  and parked-call state.

- **RPC persistence consumer guide and test harness.** A test-only persistence
  harness now shares HostPeer pump helpers plus persistence/cap-table invariant
  checks across peer and reconnect tests. The new persistence guide documents
  the Experimental Save/Restore flow, retained-cap ownership, exact-pin advice,
  and the generated-client path for callers that need `sealFor`.

- **Reusable L3 handoff test invariants.** The three-party handoff peer tests now
  share test-local assertions for import/export presence, drained Provide state,
  outbound vine coupling, and cross-peer proxy backlink cleanup. This keeps the
  Zig↔Zig L3 hardening matrix focused on lifecycle behavior instead of repeating
  table-count checks in every scenario.

- **RPC Level-3 three-party handoff — introducer now FORWARDS parked pipelined
  promise-calls to the capability host (Experimental; closes issue #56).** When a
  caller (VatA) pipelines calls on a promise it holds from the introducer (VatB)
  and VatB hands that promise off to the capability host (VatC) via
  `resolvePromiseExportToThirdParty`, the caller's parked pipelined calls are now
  FORWARDED cross-peer to VatC — targeting the provided capability on the B↔C
  connection — and VatC's results are relayed back to complete VatA's original
  pipelined questions. Previously those replayed calls hit the Level-1/2 rejecting
  vine and returned an exception. The forward preserves e-order (parked calls
  reach VatC in the exact order VatA sent them, and strictly before any
  post-pickup direct call) and is refcount/UAF-safe: it takes no new ref on the
  vine, completes each caller question exactly once, and the vine→Provide liveness
  coupling (the #55 fix) stays intact — a torn-down provided-cap peer safely
  no-ops the forward. The cross-peer relay is also allocation-failure-safe under
  the hardest interleaving: if the forwarded call's own post-return work (e.g. the
  auto-Finish to the host) fails with OOM AFTER the caller's question was already
  answered, the relay neither sends a duplicate Return nor double-frees/restores a
  question whose relay context was already released — the forwarded question is
  created non-restorable, and a settle flag distinguishes "already answered" from
  "never sent". Proven by a new leak-checked three-peer Zig↔Zig test (forwarded
  parked calls reach VatC with the correct values, in order), a fault-injection
  test that forces that post-return OOM and asserts exactly one Return reaches the
  caller, and by the existing live-promise embargo test, now upgraded to assert the
  ordering on the REAL P-pipeline (forwarded parked call reaches VatC before the
  post-pickup direct call) rather than only approximating it via the Accept-result
  pipeline.
  This slice now also remaps cap-bearing params/results by proxying capabilities
  across the two independent peer connections. Still Experimental and
  Zig↔Zig-only.

## [0.3.0] - 2026-07-05

This release promotes the two-party RPC core and serialization to **Stable**;
L3 three-party origination, the reflected-cap resolver, QUIC, and persistence
remain **Experimental** (may change at 0.x minor bumps). The public Stable
surface is now **frozen and CI-gated** — pinned by `docs/api-snapshot.txt` and
enforced by `zig build check-api`; the Experimental surface evolves in
`docs/api-snapshot-experimental.txt`. See
[`docs/supported-surface.md`](docs/supported-surface.md).

### Added

- **RPC Level-3 three-party handoff origination — minimal slice (Experimental).**
  `Peer.sendProvide` / `Peer.sendAccept` originate a three-party capability
  handoff, plus a `rpc.vat.network.VatNetwork` addressing seam (application
  supplies how a peer names/reaches a third vat; a `LoopbackVatNetwork` drives
  in-process tests) and `thirdPartyHosted` descriptor emission. Proven end to end
  by a three-peer Zig↔Zig loopback `Provide`+`Accept` handoff test (leak-checked).
  `Peer.resolvePromiseExportToThirdParty` resolves a promise export to a third-vat
  cap, emitting a `thirdPartyHosted` Resolve; and a recipient auto-pickup seam
  (`setHandoffPickupHandler`) makes a Zig peer, on receiving such a Resolve,
  connect to the third vat via its `VatNetwork` and `Accept` the cap automatically
  (falling back to the Level-1/2 vine proxy when no `VatNetwork` is attached).
  The embargo/disembargo ordering during a live-promise handoff is implemented
  (Phase 4): when the recipient has an in-flight pipelined call, its auto-pickup
  sends `Accept{embargo}` + a `context.accept` `Disembargo` down the promise path,
  the introducer forwards that Disembargo to the capability host, and the host
  holds the `Accept` until it arrives — proven to preserve e-order (a pipelined
  call reaches the third vat strictly before the post-pickup direct call, and the
  embargo demonstrably gates the direct call). Still Zig↔Zig-only (no reference
  impl performs spec-current three-party pickup), and one orthogonal gap remains:
  the introducer does not yet forward the *original* parked pipelined calls on the
  promise to the host (they hit the Level-1/2 rejecting vine) — tracked separately.
  The redirected-return path is also originated (Phase 5): `Peer.sendThirdPartyAnswer`
  lets a callee send its results straight to a third party (callee-allocated
  answer-id in the [2^30, 2^31) range) when a Call arrived with
  `sendResultsTo=thirdParty`, and `Peer.registerPendingThirdPartyAwait` primes the
  third party to adopt it — proven end to end (results reach the third party via
  `ThirdPartyAnswer`, never the caller, which settles with `awaitFromThirdParty`).
  The vine→Provide coupling is now liveness-safe under arbitrary per-connection
  teardown order (resolves the earlier #55 constraint): each coupling records a
  reverse back-link on the provided-cap peer, and that peer's `deinit`
  neutralizes every coupled vine on the recipient peer — nulling the borrowed
  pointer before its own memory is freed — so a later vine `Release` is a safe
  no-op rather than a freed-peer dereference. Proven by a leak-checked chaos test
  driving the previously-forbidden order (drop the provided-cap connection first,
  then Release the vine). Two-party peers are unaffected (all additive behind an
  unset `vat_network`).
- **Cross-impl `resolve_disembargo` e2e scenario** — a reflected-capability
  resolve/embargo scenario in the Docker interop matrix, exercising the full
  `senderLoopback`/`receiverLoopback` `Disembargo` handshake end to end against
  the C++, Go, Python, and Rust reference implementations. capnp-zig both
  originates the reflection (server, via `resolvePromiseExportToImport`) and
  drives the embargo (client). One remaining `SKIP`: pycapnp cannot host the
  reflecting server. (The earlier go/rust client skips were removed once the
  reflected-loopback return was changed to a plain caller return — see the entry
  below.) Net 7 pass / 1 skip / 0 fail, stable across repeated runs. See
  `docs/supported-surface.md` Known limitations #4.
- **Reflected-loopback returns are now consumable by every reference client.**
  When capnp-zig relays a caller's parked pipelined call back to a caller-hosted
  cap (reflected loopback), it no longer returns the eager `takeFromOtherQuestion`
  redirect — it forwards the relayed call as a plain `sendResultsTo=caller` call
  and translates the real results straight back onto the caller's pipelined
  question as a plain `.results` return (a new internal `.translate_to_caller`
  forward mode in `peer_forward_orchestration.zig`, only for the `.imported`
  reflected case; non-reflected forwarding and two-party paths are unchanged).
  This shape is consumed by all four reference clients — go-capnp (which rejects
  an inbound `sendResultsTo != caller`) and capnp-rpc (which stalled on
  `takeFromOtherQuestion`) now both pass the `resolve_disembargo` matrix in the
  embargo-client direction. Resolves Known limitation #4. E-order preserved
  (pipelined-before-direct); proven by the updated reflected-loopback tests + the
  docker matrix.
- **`Peer.resolvePromiseExportToImport`** — resolve a previously exported
  promise capability to a cap the *remote* peer hosts (one we hold as an
  import). This is the "reflected capability" resolution: because the promise
  resolves to a target reached by a different path than the promise itself, a
  conformant remote runs the embargo/Disembargo (`senderLoopback` →
  `receiverLoopback`) handshake against it. The server-side counterpart to the
  already-present client-side embargo handling. Parked pipelined calls on the
  promise are forwarded to the import's owner via the existing resolved-call
  forwarding path. Proven end-to-end (resolve → forwarded pipelined call →
  disembargo round-trip → post-embargo direct call, leak-checked) by
  `tests/rpc/peer/rpc_reflected_resolve_disembargo_test.zig`.
- **Standalone serialization example** (`examples/serialization_demo.zig`,
  schema `examples/addressbook.capnp`): the first runnable non-RPC example — a
  copy-pasteable companion to `docs/getting-started-serialization.md` that
  builds an address book, writes it in both the plain and packed encodings, and
  reads each back zero-copy, exercising nested structs, an enum, lists (of
  struct), an unnamed union, and both Text and Data fields. Both the generated
  schema code and the runtime are wired through `capnpc-zig-core` (no
  RPC/transport in the module graph), demonstrating the serialization-only
  dependency path. `zig build example-serialization` builds and runs it.
- **`rpc.transport.tcp.ServerSession`** — the server-side mirror of
  `ClientSession`: `ServerSession.accept(gpa, listener, options)` takes one
  connection off a `Listener` and returns a heap-owned session bundling the
  `Connection` + `Peer`. Set the bootstrap on `session.peer` (via the generated
  `setBootstrap`), then `run()`; `close()`/`requestStop()`/`deinit()` mirror
  `ClientSession`. Strictly one connection per session. `examples/rpc_pingpong.zig`
  and the getting-started guide now use it — the last hand-wired
  `Listener`/`Connection`/`Peer` server recipe is gone.
- **RPC-level performance + soak evidence (v0.3.0 gate).** A new `bench-rpc`
  benchmark reports round-trip latency (p50/p99/max) + calls/sec and is wired
  into `bench-check` regression gating with a committed baseline (proven to go
  red on an intentional regression). The RPC soak harness (`tools/soak_rpc.zig`)
  now emits latency percentiles + a periodic memory-growth curve asserted flat
  within noise, and runs at ≥100 concurrent peers. This is the committed
  soak/regression evidence backing the two-party core's promotion to Stable.
- **Cross-impl `resolve_disembargo` de-SKIP + new conformance scenarios (E1/E2).**
  After the reflected-loopback return fix (below), the go-capnp and capnp-rpc
  embargo-client directions of `resolve_disembargo` are de-SKIPped and pass; the
  only remaining skip is `zig-client -> python-server` (pycapnp cannot host the
  reflecting server). The scenario additionally exercises **server-invokes-a-
  client-cap** (a capability passed in call params, invoked by the reflector) and
  **disconnect-mid-call** (the server closes its own transport mid-call, and every
  reference client asserts a disconnect-class error) across the reference matrix.

### Changed

- **Public API frozen and CI-gated (F1-F4 API freeze).** The Stable public
  surface is now pinned by `docs/api-snapshot.txt` (the Stable-only contract) and
  gated by `zig build check-api` — a diff is a reviewed breaking change, not an
  accident. The Experimental surface is tracked separately (ungated) in
  `docs/api-snapshot-experimental.txt`. As part of the freeze:
  - **F1** — narrowed leaked public error sets on the frozen entry points
    (`releaseImport`, the `sendCall*` family, `Connection.init` /
    `Connection.enableWake`) to named sets in `rpc/peer/errors.zig`. The five
    public user-callback typedefs (`CallBuildFn` / `QuestionCallback` /
    `CallHandler` / `SaveHandler` / `RestoreHandler`) intentionally **keep**
    `anyerror` — an application handler may fail any way it likes.
  - **F2** — `Peer.test_hooks` and the `rpc.testing` white-box internals moved
    off the frozen public surface behind an Internal facade; no Internal entry is
    reachable from `src/lib.zig`.
  - **F3** — canonicalized the entry-point shape: one public `Peer` constructor
    and one primary transport-attach (the `initDetached*` / `attachTransport*`
    variants are demoted to Experimental/deprecated aliases, not deleted), a
    `Connection.Options.default()` contract, and an opaque socket-handle wrapper
    so a raw POSIX handle is not baked into the frozen signature.
    `ClientSession.connect` / `ServerSession.accept` compile unchanged.
  - **F4** — the api-snapshot gate is scoped to a promoted-symbol allowlist that
    structurally excludes every Experimental symbol, so the Experimental surface
    can keep evolving post-tag without a false-red gate or an accidental freeze.

### Fixed

- **Echoed `Unimplemented(Disembargo)` now aborts the connection (W2, #1).** An
  echoed `Unimplemented(Disembargo)` was silently dropped, leaving the target
  import flagged `embargoed` with a retained `pending_embargoes` entry for the
  connection's life. It is now treated as the protocol violation it is: the peer
  sends an `Abort` and returns `error.EchoedDisembargoUnimplemented`, tearing the
  connection down. No stuck embargo state accumulates.
- **`hasKnownDisembargoTarget` validates exports-only (W3, #2).** Its
  `importedCap` arm accepted an export *or* import id; it now validates against
  the export table only — the exact id space the spec names on the
  `senderLoopback` path.

## [0.2.0] - 2026-07-04

The first tagged release. Serialization, codegen, and the `capnpc-zig` plugin
are Stable; the RPC runtime and transport are Experimental (may change at 0.x
minor bumps). See [`docs/supported-surface.md`](docs/supported-surface.md).

### Added

- **`docs/supported-surface.md`** — the authoritative v0.2.0 consumer contract:
  module choice (`capnpc-zig` vs `capnpc-zig-core`), stability tiers, the frozen
  `MessageValidationError` / `rpc.peer.CallError` error contract, declared RPC
  conformance (Level 1; Level-3 receive-only), and the documented known
  limitations.
- **Remote-dependency onboarding**: `zig fetch --save git+…#v0.2.0` documented in
  the README and `docs/build-integration.md`; `.paths` slimmed to the consumable
  package (`build.zig`, `build.zig.zon`, `src`, `README.md`, `LICENSE`) — no
  vendored submodules or dev trees in the published archive.
- **`rpc.transport.tcp.ClientSession`** — one-call TCP client lifecycle:
  `connect(gpa, io, address, options)` / `connectHost(gpa, io, host, port,
  options)` return a heap-owned session bundling the `Connection` and `Peer`
  (single allocation; `ClientSession.fromPeer` recovers the session inside
  generated callbacks). `run()` blocks on the read loop, `close()` is the
  idempotent graceful stop, `requestStop()` is the sole thread-safe abort,
  and `deinit()` encapsulates the one safe teardown ordering. Call deadlines
  are on by default (30s, 100ms tick) via `ConnectOptions
  .default_call_timeout_ms`; `Peer.adoptOwnerThread` /
  `Connection.adoptOwnerThread` legalize connect-on-A/run-on-B handoffs.
  `examples/rpc_pingpong.zig` and the e2e client now ride it — the two
  divergent hand-rolled teardown recipes are gone.

### Breaking

- **Nested interfaces are parent-qualified in generated code**: a nested
  interface now emits as `Parent.Inner` (with `Parent.Inner.Client` /
  `.Server` / `.VTable`) instead of flat at file scope by bare name. Two
  same-named nested interfaces under different parents — legal Cap'n Proto —
  previously collided (`error.DuplicateGeneratedName`) and failed to compile;
  they now generate distinct qualified types. File-scope interfaces are
  unaffected (byte-identical generated output). This is the last generated
  symbol-layout change before the v0.2.0 freeze.
- **`Message.validate` / `validateCounted` return a declared error set**:
  `message.Message.MessageValidationError` replaces the previous `anyerror`.
  The set is compiler-enforced complete and pinned by a test.
- **Serde manifest names and C export symbols are parent-qualified**: nested
  types render as `Parent.Child` in `CAPNP_SCHEMA_MANIFEST_JSON` and export
  `capnp_<module>_<parent>_<child>_to_json` (method param/result structs
  qualify under their interface), matching the scope-qualified generated
  code. Two same-simple-name types under different parents — legal in
  source — previously collided silently on one C symbol. A residual
  collision (identifier normalization folding two names together) now fails
  generation with `error.DuplicateSerdeExportSymbol` naming both types.

- **Generated client call methods take by-value receivers**:
  `Client.callX(self: Client, ...)`, `Client.callXPipelined(self: Client,
  ...)`, and `PipelinedClient.callX(self: PipelinedClient, ...)` (previously
  `*Client` / `*PipelinedClient`). Call sites no longer need the
  `var c = client;` copy ritual; `StreamClient` methods still take
  `*StreamClient` (they mutate stream state).
- **Generated `Response.unwrap()` / `BootstrapResponse.unwrap()`**: every
  per-method Response union and the BootstrapResponse union now carry an
  `unwrap()` that collapses the union into the success payload
  (`Results.Reader` / `Client`) or a typed `rpc.peer.CallError`
  (`RemoteException`, `Disconnected`, `CallTimedOut`, `Canceled`,
  `UnexpectedReturn`). The peer's exported locally-synthesized exception
  reasons (`disconnected_reason`, `shutdown_reason`, `deadline_reason`) map
  to their dedicated errors; any other exception is `RemoteException`.
  `rpc.peer.CallError` is the new shared error set (`src/rpc/peer/errors.zig`).
- **Generated `Client.release()`**: every generated Client can now release
  the import ref it owns (balances the bootstrap-return `retainCapability`);
  best-effort, at most once per owned Client.
- **Generated sibling schema imports are now `pub`**
  (`pub const chat = @import("chat.zig");`), so consumers can name
  cross-schema types the generated API returns.
- **`peer.start(cb_ctx, on_error, on_close)`** (landed earlier on this
  branch): both callbacks gained a leading user-context argument, threaded
  through connection error/close and nonfatal-error reporting;
  `HostPeer.start` mirrors the change. The deadline/shutdown exception
  reasons became exported constants (`deadline_reason`, `shutdown_reason`)
  alongside `disconnected_reason` so response unwrapping can match them.
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
- **Transport entry points take `SocketFd`** (`rpc.transport.tcp.SocketFd`,
  a thin named wrapper over `std.Io.net.Socket.Handle`): `Connection.init`,
  `Transport.init`/`initWithOptions`, `Listener.initFd`, and `closeFd` now
  take `.{ .handle = fd }` instead of a raw handle, and
  `Listener.listenHandle` returns the wrapper. The raw handle's width
  varies by target (i32 on POSIX, a pointer on Windows), which made the
  public surface — and the `check-api` snapshot gate — platform-dependent.

### Added

- **Windows TCP transport parity** (`docs/windows-first-class-plan.md`
  phase 1): ticks, idle reaping, and cross-thread wake now work on
  Windows. std's Windows sockets are raw AFD handles that winsock APIs
  (WSAPoll, setsockopt) reject, so the Windows run loop uses no readiness
  primitive: each blocking read runs as a cancellable `io.concurrent`
  task while the run thread waits on a timed `std.Io.Condition` for read
  completion, wake, or the tick timeout — callbacks all stay on the
  `run()` thread. The read is on an io worker (not a raw thread) so
  teardown can cancel it (`NtCancelIoFileEx` unblocks the pending AFD
  receive); a raw-thread read has no cancellation token and would wedge
  teardown for the kernel's multi-minute read timeout.
  `TCP_NODELAY` on Windows is a documented no-op until upstream exposes
  AFD socket options. The portable pair helper
  (`rpc.transport.tcp.createLoopbackSocketPair`) replaces POSIX-only
  socketpair test plumbing, the tick/idle, connection-failure, and
  raw-frame suites run on Windows (one documented Nagle-dependent skip),
  `zig build check-api` and a 2-second soak smoke run in every Test
  matrix OS, and the nightly soak gains a Windows lane.
- **Windows developer experience** (`docs/windows-first-class-plan.md`
  phase 3): the Justfile pins `windows-shell` to `sh` (Git Bash) so every
  recipe works unchanged; benchmarks use the Io-backed monotonic clock
  (no libc dependency) and compile for Windows; CONTRIBUTING gains a
  "Developing on Windows" quickstart.
- **`zig build e2e-self`** (`tools/e2e_self.zig`): no-docker self-interop
  e2e — the zig e2e client runs against the zig e2e server over real
  loopback TCP for all four schemas with TAP accounting, on every
  platform. Runs in each Test matrix OS in CI (Windows runners cannot run
  the Linux reference containers, so this is the Windows end-to-end
  socket-stack exercise) and locally via `just e2e-self`.
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

- **Generated `buildReturnDirect` no longer emits a zero-length cap table**:
  `peer.sendReturnResults` re-derives and rewrites the payload cap table
  unconditionally, so the placeholder was dead weight. No wire change.
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

[Unreleased]: https://github.com/nullstyle/capnp-zig/compare/v0.15.0...HEAD
[0.15.0]: https://github.com/nullstyle/capnp-zig/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/nullstyle/capnp-zig/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/nullstyle/capnp-zig/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/nullstyle/capnp-zig/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/nullstyle/capnp-zig/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/nullstyle/capnp-zig/compare/v0.9.0...v0.10.0
[0.9.0]: https://github.com/nullstyle/capnp-zig/compare/v0.8.0...v0.9.0
[0.8.0]: https://github.com/nullstyle/capnp-zig/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/nullstyle/capnp-zig/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/nullstyle/capnp-zig/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/nullstyle/capnp-zig/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/nullstyle/capnp-zig/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/nullstyle/capnp-zig/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nullstyle/capnp-zig/releases/tag/v0.2.0
