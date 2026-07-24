# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Documentation

- Fixed uncompilable serialization snippets in the README and the serialization
  getting-started guide: `Message.init` / `initPacked` take a
  `ValidationOptions` argument (`.{}` for defaults), and a scalar setter
  returns an error union (`try person.setAge(30)`).
- Corrected the `build-integration.md` generated-file path: `-ozig:gen` writes
  under `gen/` preserving the schema path (`schema/addressbook.capnp` ->
  `gen/schema/addressbook.zig`); the module now imports the right file, and its
  snippet test asserts the output-dir-prefixed convention.

### Fixed

- **CI compile-gates the Experimental L3/L4 e2e drivers.** They previously
  compiled only inside their own run-only steps, which no per-push job invokes,
  so their compile rot went unnoticed. `e2e-l4-zig` now joins the cross-target
  `check-compile`; the Zig/C++ L3 driver (`e2e-l3-cpp`), whose TCP rendezvous
  uses posix `poll` that std does not wire for Windows on the current
  toolchain, joins the native `check` step instead of the Windows cross-target
  matrix. (Wiring the L4 driver into the cross-target gate immediately caught a
  latent Windows-incompatibility in the L3 driver, which is why it is
  native-gated.)

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

- **Codegen: group-typed union members now guard on the discriminant.** A
  union member whose type is a group generated a getter with no `which()`
  check — reading it while a sibling variant was selected silently
  reinterpreted the sibling's bits. Group union-member getters are now fallible
  (`!Group.Reader`) and return `error.WrongUnionMember` when a different variant
  is selected, matching the guard slot union-members already had. Plain
  (non-union) groups keep their infallible getter. This is a source-visible
  change to generated code for schemas with group union members: such getters
  now require `try`.

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

[Unreleased]: https://github.com/nullstyle/capnp-zig/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/nullstyle/capnp-zig/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nullstyle/capnp-zig/releases/tag/v0.2.0
