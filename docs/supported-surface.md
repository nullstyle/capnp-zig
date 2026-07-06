# Supported Surface (v0.3.0)

This is the authoritative statement of what `capnpc-zig` promises a consumer at
v0.3.0 — which modules are stable, what the RPC implementation conforms to, the
error contract, and the known limitations you are opting into. It is the single
source of truth; where other docs disagree, this file wins.

Pair it with [`stability.md`](stability.md) (per-module / per-platform maturity)
and [`CHANGELOG.md`](../CHANGELOG.md) (what changed).

## Semver at 0.x

The project is pre-1.0. Per semver, **0.x minor bumps may break the API** — and
they will, deliberately, for anything marked Experimental below. Pin an exact
version (`zig fetch --save …#v0.3.0`) and read the CHANGELOG before bumping.

- **Stable** (serialization + codegen + the two-party RPC core): the Stable
  surface is **frozen and CI-gated**. It is pinned by
  [`docs/api-snapshot.txt`](api-snapshot.txt) — the categorized Stable-only
  contract — and `zig build check-api` fails on any unreviewed drift. Breaking
  changes are avoided within 0.3.x and called out in the CHANGELOG when
  unavoidable.
- **Experimental** (L3 three-party origination, reflected-cap resolve, QUIC,
  persistence vat-restore, events, `io_backend`, the demoted transport/ctor
  variants): may break at any 0.x minor bump. Functional and tested, but the API
  is not frozen; its surface evolves in
  [`docs/api-snapshot-experimental.txt`](api-snapshot-experimental.txt) (ungated).

## Modules — which to import

The package exposes two library modules:

| Module | Contents | Import when |
|---|---|---|
| `capnpc-zig` | Full surface: serialization + codegen + RPC runtime + transport | You use RPC (`rpc.transport.tcp.connect`, generated interface clients/servers). |
| `capnpc-zig-core` | Serialization + codegen only; no RPC/transport in the module graph | You only read/write messages — keeps the RPC/transport code out of your build. |

Generated code imports the runtime as `@import("capnpc-zig")`; wire that name to
whichever module above you selected (see
[`build-integration.md`](build-integration.md)). This is the canonical rule —
earlier docs that mention only one name are being reconciled to point here.

## Stability by layer

| Layer | Tier |
|---|---|
| Wire format / packing (`message`) | **Stable** |
| Schema types + parsing (`schema`, `request`) | **Stable** |
| Code generation (`codegen`, the `capnpc-zig` plugin) | **Stable** |
| Reader convenience (`reader`) | **Stable** |
| RPC two-party core — frozen entry points (`rpc.wire.protocol` / `.framing`, `rpc.caps.table`, narrowed `Connection`, `ClientSession`, `ServerSession.accept`, the canonical two-party `Peer` surface + `CallError` / callback typedefs / `PeerLimits`, generated interface code) | **Stable** (frozen, CI-gated) |
| RPC L3 three-party origination, L4 Join runtime pilot/readiness, reflected-cap resolve (`resolvePromiseExportToImport`), `ServerSession`-as-a-type, `VatNetwork`, `JoinNetwork`, QUIC, persistence vat-restore, events, `io_backend`, demoted ctor/transport variants | **Experimental** |
| WASM host ABI (`src/wasm`) | **Experimental** |

The frozen Stable RPC surface is exactly the categorized set in
[`api-snapshot.txt`](api-snapshot.txt); the Experimental RPC surface is tracked
(ungated) in [`api-snapshot-experimental.txt`](api-snapshot-experimental.txt).
See [`stability.md`](stability.md) for the full matrix and per-platform status.

## Error contract

The one frozen public error set is validation:

- `message.Message.MessageValidationError` — the complete, compiler-enforced set
  `Message.validate` / `validateCounted` can return
  (`src/serialization/message.zig`). A returned error outside it fails to build,
  so the set stays exact.

For RPC, the frozen client-facing set is:

- `rpc.peer.CallError` = `{ RemoteException, Disconnected, CallTimedOut,
  Canceled, UnexpectedReturn }` (`src/rpc/peer/errors.zig`) — returned by the
  generated `Response.unwrap()` / `BootstrapResponse.unwrap()`. This is the
  contract for consuming a call result. In 0.3.0 it is part of the **frozen**
  Stable RPC surface (pinned in `api-snapshot.txt`).

**Deliberately open:** the public user-callback typedefs (`CallBuildFn` /
`QuestionCallback` / `CallHandler` / `SaveHandler` / `RestoreHandler`) keep
`anyerror` by design — an application handler may fail any way it likes; the F1
freeze intentionally left these open. Consume call results through `unwrap()` —
that is the stable, `CallError`-typed path.

## RPC conformance

capnpc-zig implements **Cap'n Proto RPC Level 1** — the two-party core:
bootstrap, calls, returns, finish, promise pipelining, and promise
resolution/embargo. In 0.3.0 this core is **Stable and frozen**: its public
entry points are the categorized Stable set in [`api-snapshot.txt`](api-snapshot.txt),
CI-gated by `zig build check-api`. It is proven bidirectionally against the C++,
Go, Python, and Rust reference implementations in the cross-implementation e2e
matrix (returned-capability invocation, typed pipelining with E-order, and
capability release).

**Performance + soak evidence.** The two-party core carries committed regression
evidence: `bench-rpc` gates round-trip latency (p50/p99/max) + calls/sec against
a committed baseline (`bench-check`), and the RPC soak harness reports latency
percentiles plus a memory-growth curve asserted flat at ≥100 concurrent peers.

The reflected-capability resolve/embargo handshake — a promise capability
resolved to a *caller-hosted* cap (`Peer.resolvePromiseExportToImport`), driving
the `senderLoopback`/`receiverLoopback` `Disembargo` — is exercised end to end by
the `resolve_disembargo` e2e scenario. As of 0.3.0 (W1) capnp-zig relays the
reflected call as a plain `sendResultsTo=caller` call and translates the results
straight back onto the caller's pipelined question, a shape **all four** reference
clients consume: the go-capnp and capnp-rpc client directions are de-SKIPped and
now pass. The scenario also exercises **server-invokes-a-client-cap** (cap in call
params) and **disconnect-mid-call** cross-impl (E2). The one remaining `SKIP` is
`zig-client → python-server`: pycapnp cannot host the reflecting server. The
matrix is asymmetric only because of that single reference-library gap, not
capnp-zig behavior — see Known limitations #4.

Note this cross-implementation matrix runs on a **local Docker host only** (Docker
Desktop / WSL2); hosted CI runners cannot run the Linux-container reference
matrix, so it is verified locally before each release. The Zig↔Zig self-interop
e2e (`zig build e2e-self`) does run in hosted CI on every push.

Beyond Level 1 (all **Experimental**, outside the frozen contract):

- **Level 2 (persistence):** Save/Restore SturdyRef hooks are present
  (`rpc.peer` persistence surface) and documented in
  [`rpc-persistence.md`](rpc-persistence.md). Current mainline evidence covers
  reconnect/resave, malformed params/results, Return send failures, callback
  failure after restored-cap retention, independent hook clearing, and allocator
  rollback for the persistence entry points. Experimental; the vat-level restore
  interface and realm conventions may still change.
- **Level 3 (three-party handoff):** a minimal **origination** slice has landed
  as of 0.3.0 (Experimental): `Peer.sendProvide` / `sendAccept`,
  `resolvePromiseExportToThirdParty`, `sendThirdPartyAnswer`,
  `registerPendingThirdPartyAwait`, a recipient auto-pickup seam
  (`setHandoffPickupHandler`), the `rpc.vat.network.VatNetwork` addressing seam
  (+ `LoopbackVatNetwork`), and `thirdPartyHosted` descriptor emission — in
  addition to the inbound `Provide` / `Accept` / `Join` / `ThirdPartyAnswer`
  handling that was already present. This arc is **lightly soaked and
  Experimental**. Main now has one positive cross-implementation proof:
  `just e2e-l3-cpp` runs a Zig↔C++ Level-3 handoff over real TCP against the
  C++ reference stack built from vendored Cap'n Proto 2.0, with Vat A accepting
  and invoking the hosted cap directly on its A↔C++ connection. This is
  C++-first evidence, not a full reference matrix: `just e2e-l3-go` now checks
  that vendored go-capnp exposes `Network3PH` hook names but still leaves the
  required runtime paths (`Accept`/`Provide`, `thirdPartyHosted` pickup with a
  network, `awaitFromThirdParty`, accept-context `Disembargo`, same-network
  locality) behind `TODO: 3PH` guards, and the current Rust/Python e2e adapters
  expose only the two-party path. The former gap — the introducer not
  forwarding the *original* parked pipelined calls on a promise to the host (they
  hit the Level-1/2 rejecting vine) — is now
  **RESOLVED**: when the handed-off promise resolves, the introducer forwards
  each parked pipelined call cross-peer to the capability host (targeting the
  provided cap on the introducer↔host connection) and relays the host's result
  back to complete the caller's original pipelined question, preserving e-order
  (spec `rpc.capnp:898` and the handoff ordering section: parked calls reach the
  host in send order and strictly before any post-pickup direct call) and the
  vine refcount/liveness coupling. On main, this forwarding also remaps
  cap-bearing params/results by minting cross-peer proxy exports, so caller-hosted
  param caps and host-returned result caps can be invoked through the relay. The
  current mainline hardening evidence also covers loopback token duplicate/unknown
  paths, `sendProvide` / `sendAccept` allocator rollback, vine/provide teardown
  ordering, embargoed pickup ordering, and cross-peer proxy cleanup. The C++
  interop lane is now a small failure matrix rather than only a happy-path proof:
  bad contact data falls back to the vine proxy, invalid/unknown completion tokens
  and await-side C++ rejection produce deterministic pickup exceptions, a C++
  disconnect after `Provide` still permits direct pickup, duplicate/late `Accept`
  is rejected without a second cap, hosted-cap exceptions report over the direct
  A↔C++ path, and every case asserts local Provide/Accept/vine/embargo state
  drains. Do not depend on the L3 surface for production interop without exact
  pins.
- **Level 4 (Join):** a guarded Zig↔Zig runtime pilot is present and
  documented in [`rpc-l4-join-readiness.md`](rpc-l4-join-readiness.md).
  `Peer.sendJoinExperimental` can originate raw Join parts, and inbound `Join`
  can collect local JoinKeyPart structs, compare resolved targets, return
  provided caps on the legacy raw-helper path, or, with an Experimental
  `rpc.vat.join.JoinNetwork` attached, return compact Zig JoinResult payloads
  that the joiner resolves into a direct `Accept` and callable cap.
  `JoinCoordinator` is the first Experimental Zig-shape helper above the raw
  sender: it sends compact key parts, collects matching JoinResults, sends
  direct Accept, retains/releases the accepted cap, and Finishes JoinResult
  questions on their originating peers after pickup; it tracks Finish state per
  JoinResult question so partial send failures retry only unfinished questions.
  It rejects duplicate local part numbers before sending and can cancel after
  JoinResults arrive, including a pending direct Accept whose Return is lost.
  Dropping a coordinator best-effort cancels pending Join and Accept questions
  before freeing its callback context. Transparent cross-peer proxy exports can
  relay Join requests to their source peer, relay downstream JoinResult/exception
  Returns upstream, and keep the downstream JoinResult alive until the upstream
  Finish.
  `AddressedJoinNetwork` adds an Experimental registry proof where
  application-supplied opaque addresses are carried in provision tokens,
  resolved through already-live registry entries, or resolved through an
  app-supplied connector for unknown addressed provisions.
  Regressions cover Finish/send-failure/OOM paths, pending direct-Accept
  rollback, coordinator duplicate-send rejection, post-JoinResult and
  post-Accept-send cancel cleanup, drop-time pending Join/Accept cancellation,
  partial-Finish retry without replaying successful Finishes, sendPart OOM
  rollback, addressed unknown/stale/duplicate provision handling, connector
  malformed-token/no-dial, network-teardown-before-release, and OOM-before-dial
  handling, retained result import release, mismatch/cancel cleanup, callback
  failure after retention, proxy relay success through the real coordinator,
  source unavailable, downstream send failure, owner/source
  teardown, target mismatch through relay, and relay setup OOM rollback. `just
  e2e-l4-zig` now runs a real Zig↔Zig TCP gate for the addressed
  JoinResult→Accept path. There is no Stable `Peer.sendJoin`, no production Join
  addressing policy or bundled dialer, no multi-hop relay beyond transparent
  proxy relay, and no cross-implementation L4 interop claim. The C++ L3 e2e lane
  includes shape probes plus a source-backed runtime-surface probe; it currently
  confirms that the C++ reference stack exposes no callable generic
  `VatNetwork` Join hook for this TCP harness.
  `just e2e-l3-go` confirms Go has generated Join/twoparty shapes but no
  runtime dispatch for `Message.join`. Experimental; exact-pin only.

## Known limitations (v0.3.0)

Each of these is a defined, non-corrupting behavior — safe to tag with, listed so
you know exactly what you are relying on. None is a leak/UAF/hang against a
cooperating peer.

The forwarded-return intermediary case that shipped as the one remaining active
v0.3.0 limitation is resolved on main; no active limitation is currently
documented for the frozen two-party surface. Historical resolved items are listed
below so release-to-release behavior changes stay auditable.

### Resolved since v0.3.0

- **Forwarded-return `takeFromOtherQuestion` / `resultsSentElsewhere` — RESOLVED.**
  When acting as a forwarding intermediary, capnp-zig now translates nested
  `takeFromOtherQuestion` IDs through the forwarded-question map in the
  caller-translation and propagation modes, and preserves `resultsSentElsewhere`
  markers instead of degrading to a clean exception
  (`src/rpc/peer/forward/peer_forwarded_return_logic.zig`). Reachable only in
  proxy topologies; standard two-party client/server calls were already
  unaffected.

### Resolved since v0.2.0

- **Echoed `Unimplemented(Disembargo)` (was #1) — RESOLVED (W2).** An echoed
  `Unimplemented(Disembargo)` was previously silently dropped, leaving the target
  import flagged `embargoed` with a retained `pending_embargoes` entry. It is now
  treated as the protocol violation it is: the peer sends an `Abort` ("peer echoed
  Disembargo as Unimplemented") and returns `error.EchoedDisembargoUnimplemented`,
  which tears the connection down (`src/rpc/peer/bootstrap.zig`,
  `src/rpc/peer/errors.zig`). No stuck embargo state can accumulate.

- **`hasKnownDisembargoTarget` over-accepts (was #2) — RESOLVED (W3).** The
  `importedCap` arm previously accepted an export *or* import id; it now validates
  against the export table only, the exact id space the spec names on the
  `senderLoopback` path (`src/rpc/peer/mod.zig`, `hasKnownDisembargoTarget`).

- **Reflected-loopback return interop (was #4) — RESOLVED (W1).** capnp-zig
  previously returned the caller's original question with an eager
  `takeFromOtherQuestion` redirect (forwarding the relayed call with
  `sendResultsTo=caller`); kj-capnp (C++) and pycapnp consumed it, but go-capnp
  could not parse `takeFromOtherQuestion` and capnp-rpc (Rust) stalled on it. It
  now relays the reflected call as a plain `sendResultsTo=caller` call and
  translates the real results straight back onto the caller's pipelined question
  as a plain `.results` return (internal `.translate_to_caller` forward mode,
  `src/rpc/peer/forward/peer_forward_orchestration.zig`) — a shape **all four**
  reference clients consume. The `resolve_disembargo` go/rust client skips were
  removed; the only remaining skip is `zig-client → python-server` (pycapnp cannot
  host the reflecting server). A former side effect — a *direct* (non-deferred)
  generated handler on a reflected-to cap not running its body on the
  `resultsSentElsewhere` path — no longer applies on the reflected loopback path,
  since results now flow as plain `.results`.
