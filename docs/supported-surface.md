# Supported Surface (v0.2.0)

This is the authoritative statement of what `capnpc-zig` promises a consumer at
v0.2.0 — which modules are stable, what the RPC implementation conforms to, the
error contract, and the known limitations you are opting into. It is the single
source of truth; where other docs disagree, this file wins.

Pair it with [`stability.md`](stability.md) (per-module / per-platform maturity)
and [`CHANGELOG.md`](../CHANGELOG.md) (what changed).

## Semver at 0.x

The project is pre-1.0. Per semver, **0.x minor bumps may break the API** — and
they will, deliberately, for anything marked Experimental below. Pin an exact
version (`zig fetch --save …#v0.2.0`) and read the CHANGELOG before bumping.

- **Stable** (serialization + codegen): breaking changes are avoided within 0.2.x
  and called out in the CHANGELOG when unavoidable.
- **Experimental** (RPC runtime + transport): may break at any 0.x minor bump
  (0.2 → 0.3). Functional and tested, but the API is not frozen.

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
| RPC runtime (`rpc.peer`, `rpc.transport`, generated interface code) | **Experimental** |
| WASM host ABI (`src/wasm`) | **Experimental** |

See [`stability.md`](stability.md) for the full matrix and per-platform status.

## Error contract

The one frozen public error set is validation:

- `message.Message.MessageValidationError` — the complete, compiler-enforced set
  `Message.validate` / `validateCounted` can return
  (`src/serialization/message.zig`). A returned error outside it fails to build,
  so the set stays exact.

For RPC, the frozen client-facing set is:

- `rpc.peer.CallError` = `{ RemoteException, Disconnected, CallTimedOut,
  Canceled, UnexpectedReturn }` (`src/rpc/peer/errors.zig:63`) — returned by the
  generated `Response.unwrap()` / `BootstrapResponse.unwrap()`
  (`src/capnpc-zig/generator.zig:1547,1765`). This is the contract for consuming
  a call result and is stable in shape.

**Not frozen:** the *inferred* error sets on `callX` / `sendCall` / other `Peer`
methods (they fan into deep dispatch stacks and render by-reference in the API
snapshot). `Peer` is Experimental-tier; these may narrow in a later 0.x. Consume
results through `unwrap()` — that is the stable path.

## RPC conformance

capnpc-zig implements **Cap'n Proto RPC Level 1** — the two-party core:
bootstrap, calls, returns, finish, promise pipelining, and promise
resolution/embargo. This is proven bidirectionally against the C++, Go, Python,
and Rust reference implementations in the cross-implementation e2e matrix
(returned-capability invocation, typed pipelining with E-order, and capability
release).

The reflected-capability resolve/embargo handshake — a promise capability
resolved to a *caller-hosted* cap (`Peer.resolvePromiseExportToImport`), driving
the `senderLoopback`/`receiverLoopback` `Disembargo` — is exercised end to end by
the `resolve_disembargo` e2e scenario. capnp-zig plays both roles: it originates
the reflection (server) against C++ and Python clients, and receives-and-embargoes
(client) against C++, Go, and Rust servers. The matrix is asymmetric only because
of reference-library gaps, not capnp-zig behavior — see Known limitations #4.

Beyond Level 1:

- **Level 2 (persistence):** Save/Restore SturdyRef hooks are present
  (`rpc.peer` persistence surface). Experimental.
- **Level 3 (three-party handoff):** **receive-only, no origination.** The peer
  handles *inbound* `Provide` / `Accept` / `Join` / `ThirdPartyAnswer`
  (`src/rpc/peer/mod.zig:3651,3687,3707,3752`) but never *originates* them —
  there is no `sendProvide` / `sendAccept` / `sendJoin`, and no outbound
  `thirdPartyHosted` descriptor. The runtime is architecturally two-party. A
  full Level-3 initiator (VatNetwork) is out of scope for 0.2.0.

## Known limitations (v0.2.0)

Each of these is a defined, non-corrupting behavior — safe to tag with, listed so
you know exactly what you are relying on. None is a leak/UAF/hang against a
cooperating peer.

1. **Echoed `Unimplemented(Disembargo)`** — if a peer that cannot parse our
   `Disembargo` echoes it back as `Unimplemented`, we do not clear the associated
   embargo state: the target import stays flagged `embargoed` and its
   `pending_embargoes` entry is retained for the connection's life
   (`src/rpc/peer/bootstrap.zig:21-30`, `src/rpc/peer/mod.zig:1924-1928`). Bounded
   (one entry per stuck disembargo), reachable only against a broken/hostile Level-0
   peer, no corruption or hang. (Echoed `Provide`/`Accept` are unreachable — no
   sender emits that state.)

2. **`hasKnownDisembargoTarget` over-accepts** — it treats a target as known if it
   matches *either* an export or an import id (`src/rpc/peer/mod.zig:3564`), rather
   than the exact origin the spec implies. Side-effect-free (a control-flow echo
   that allocates nothing and mutates no refcount); the laxity is not exploitable.

3. **Forwarded-return `takeFromOtherQuestion` / `resultsSentElsewhere`** — when
   acting as a forwarding intermediary, these two Return modes are not fully
   translated and degrade to a clean exception Return rather than corrupt state
   (`src/rpc/peer/forward/peer_forwarded_return_logic.zig:90-96`). Reachable only in
   a proxy topology no two-party deployment exercises.

4. **Reflected-loopback return interop (`takeFromOtherQuestion`)** — when capnp-zig
   resolves a promise to a *caller-hosted* capability and relays the caller's parked
   pipelined calls back to it, it returns the original question with
   `takeFromOtherQuestion` and forwards the relayed call with `sendResultsTo=caller`
   (`src/rpc/peer/forward/peer_forward_orchestration.zig`). This is protocol-correct
   and consumed transparently by kj-capnp (C++) and pycapnp, but two reference
   *clients* cannot follow it: go-capnp does not parse a `takeFromOtherQuestion`
   return at all, and capnp-rpc (Rust) parses it but only completes the call when the
   forwarded call used `sendResultsTo=yourself`, so it stalls. Reachable only in the
   reflected-loopback topology (a peer pipelining on a promise that resolves to its
   own cap); the `resolve_disembargo` e2e scenario skips those two reference-client
   directions (recorded as `SKIP`, not failures). A related consequence: a *direct*
   (non-deferred) generated handler on a reflected-to cap does not run its body,
   because the results-build closure is skipped on the `resultsSentElsewhere` path —
   use a deferred handler for caps that may be reflected to. Emitting
   `sendResultsTo=yourself` for the relayed loopback call would run direct handlers,
   deliver results inline, and broaden client interop; tracked separately.

These are tracked as the top targets for the post-tag conformance push; they do
not affect correctness for a standard two-party client/server.
