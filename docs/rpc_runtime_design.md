# RPC Runtime Design

## Goals
- Full Cap'n Proto RPC protocol compliance (bootstrap, calls, returns, pipelining, capability transfer).
- Production-ready performance: low overhead, backpressure-aware, minimal allocations.
- Integration with the existing `src/serialization/message.zig` wire-format layer and codegen.
- Concurrent read/write I/O over `std.Io` with dedicated writer threads. The runtime is polymorphic over the concrete `std.Io` backend so `std.Io.Threaded`, `std.Io.Evented` where Zig exposes it, and the process-provided default share the same protocol code -- see `src/io_backend.zig`.

## Non-Goals (Initial Phase)
- TLS or authentication for the TCP transport (assume a trusted transport).
- Multi-transport multiplexing in a single connection.
- HTTP/WebSocket bridges.

## Architecture Overview
The runtime is organized into a small set of components, with strict ownership and lifetime rules:

- `rpc.transport.tcp.Runtime` (`src/rpc/transport/tcp/runtime.zig`): listener and socket helpers for creating TCP connections.
- `rpc.transport.tcp.Connection` (`src/rpc/transport/tcp/connection.zig`): per-transport state machine for framing, parsing, dispatch, and write scheduling.
- `rpc.transport.tcp.Transport` (`src/rpc/transport/tcp/stream_transport.zig`): concurrent read/write transport, handling blocking I/O and exposing buffers to `Connection`.
- `rpc.transport.quic.Connection` (`src/rpc/transport/quic/connection.zig`): optional quic-zig-backed QUIC vat session using ALPN `capnp-rpc/1`, with baseline stream mode by default and an opt-in native control/data-stream mode.
- `rpc.transport.quic.Server` (`src/rpc/transport/quic/server.zig`): optional multi-session QUIC fanout API.
- `rpc.wire.protocol` (`src/rpc/wire/protocol.zig`): Cap'n Proto RPC wire message definitions and parsing helpers.
- `rpc.caps.table` (`src/rpc/caps/table.zig` facade over `caps/lifecycle.zig`, `caps/inbound.zig`, `caps/outbound.zig`, and `caps/descriptors.zig`): export/import capability tracking with reference counting and lifetime management.
- `rpc.events` (`src/rpc/events.zig`): redacted observer events for connection lifecycle, frame movement, backpressure, resource rejection, protocol errors, and close.
- `rpc.peer.Peer` (`src/rpc/peer/mod.zig` + `src/rpc/peer/*`): public peer facade, state limits, inbound/outbound call orchestration, return handling, and lifecycle dispatch.
- `rpc.promises.pipeline` (`src/rpc/promises/pipeline.zig`, `src/rpc/promises/peer_promises.zig`): promised-answer transforms and queued pipelined-call replay.

All runtime types are single-threaded unless explicitly documented. Each connection uses a dedicated writer thread for outbound I/O and blocking reads on the main connection thread.

## Transport
Each connection uses a `Transport` with concurrent read/write I/O:
1. The read side performs blocking reads into a fixed buffer on the connection thread.
2. `Connection` consumes bytes into a framing parser.
3. Complete frames are parsed into RPC messages (Cap’n Proto message framing).
4. Parsed messages are dispatched to handlers.
5. Outbound messages are serialized and enqueued; a dedicated writer thread drains the write queue.

The TCP transport is the only place that should know about POSIX wake pipes,
`poll`, and socket options such as `TCP_NODELAY`. Those details currently live
under `src/rpc/transport/tcp/`; Evented-specific transport work should either
keep them behind that boundary or extract a very small platform shim there,
without touching peer/capability/promise semantics.

The explicit Evented backend is a supported compile-check path through
`just check-evented` (`zig build -Dio-backend=evented check`) on targets where
Zig exposes `std.Io.Evented`. This gate is intended to catch backend-selection
regressions without requiring a full RPC runtime execution path.

## QUIC Transport
QUIC is opt-in at the build-module boundary. Default builds expose
`rpc.transport.quic` as a disabled facade with QUIC-dependency-free framing
helpers and clear compile-time errors for transport construction. The
dependency remains declared in `build.zig.zon` for opt-in users, but
`build.zig` resolves it only when `-Dquic=true` selects the QUIC-enabled
library root and exposes the quic-zig-backed transport implementation.
For setup examples, mode selection, and production budget guidance, see
`docs/quic-transport.md`.

The default QUIC transport is intentionally conservative:

- ALPN is `capnp-rpc/1`.
- One QUIC connection represents one authenticated vat-to-vat RPC session.
- Client-initiated bidirectional stream 0 carries the baseline Cap'n Proto RPC message stream.
- Each RPC message is length-delimited with a 32-bit little-endian byte length, followed by the existing Cap'n Proto RPC message bytes.

This gives the existing peer/protocol layers the same complete-message callback shape as TCP while letting QUIC handle handshake, loss recovery, stream flow control, and connection migration. When QUIC is enabled, the implementation is exposed as `rpc.transport.quic.Connection` and uses the same `Peer.attachConnection` path as the TCP `rpc.transport.tcp.Connection`.

`rpc.transport.quic.TransportMode.native` keeps the same ALPN and vat session model but changes the QUIC stream layout while preserving standard `rpc.capnp` messages as the canonical RPC payload:

- Bidirectional stream 0 starts with a native preface plus versioned hello, then carries ordered control envelopes.
- Small RPC frames are sent inline in `inline_rpc` control envelopes.
- Larger RPC frames are written to one-shot unidirectional data streams and referenced by ordered `data_rpc` control envelopes.
- Inbound control envelopes are dispatched in strict control-stream order. If a `data_rpc` envelope is next but its referenced data stream is incomplete, later control envelopes remain buffered until the data frame is complete.
- QUIC DATAGRAM is intentionally unused for RPC or telemetry in this phase; sideband telemetry should be designed as a transport-general facility later.

## Framing and Parsing
- RPC messages are Cap’n Proto messages with standard segment framing (and optional packing in future).
- The `Connection` maintains a framing state machine: header parse -> segment sizes -> payload.
- Parsed messages are validated using `schema_validation.zig` where schema information is known (e.g. generated stubs).
- Malformed frames or protocol violations abort the connection.

## Capability Model
- Each connection maintains:
  - `exports`: server-side capabilities this peer can invoke.
  - `imports`: client-side capabilities received from the peer.
- Capabilities are represented by IDs and refcounts. The runtime sends `Release` when the refcount reaches zero.
- `AnyPointer` capability pointers are treated as interface pointers in schema validation and canonicalization.

## Persistence (Sturdy Refs, Level 2)

Spec level 2 (per the vendored `rpc.capnp` level definitions) is the ability
to save a capability into a `SturdyRef` that outlives the connection and to
restore it later. `Persistent.save()` is an ordinary interface call defined by
the vendored `persistent.capnp` (`@0xc8cb212fcd9f5691`); the `SturdyRef` and
`Owner` type parameters are realm-defined, and restore is deliberately
vat-specific in the modern spec (the old `Restore` message is obsolete — the
bootstrap interface took its place). capnp-zig's realm conventions, defined in
`src/rpc/peer/persistence.zig`:

- A sturdy ref is an opaque application-defined byte string. On the wire it
  travels as a `Data` pointer in the `SaveResults.sturdyRef` `AnyPointer`
  slot. `SaveParams.sealFor` is passed through to the save handler raw and
  uninterpreted.
- Restore is served as a call on the bootstrap capability using a vat-level
  `Restorer` convention interface
  (`restore @0 (sturdyRef :Data) -> (cap :Capability)`, ID
  `0xac47e3f6453b50f3`). Vats that don't install a restorer answer it like
  any other unknown interface on their bootstrap export.

Export side: `Peer.setPersistentExport(export_id, ctx, on_save)` marks an
existing export persistent. The peer swaps the export's stored handler for a
trampoline that answers `Persistent.save()` (and restore, on the bootstrap
export) and forwards every other interface to the original handler — so save
dispatches through the normal inbound-call path, including promised-answer
targets and queued-call replay. The save handler returns the sturdy-ref
payload (allocated from `peer.allocator`; the peer embeds and frees it).
`Peer.setRestorer(ctx, on_restore)` installs the vat restore hook on the
bootstrap export; the hook maps sturdy-ref bytes to `.existing` (re-expose an
already-registered export), `.host` (register a fresh export), or `.unknown`
(answer an "unknown sturdy ref" exception).

Client side, the documented reconnect flow:
1. Connect and `sendBootstrap` to import the remote's bootstrap capability.
2. `Peer.sendSave(import_id, ctx, callback)` on any imported capability; the
   callback receives the sturdy-ref bytes (borrowed from the Return frame).
   Persist them wherever the application likes.
3. After a disconnect, in-flight questions resolve through the existing
   deadline/cancel machinery (`checkDeadlines`, `cancelQuestion`).
4. Reconnect with a fresh peer, `sendBootstrap`, then
   `Peer.sendRestore(bootstrap_import_id, sturdy_ref, ctx, callback)`. The
   callback receives the restored capability already retained; resume calling
   it via `sendCall`.

See [`rpc-persistence.md`](rpc-persistence.md) for the consumer-facing flow and
current Experimental evidence.

Hardening follows the house pattern: the hook registry is budgeted by
`PeerLimits.max_persistent_exports` with an 80% pressure-event crossing
(`persistent_exports`), and single sturdy-ref payloads are bounded by
`PeerLimits.max_sturdy_ref_bytes` in both directions (save-handler output and
inbound restore params), emitting a `sturdy_ref_bytes` resource rejection on
violation. `PeerStats` gains `persistent_exports`, `saves_served`, and
`restores_served`.

## Retained Outbound-Answer Lifetimes

Ordinary outbound calls keep their historical automatic lifetime: after a
terminal `Return` is dispatched, the peer sends `Finish`. The Experimental
`rpc.peer.CallOptions` makes that policy explicit. Passing
`.result_lifetime = .retained` to a `sendCall*WithOptions` method suppresses
automatic Finish; generated interface `Client`, `PipelinedClient`, and
`callXxxPipelined` methods expose matching `WithOptions` forms. Their existing
methods delegate with `.automatic`, and streaming fire-and-forget calls remain
automatic.

Retained registration is an atomic pre-send step, including synchronous
loopback/transport Returns. The terminal callback runs at most once and makes
the answer caller-owned before callback dispatch. A post-visibility parse or
callback error is reported through the peer's non-fatal error hook rather than
hiding the question id behind a failed send result. The caller then invokes
`finishRetainedQuestion(question_id, release_result_caps)`. A transport failure
rolls the state back so the same Finish can be retried; an early, duplicate,
reentrant, or post-transfer Finish is rejected without changing ownership. A
Return carrying `noFinishNeeded` retires the record automatically. Cancellation,
terminal transport close, and peer teardown also remove local records.

Retained state is independently budgeted by
`PeerLimits.max_retained_questions` (default 1024) before a Call is emitted.
`PeerStats.retained_questions` counts caller-owned entries, including calls
still awaiting Return, while `transferred_retained_questions` counts entries
owned by another protocol lifecycle. The `retained_questions` event resource
reports redacted pressure crossings and `PeerLimitExceeded` rejections.

The first transfer owner is Level-3 Provide. Once the retained call has
returned, `sendProvideFromRetainedAnswer` accepts its question id and a
promised-answer op path; the combined
`resolvePromiseExportToThirdPartyFromRetainedAnswer` performs the same transfer
while resolving the recipient-facing promise. Setup is transactional until the
Provide commit: earlier failure restores caller ownership. Once the Provide may
have been delivered, a later combined Resolve failure conservatively consumes
the source into protocol cleanup instead of returning it to the caller. Success
couples the source answer to the vine and held-open Provide. Vine fallback calls target that promised-answer path,
and direct pickup redeems the same provision. When the coupling ends, both the
Provide and source questions are Finished exactly once. A failed
protocol-owned Finish remains marked and is retried immediately and from
`Peer.checkDeadlines()` maintenance; that retry does not change the method's
documented count of expired outbound calls.

## L3 VatC Parked-Accept Lifecycle

`ProvisionIndex` is the Experimental vat-wide rendezvous for a `Provide` and
`Accept` arriving on different peers. An unmatched Accept may park, but
admission is bounded twice: the vat-wide defaults remain 1024 entries / 256
KiB, while each enrolled peer defaults to 64 entries / 16 KiB. Byte admission
charges the normalized recipient token plus embargo bytes for every Accept,
even when multiple parks share one token.

Every parked record has one centralized retirement path. Adoption, Finish,
expiry, transport close, rollback, peer teardown, and index teardown detach the
holder/back-link before best-effort sends or callbacks, then refund peer and
index gauges exactly once. Transport close removes only that connection's
parked and embargo-queued holder state; an active provider-owned provision
survives so disconnect-after-Provide pickup keeps working. `detachTransport()`
is non-terminal. Bound transports notify this lifecycle automatically; a raw
frame-pump host must call idempotent `HostPeer.notifyTransportClosed()` once on
EOF, reset, or explicit terminal close.

The raw index leaves `park_ttl_ms = null`. The higher-level `Vat` defaults it
to 30 seconds and requires a custom `Options.clock` or value-stored
`Options.io`; a custom clock wins. Expiry uses an O(1) next-deadline check at
every inbound-frame boundary and from `Peer.checkDeadlines()`, followed by a
reentrancy-safe full sweep only when due. `now >= deadline` expires. Explicit
`ProvisionIndex.sweepExpiredParkedAccepts()` and `Vat`'s forwarding method
return the number detached; exception Returns remain best-effort. Since each
park stores an absolute deadline, `Vat.setClock()` rejects an effective clock
domain change while parks are live or a new deadline sample is in flight
(`error.ParkClockInUse`); identical-source calls remain no-ops.

Operability stays redacted: index/vat stats expose provision, attributable,
embargo, and queued gauges; `PeerStats` exposes only that peer's park count and
bytes; resource events use `parked_accepts` / `parked_accept_bytes`; and a
`parked_accept` timeout carries only the inbound answer ID.

## Automatic L3 Redirected Results

Inbound `Call.sendResultsTo = thirdParty` keeps the conservative
`ThirdPartyResultPolicy.reject` default. Two Experimental opt-ins sit above it:

- `.application` preserves the existing manual contract. The handler delivers
  the result out of band and calls `sendReturnResultsSentElsewhere()` itself.
- `.vat_network` uses the `VatNetwork` previously supplied through
  `attachVatNetwork()`. The runtime resolves `ThirdPartyToContact`, registers a
  synthetic callee-range answer on the introduced peer, and sends that peer a
  `ThirdPartyAnswer` before dispatching the application handler.

Under `.vat_network`, the handler still answers through ordinary
`sendReturnResults()` / `sendReturnException()` calls. A results payload is
built once in the source peer's capability-id space, then cloned onto the
result peer through pinned cross-peer proxies. The synthetic answer uses the
introduced peer's normal unresolved/resolved, terminal-Return, pipelining, and
Finish lifecycle. Once that terminal is committed, the source caller receives
the protocol-required `resultsSentElsewhere` marker.

Route ownership is source-side with a borrowed target backlink. Setup is
transactional: a failed ThirdPartyAnswer announcement rolls the route back; a
failed target result send retires it with one target exception; and a source
marker failure after target delivery cannot emit a second terminal Return.
An error reported after a ThirdPartyAnswer write is inherently ambiguous
because that frame has no acknowledgement; the transport must treat it as
terminal so an adopted recipient drains on connection close. A target result
write is considered committed after a trailing error only when a newly-created
reentrant Finish proves consumption. Early target Finish detaches the synthetic
answer from the source route without replaying the handler and fails any
already-pipelined children. Source/target deinit reentered from ThirdPartyAnswer,
result delivery, or the application handler is deferred until current route
borrows unwind. Transport close without deinit detaches the closing side before
its close callback. `detachVatNetwork()` affects future contact resolution
only; a route already established retains its target peer. All peers and the
network implementation remain single-thread-affine, and the network is
borrowed rather than owned by `Peer`.

Focused evidence covers scalar results, capability-bearing result remap and
source pins, pipelining before the terminal result, direct calls through the
returned proxy followed by Release, one handler dispatch, early target Finish,
missing-network refusal, reentrant source/target deinit, and complete route
drain. The allocation-index sweep checks every partial route/synthetic-answer/
proxy/pin state, and a separate case uses distinct network, source, and target
allocators. The focused redirected-return binary also covers pre-/post-delivery
frame-send failure boundaries and transport close on either peer without
deinit.

This is an automatic routing policy over an already attached application
network, not a production dialer, identity policy, authentication layer, or L4
Join implementation. Its runtime evidence is Zig-to-Zig only: the reference
implementations in this repository do not accept an inbound redirected-result
call, so no new cross-implementation claim follows from the feature.

## L4 Join Readiness

Level 4 `Join` remains Experimental. The runtime has receive-side Join state
machinery, a raw `Peer.sendJoinExperimental` sender, an Experimental
`JoinNetwork` seam for Zig `JoinResult` payloads, and an Experimental
`JoinCoordinator` that drives the compact Zig JoinResult→Accept workflow. There
is still no Stable `Peer.sendJoin`, no stable key/result format, and no
cross-implementation L4 runtime claim. Inbound `Join` messages use the shared
`ProvideTarget` representation: each part resolves its target, stores that
target under `pending_joins`, and records a question-to-part back-link in
`pending_join_questions`. When all parts for a join ID arrive, matching targets
return either the legacy direct-cap pilot result or, with `JoinNetwork`
attached, a compact Zig `JoinResult` that the coordinator can resolve into a
direct `Accept`.

Cleanup follows the same question lifecycle as Provide/Accept: `Finish` clears
the matching Join part, deinitializes its target, and removes the join bucket
when it becomes empty. Fresh join-bucket insertion is rollback-safe under OOM,
and completion drains `pending_joins` / `pending_join_questions` before fan-out.
For the JoinResult path, the coordinator records the peer for every originated
part, rejects duplicate local part numbers, and Finishes each JoinResult
question on that same peer after the direct Accept succeeds. Finish state is
tracked per question, so a partial send failure can be retried without replaying
already-sent Finishes. Malformed, exception, or unexpected JoinResult Returns
are terminal: the coordinator records failure state, sends the affected
question's Finish, and lets the generic Return dispatcher remove the question
instead of restoring it or leaving cleanup to a later cancel path.
If the direct Accept returns an exception, malformed result, or other terminal
Return, the coordinator still Finishes those JoinResult questions because the
host-side pending provision has already been consumed. That releases the
host-side pending Accept provision even when the JoinResults arrived through
multiple proxy paths, and it drops the retained `Joined` inputs immediately.
Dropping the coordinator
best-effort cancels outstanding Join and direct Accept questions first, leaving
cancelled question entries to absorb late Returns without calling back into
freed coordinator state.

Transparent cross-peer proxy exports can relay inbound `Join` requests to their
source peer. The relay keeps an owner answer -> source question record plus a
source-peer back-link until the upstream caller sends `Finish`; only source
targets that are already imports on that source peer are forwarded as downstream
Join targets. If forwarding that downstream Finish fails, both records stay live
so a later retry can drain the downstream lifetime instead of silently losing
the cancellation edge. During owner teardown there is no retrying owner left, so
a failed best-effort downstream Finish still neutralizes the source question
locally before the owner state is freed. If the downstream results or exception
Return cannot be relayed upstream, or if
the downstream peer sends an unexpected Return tag, the relay sends a downstream
Finish/cancel and drains both records before reporting a fallback exception
upstream.
See [`rpc-l4-join-readiness.md`](rpc-l4-join-readiness.md) for the current
evidence and limitations.

## Call Flow
Inbound call:
1. `Call` message parsed with target capability ID and method.
2. `Peer` dispatch logic locates server implementation and invokes it.
3. Results are serialized into a `Return` message.
4. Exceptions map to `Return` with an error payload.

Outbound call:
1. Generated client stubs/peer helpers allocate a `QuestionId` and build params.
2. `Connection` sends `Call` and tracks outstanding question state.
3. `Return` resolves the promise and releases temporary capabilities.

## Concurrency and Scheduling
- `Connection.run()` is a blocking read loop on the owner thread; a writer
  thread drains the outbound queue, and on Windows a reader-thread bridge
  (`WinReadBridge`) stands in for `poll(2)` because std's sockets are AFD
  handles.
- `Peer` and `Connection` are single-thread-affine by contract (debug-checked
  panics; `adoptOwnerThread` re-captures affinity at quiescent handoffs). All
  user handlers and callbacks run inside `run()` on that thread.
- The only thread-safe entry points are `Connection.wake`/`requestClose` (and
  `ClientSession.requestStop`, which wraps them).

## Error Handling
- Protocol errors close the connection and fail all in-flight questions.
- Transport errors propagate to `Connection` and trigger cleanup.
- Application errors are serialized as RPC exceptions.

### Terminal question failure on disconnect

When the transport closes (EOF, transport error, or explicit close), every
still-outstanding outbound question is failed so a caller awaiting a `Return`
is resolved rather than hung. The chosen signal is a **synthetic exception
`Return`** (`reason = "disconnected"`, exported as `peer.disconnected_reason`)
delivered through the question's normal `QuestionCallback` (`on_return`) — the
lowest-churn shape, since `QuestionCallback` already receives a
`protocol.Return`, and it reuses the existing `deliverLocalException` /
`forceCancelAllQuestions` path used by the shutdown drain.

Contract:
- Delivered from `Peer.onConnectionClose` **before** the owner's `on_close`
  callback runs, while the peer's maps are still intact — so a callback that
  re-enters the peer is safe and waiters are resolved before any teardown.
- Delivered **exactly once** per question (the question is removed from the
  table as it is failed; `on_close`/`deinit` then see an empty table).
- The synthetic failure is **never** recorded in `resolved_answers` and is
  **never** replayed to pipelining/`pending_promises` — it is purely an
  outbound-question terminal signal, distinct from an inbound answer.
- Direct `Peer.deinit` without a prior transport close delivers the same
  terminal signal: its first act is `forceCancelAllQuestions("disconnected")`,
  run while all maps are intact, so every outstanding question's callback
  fires exactly once (delivery transfers ctx ownership to the callback; if
  synthesis fails before the callback can run, the question's `deinit_ctx`
  frees the ctx instead). After a transport close this pass is a no-op —
  `onConnectionClose` already delivered the terminals. Questions parked in
  `pending_third_party_awaits` are the documented exclusion: they receive
  `deinit_ctx`-only cleanup (forward/save contexts, not user return
  callbacks).
- Peer lifecycle callbacks carry a user context: `start(cb_ctx, on_error,
  on_close)` stores `cb_ctx` and passes it as the leading argument of both
  callbacks, so owners no longer need globals to reach their state.

## Current Module Layout
- `src/rpc/transport/tcp/runtime.zig`
- `src/rpc/transport/tcp/connection.zig`
- `src/rpc/transport/tcp/stream_transport.zig`
- `src/rpc/wire/protocol.zig`
- `src/rpc/caps/table.zig`
- `src/rpc/caps/descriptors.zig`
- `src/rpc/caps/lifecycle.zig`
- `src/rpc/caps/inbound.zig`
- `src/rpc/caps/outbound.zig`
- `src/rpc/caps/payload_remap.zig`
- `src/rpc/wire/framing.zig`
- `src/rpc/promises/pipeline.zig`
- `src/rpc/promises/peer_promises.zig`
- `src/rpc/peer/mod.zig`
- `src/rpc/peer/state.zig`
- `src/rpc/peer/errors.zig`
- `src/rpc/peer/transport.zig`
- `src/rpc/peer/dispatch.zig`
- `src/rpc/peer/bootstrap.zig`
- `src/rpc/peer/finish.zig`
- `src/rpc/peer/resolve.zig`
- `src/rpc/peer/disembargo.zig`
- `src/rpc/peer/return/*`
- `src/rpc/peer/forward/*`
- `src/rpc/peer/provide/*`
- `src/rpc/peer/third_party/*`
- `src/rpc/events.zig`

## Test Plan
- Unit tests for framing and state machines.
- The seven focused Level-3 handoff binaries through `zig build test-rpc-l3`,
  with VatC also present in resource-budget, OOM, ReleaseSafe, and ReleaseFast
  hardening lanes.
- Loopback tests with in-process client/server.
- Interop tests against reference backends in the canonical `tests/e2e` harness.
- Documentation/API smoke checks through `zig build docs-smoke` and snippet
  fixture compilation through `zig build test-docs-snippets`.

## Resolved Design Questions
- Cap'n Proto RPC protocol types map to generated Zig types via the checked-in
  `src/rpc/gen/capnp/rpc.zig` (generated from the canonical
  `src/rpc/capnp/rpc.capnp` by our own plugin).
- Pipelined call ergonomics ship as generated `callXPipelined` /
  `XPipeline.getService()` / `PipelinedClient.callY` stubs (runtime-verified
  by `tests/rpc/integration/rpc_typed_pipelining_test.zig`).
- Packed RPC streams remain unsupported; the wire format's packing is
  serialization-only for now.
