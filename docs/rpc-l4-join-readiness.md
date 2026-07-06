# RPC L4 Join Readiness

Status: Experimental, Zig↔Zig JoinResult runtime pilot with addressed
registry/connector proof, transparent proxy relay, raw origination, and
receive-side readiness.

`capnp-zig` has a guarded slice of Cap'n Proto RPC Level 4 `Join`: inbound
Join state handling, a low-level Experimental sender for raw Join parts, and a
Zig-only `JoinResult` → direct `Accept` runtime path behind an Experimental
`JoinNetwork` seam. Transparent cross-peer proxy exports can now relay Join
requests to their source peer and hold the downstream JoinResult lifetime until
the upstream caller sends Finish. The addressed pilot can resolve an already
registered direct peer or call an application-supplied connector for the opaque
address in the provision token. This is not a complete L4 implementation and is
not part of the Stable surface. It exists to keep the provide/join state model
correct while L3 handoff grows toward cross-implementation use.

## What Exists

- The wire layer can encode and decode `Message.join`.
- `Peer.sendJoinExperimental` can originate a raw Join question with a caller
  supplied `Join.keyPart` AnyPointer and ordinary `Return` callback. It is a
  manual probe helper, not a high-level E-join API.
- `rpc.peer.JoinCoordinator` is an Experimental Zig-shape coordinator above the
  raw sender. It originates compact Join key parts, collects matching
  `JoinResult`s through a `JoinNetwork`, sends the direct `Accept`, retains the
  accepted cap, and Finishes each JoinResult question on the peer where that
  Join part was sent. It rejects duplicate local part numbers before sending,
  rejects new parts after Accept/cancel begins, tracks Finish state per
  JoinResult question, and can cancel both pending JoinResult lifetimes and a
  pending direct Accept question. Dropping the coordinator is also a best-effort
  cancel path for pending Join and Accept questions, so late Returns are
  absorbed by cancelled question entries instead of targeting freed coordinator
  state. It is still not a Stable E-join API or cross-implementation key/result
  format.
- `rpc.vat.join.JoinNetwork` is an Experimental L4 addressing seam. The
  `LoopbackJoinNetwork` test implementation maps completed Join results to the
  joiner's direct peer and an opaque `Accept.provision` payload.
- `JoinNetwork.hostJoinResult()` and `JoinNetwork.connectJoined()` take an
  explicit allocator for returned caller-owned buffers. The network may keep
  registry keys and connector lease state in its own allocator, but the returned
  `HostJoinResult` and `Joined.provision` buffers are freed by the caller with
  the allocator it supplied.
- `rpc.vat.join.AddressedJoinNetwork` is an Experimental addressed-registry
  implementation for the Zig pilot. Callers register a host peer with an opaque
  application address and an already-live direct peer. Joiners can also install
  `setAddressConnector()` so unknown addressed provisions are parsed, resolved
  by the application connector, cached for the lifetime of returned `Joined`
  handles, and drained when those handles are released. The generated provision
  token carries that address plus a nonce; it is still a registry/connector
  proof, not a bundled production dialer or stable address format.
- `Peer.handleJoin` accepts inbound `Join` messages and resolves their
  `target` through the same target machinery used by `Provide`.
- Join parts are collected in `pending_joins`, with question-to-part back-links
  in `pending_join_questions`.
- When all parts for a join ID arrive, the peer compares the resolved targets.
  Without a `JoinNetwork`, matching targets still receive the legacy raw-pilot
  results Return carrying the provided target. With a `JoinNetwork`, matching
  targets receive compact Zig `JoinResult` payloads, and the host stores a
  one-shot pending direct Accept in `pending_join_accepts`.
- A joiner can resolve every Zig `JoinResult`, validate that the results agree,
  send `Accept` on the direct peer, receive the accepted cap, and invoke it on
  that direct peer.
- An inbound `Join` targeting an exported transparent `CrossPeerProxyContext`
  relays to the proxy source peer using the same `keyPart`. Successful
  downstream JoinResult Returns are relayed back upstream, while the downstream
  Join question remains live until the upstream caller sends Finish.
- Relay state is tracked by upstream answer id and paired with source-peer
  back-links. Owner-peer-first teardown sends downstream Finish and neutralizes
  late Returns; source-peer-first teardown nulls owner back-links before the
  source peer is freed.
- `Finish` for an outstanding Join question clears the matching part, releases
  its target payload, and drops the join bucket when it becomes empty.
- Return send failure while completing either direct-cap Join Returns or
  JoinResult Returns degrades each affected question to an exception Return and
  drains the pending maps. If no JoinResult Return is successfully sent, the
  pending direct Accept provision is rolled back.

The current local JoinKeyPart convention is an `AnyPointer` to a one-data-word
struct:

- bytes 0..3: `join_id : UInt32`
- bytes 4..5: `part_count : UInt16`
- bytes 6..7: `part_num : UInt16`

That convention is internal and Experimental. It is enough to prove state
ordering, cleanup, and target equality behavior; it is not a frozen public key
format.

The current Zig JoinResult convention is an `AnyPointer` to a one-data-word,
one-pointer struct:

- bytes 0..3: `join_id : UInt32`
- byte 4 bit 0: `succeeded : Bool`
- pointer 0: `provision : Data`, a serialized `Accept.provision` AnyPointer
  message

That convention is also internal and Experimental. It proves the runtime shape:
Join returns data needed to form the direct pickup, and the final capability
arrives only after a follow-up `Accept`.

The C++ L3 interop lane uses the same struct shape in its test schema
(`JoinKeyPart { joinId :UInt32, partCount :UInt16, partNum :UInt16 }`) and a
twoparty-style `JoinResult { joinId :UInt32, succeeded :Bool, cap :Capability }`
fixture. The Zig orchestrator decodes both shapes as compatibility data-shape
checks. This is not a full C++ L4 Join runtime.

## Invariants

- A Join question ID cannot collide with an active inbound answer, active
  `Provide`, or existing pending Join question.
- Duplicate parts and part-count mismatches do not replace already-recorded
  parts.
- The Experimental `JoinCoordinator` records local part numbers before accepting
  more work, so duplicate local sends do not put a second Join frame on the
  wire.
- A fresh join bucket is rollback-safe: if allocation fails after the bucket is
  created but before the first part is fully indexed, the empty bucket is removed.
- `pending_joins` and `pending_join_questions` drain together on successful
  completion, mismatch completion, Finish cleanup, send-failure fallback, and
  peer deinit.
- `pending_join_accepts` drains on direct Accept success, JoinResult send
  rollback when no result reached the joiner, fallback exception send failure
  before any JoinResult is delivered, and peer deinit.
- Pending direct-Accept provisions are cloned into the Accept peer's allocator
  before insertion, so a Join completed on one peer can safely transfer a
  promised target to a different Accept-host peer.
- `pending_join_relays` and `cross_peer_join_relay_links` drain together on
  upstream Finish, downstream exception, owner/source teardown, downstream send
  failure, and allocation rollback. If relaying the downstream Finish fails,
  the owner relay record and source back-link remain live so a later upstream
  Finish retry can drain the downstream lifetime.
- Coordinator-originated joins split across multiple origin peers Finish or
  cancel each JoinResult on the peer that originated that part, preserving relay
  lifetime across A→B/A→C-style proxy paths.
- Coordinator Finish retries are per-question: if one peer's Finish send fails,
  later cleanup retries only the unfinished JoinResult questions instead of
  replaying already-sent Finishes.
- Malformed or exception JoinResult Returns are terminal for the affected
  coordinator question: the coordinator records failure state, sends Finish,
  cancels the aggregate Join, releases any retained `Joined` leases, and does
  not let the generic Return dispatcher restore or defer the callback question.
- Mismatched successful JoinResults are terminal for the coordinator: the
  coordinator records the mismatch, attempts to Finish each held JoinResult
  lifetime, keeps failed Finish sends retryable, releases retained `Joined`
  leases, and does not send a direct Accept.
- A direct Accept exception is terminal for the coordinator's JoinResults: the
  coordinator records the Accept failure and still Finishes every JoinResult
  lifetime because the host-side provision has already been consumed.
- Coordinator deinit cancels outstanding Join questions and pending direct
  Accept questions before freeing the coordinator, leaving only cancelled
  question entries that absorb the peer's required late Return.
- `pending_join_result_answers` records the peer that hosts the final direct
  Accept provision. Cross-peer Accept-host back-links prevent stale pointers if
  the Accept host deinitializes before the JoinResult answers Finish.
- Target ownership is single-owner: once a part is inserted, the Join state owns
  its `ProvideTarget`; rejected or failed inserts return ownership to the caller
  for cleanup. A completed JoinResult path clones the target into
  `pending_join_accepts`, and the pending Accept state owns that clone until
  Accept success or rollback.

## Current Evidence

Focused peer regressions in `tests/rpc/peer/rpc_join_readiness_test.zig` cover:

- Zig-originated two-part Join against another Zig peer, importing the returned
  cap and successfully invoking it,
- Zig-originated mismatch Returns delivered as exceptions,
- allocation-failure rollback for `sendJoinExperimental`,
- Zig-originated two-part Join with `JoinNetwork` attached, where Join Returns
  carry compact Zig `JoinResult`s, the joiner resolves them to a direct peer,
  sends `Accept`, imports the accepted cap, and invokes it,
- `JoinCoordinator` driving that JoinResult→Accept flow end to end, including
  automatic JoinResult Finish after Accept, accepted-cap release, and sender OOM
  rollback before recording stale question ids,
- `JoinCoordinator.cancelPending()` after JoinResults have arrived but before
  Accept, proving remote pending direct-Accept state and local joined leases
  drain,
- `JoinCoordinator` duplicate local part rejection before sending a second wire
  Join,
- malformed/exception `JoinCoordinator` JoinResult Return cleanup, proving the
  question is removed and Finished instead of restored or deferred after
  callback handling,
- malformed `JoinCoordinator` cleanup after a successful addressed JoinResult,
  proving retained connector leases are released and all JoinResult lifetimes
  are Finished when the aggregate Join becomes impossible,
- mismatched successful `JoinCoordinator` JoinResults, proving `acceptFirst()`
  releases retained addressed connector leases, sends Finish for held JoinResult
  lifetimes, and keeps failed Finish sends retryable before returning
  `JoinResultMismatch`,
- `JoinCoordinator.cancelPending()` after direct Accept is sent but its Return is
  lost, proving the pending Accept question observes a local cancel exception
  while JoinResult lifetimes drain,
- `JoinCoordinator.deinit()` with an outstanding Join question and with a
  pending direct Accept whose Return is lost, proving drop-time cleanup
  neutralizes callbacks before the coordinator memory goes away,
- partial `JoinCoordinator.finishJoinResults()` failure and retry, proving a
  successful Finish is not resent while the failed peer is retried,
- direct Accept results-send failure on the host, where the coordinator receives
  the fallback exception and still drains JoinResult answers/provisions,
- JoinResult Return send failure rollback, proving no stale
  `pending_join_accepts` entry remains when all JoinResult Returns fail, even if
  the fallback exception Return send also fails before any JoinResult is
  delivered,
- distinct Join-host and Accept-host allocators for a promised target, proving
  pending direct-Accept target/provision ownership follows the Accept peer and
  caller-owned `JoinNetwork` results are freed with the supplied allocator,
- transparent proxy Join relay where A joins two caps through B/C proxy exports
  that forward to the same D-hosted cap, receives JoinResults through the real
  `JoinCoordinator`, sends direct Accept on A↔D, invokes the accepted cap
  directly, and automatically drains relay state by Finishing each upstream
  JoinResult on the correct A→B/A→C peer after pickup,
- relay failure/lifecycle cases: upstream Finish before downstream Return,
  failed downstream Finish retry, source unavailable, downstream Join send
  failure, unsupported source-target rejection, downstream results/exception
  Return relay failure, unexpected downstream Return cleanup, owner-peer-first
  teardown including downstream Finish send failure, source-peer-first teardown
  before and after downstream Return, target mismatch through the relay, and OOM
  rollback around relay setup,
- test-local coordinator selection of a callable cap across two returned Join
  parts, including release of the retained result imports,
- test-local coordinator mismatch handling with no retained joined caps,
- canceling a partial coordinator Join, which sends Finish and drains remote
  pending Join state while the local canceled question absorbs the late Return,
- callback failure after joined-cap retention, proving the retained import is
  still visible and releasable while the question table drains,
- matching two-part Join completion,
- mismatched target exceptions,
- Finish-before-completion cleanup,
- duplicate part rejection,
- provided-target Return send failure fallback,
- allocation-failure rollback for fresh join-bucket insertion.

Focused `rpc.vat.join` regressions cover the addressed registry itself:
unknown direct peers, unknown/stale provisions, duplicate provision rollback,
direct-peer removal, successful JoinResult resolution, connector resolution
with shared-cache lease cleanup, malformed connector tokens that do not dial,
network teardown before `Joined` release, connector-path allocation rollback
before dialing, and host-side OOM rollback.

The shared L3/L4 test harness now includes assertions for drained provide,
join, Join relay, JoinResult accept-host back-link, pending direct-accept,
embargoed-accept, third-party, and parked-call state.

`just e2e-l4-zig` now runs a standalone Zig↔Zig loopback TCP scenario for the
current addressed JoinResult→Accept path. The client obtains the server
bootstrap cap over a real `ClientSession`/`ServerSession`, sends two Join parts,
resolves two JoinResults through `AddressedJoinNetwork`, sends the direct
Accept, calls the accepted cap, and verifies the addressed provision registry
drains. The e2e gate uses pre-registered direct peers; the connector path is
covered by focused unit/OOM regressions.

`just e2e-l3-cpp` adds cross-implementation recon checks for the `JoinKeyPart`
and `JoinResult` shapes plus a source-backed C++ runtime-surface probe. The
probe asserts that vendored Cap'n Proto C++ 2.0 exposes the generic Level-3
`VatNetworkBase` hooks used by the lane, exposes no callable generic L4 Join
hook, leaves typed `VatNetwork` Level-4 support TODO-only, and still has only
Capability client/server Join TODO comments. The Level-4 joiner and
connection-acceptance hooks are still only described as TODO pseudo-interface
comments in `rpc.capnp`.

`just e2e-l3-go` now includes Go L4 source recon: the generated Go
`rpc.capnp` bindings expose `Message.join`, the vendored twoparty schema carries
`JoinKeyPart` and `JoinResult`, but the Go receive loop still has no runtime
dispatch for `Message_Which_join`.

## Not Implemented

- No Stable `Peer.sendJoin` API. The Experimental `JoinCoordinator` is a
  Zig-shape pilot helper, not a frozen E-join consumer contract.
- No production Join addressing policy. `LoopbackJoinNetwork` is test-local and
  `AddressedJoinNetwork` is an Experimental registry/connector proof;
  applications still need their own network-specific key/result policy.
- No bundled multi-peer/direct transport dialer for Join. The connector hook can
  call application transport code, but capnp-zig does not own the dial,
  authenticate the address, or define the address format.
- No multi-hop Join relay beyond transparent cross-peer proxy exports.
- No cross-implementation L4 runtime interop.

## Next Work

The next L4 step is runtime expansion beyond the current addressed
registry/connector and transparent proxy relay: either a production Join
addressing policy/direct connection dialer for Zig deployments, or a real
`Join` exchange against an implementation with usable Join hooks. If the C++
reference stack grows callable generic Join hooks, `just e2e-l3-cpp` should fail
its source probe and force this document to move from blocker recon to runtime
interop work. Until that exists, keep L4 documented as an Experimental Zig↔Zig
runtime pilot, not cross-implementation runtime interop.
