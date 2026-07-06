# RPC L4 Join Readiness

Status: Experimental, Zig↔Zig JoinResult runtime pilot with addressed-registry
TCP proof, transparent proxy relay, raw origination, and receive-side readiness.

`capnp-zig` has a guarded slice of Cap'n Proto RPC Level 4 `Join`: inbound
Join state handling, a low-level Experimental sender for raw Join parts, and a
Zig-only `JoinResult` → direct `Accept` runtime path behind an Experimental
`JoinNetwork` seam. Transparent cross-peer proxy exports can now relay Join
requests to their source peer and hold the downstream JoinResult lifetime until
the upstream caller sends Finish. This is not a complete L4 implementation and
is not part of the Stable surface. It exists to keep the provide/join state
model correct while L3 handoff grows toward cross-implementation use.

## What Exists

- The wire layer can encode and decode `Message.join`.
- `Peer.sendJoinExperimental` can originate a raw Join question with a caller
  supplied `Join.keyPart` AnyPointer and ordinary `Return` callback. It is a
  manual probe helper, not a high-level E-join API.
- A test-local Join coordinator drives multi-part Zig↔Zig joins on top of
  `sendJoinExperimental`, selects the agreed cap, retains/release-checks result
  imports, and records per-part exception outcomes. This helper is not exposed
  as a consumer API.
- `rpc.vat.join.JoinNetwork` is an Experimental L4 addressing seam. The
  `LoopbackJoinNetwork` test implementation maps completed Join results to the
  joiner's direct peer and an opaque `Accept.provision` payload.
- `rpc.vat.join.AddressedJoinNetwork` is an Experimental addressed-registry
  implementation for the Zig pilot. Callers register a host peer with an opaque
  application address and an already-live direct peer. The generated provision
  token carries that address plus a nonce; it is still a registry proof, not a
  production dialer or stable address format.
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
- A fresh join bucket is rollback-safe: if allocation fails after the bucket is
  created but before the first part is fully indexed, the empty bucket is removed.
- `pending_joins` and `pending_join_questions` drain together on successful
  completion, mismatch completion, Finish cleanup, send-failure fallback, and
  peer deinit.
- `pending_join_accepts` drains on direct Accept success, JoinResult send
  rollback when no result reached the joiner, and peer deinit.
- `pending_join_relays` and `cross_peer_join_relay_links` drain together on
  upstream Finish, downstream exception, owner/source teardown, downstream send
  failure, and allocation rollback.
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
- JoinResult Return send failure rollback, proving no stale
  `pending_join_accepts` entry remains when all JoinResult Returns fail,
- transparent proxy Join relay where A joins two caps through B/C proxy exports
  that forward to the same D-hosted cap, receives JoinResults, sends direct
  Accept on A↔D, invokes the accepted cap directly, and then drains relay state
  only after upstream Finish,
- relay failure/lifecycle cases: upstream Finish before downstream Return,
  source unavailable, downstream Join send failure, owner-peer-first teardown,
  source-peer-first teardown, target mismatch through the relay, and OOM
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
direct-peer removal, successful JoinResult resolution, and host-side OOM
rollback.

The shared L3/L4 test harness now includes assertions for drained provide,
join, Join relay, JoinResult accept-host back-link, pending direct-accept,
embargoed-accept, third-party, and parked-call state.

`just e2e-l4-zig` now runs a standalone Zig↔Zig loopback TCP scenario for the
current addressed JoinResult→Accept path. The client obtains the server
bootstrap cap over a real `ClientSession`/`ServerSession`, sends two Join parts,
resolves two JoinResults through `AddressedJoinNetwork`, sends the direct
Accept, calls the accepted cap, and verifies the addressed provision registry
drains.

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

- No Stable or high-level `Peer.sendJoin` API.
- No production Join addressing policy. `LoopbackJoinNetwork` is test-local and
  `AddressedJoinNetwork` is an Experimental registry proof; applications still
  need their own network-specific key/result policy and dialer.
- No bundled multi-peer/direct transport dialer for Join. The current Zig proof
  resolves to already-live peers registered with a JoinNetwork implementation.
- No multi-hop Join relay beyond transparent cross-peer proxy exports.
- No cross-implementation L4 runtime interop.

## Next Work

The next L4 step is runtime expansion beyond the current addressed registry and
transparent proxy relay: either a production Join addressing policy/direct
connection dialer for Zig deployments, or a real `Join` exchange against an
implementation with usable Join hooks. If the C++ reference stack grows callable
generic Join hooks, `just e2e-l3-cpp` should fail its source probe and force this
document to move from blocker recon to runtime interop work. Until that exists,
keep L4 documented as an Experimental Zig↔Zig runtime pilot, not
cross-implementation runtime interop.
