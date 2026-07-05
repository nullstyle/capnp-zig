# RPC L4 Join Readiness

Status: Experimental, Zig-to-Zig readiness only.

`capnp-zig` has a guarded receive-side slice of Cap'n Proto RPC Level 4
`Join`. This is not a complete L4 implementation and is not part of the Stable
surface. It exists to keep the provide/join state model correct while L3
handoff grows toward a future cross-implementation interop sprint.

## What Exists

- The wire layer can encode and decode `Message.join`.
- `Peer.handleJoin` accepts inbound `Join` messages and resolves their
  `target` through the same target machinery used by `Provide`.
- Join parts are collected in `pending_joins`, with question-to-part back-links
  in `pending_join_questions`.
- When all parts for a join ID arrive, the peer compares the resolved targets.
  Matching targets receive a results Return carrying the provided target;
  mismatched targets receive exception Returns.
- `Finish` for an outstanding Join question clears the matching part, releases
  its target payload, and drops the join bucket when it becomes empty.
- Return send failure while completing a Join degrades each affected question to
  an exception Return and drains the pending maps.

The current local JoinKeyPart convention is an `AnyPointer` to a one-data-word
struct:

- bytes 0..3: `join_id : UInt32`
- bytes 4..5: `part_count : UInt16`
- bytes 6..7: `part_num : UInt16`

That convention is internal and Experimental. It is enough to prove state
ordering, cleanup, and target equality behavior; it is not a frozen public key
format.

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
- Target ownership is single-owner: once a part is inserted, the Join state owns
  its `ProvideTarget`; rejected or failed inserts return ownership to the caller
  for cleanup.

## Current Evidence

Focused peer regressions in `tests/rpc/peer/rpc_join_readiness_test.zig` cover:

- matching two-part Join completion,
- mismatched target exceptions,
- Finish-before-completion cleanup,
- duplicate part rejection,
- provided-target Return send failure fallback,
- allocation-failure rollback for fresh join-bucket insertion.

The shared L3/L4 test harness now includes assertions for drained provide,
join, embargoed-accept, third-party, and parked-call state.

## Not Implemented

- No public `Peer.sendJoin` API.
- No JoinResult builder/parser beyond treating Join results as ordinary
  provided-cap Returns in the current receive-side slice.
- No direct-connection establishment from Join results.
- No multi-hop Join relay semantics.
- No cross-implementation L4 interop.

## Next Interop Sprint

The next L3/L4 cross-implementation sprint should start by auditing what each
reference implementation actually supports for `Provide`, `Accept`, `Join`, and
their VatNetwork-specific token formats. If a viable partner exists, target one
narrow scenario first; otherwise record a compatibility matrix and the concrete
adapter work needed before promising interop.
