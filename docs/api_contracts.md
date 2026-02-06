# API Contracts And Error Taxonomy

Updated: 2026-02-06

## Scope
This document defines stability and failure-mode expectations for the public `capnp-zig` library surface:

- `message` wire-format APIs (`MessageBuilder`, `Message`, readers/builders).
- `rpc` runtime APIs (`peer`, `protocol`, `connection`, `cap_table`).
- Generated APIs emitted by `capnpc-zig`.

Internal helper behavior may change, but exported type semantics and error classes below are considered compatibility-sensitive.

## Ownership And Lifetime Contracts
- `MessageBuilder.toBytes()` / `toPackedBytes()` return allocator-owned buffers.
  Caller must free each returned buffer exactly once.
- `Message.init*()` copies/owns decode state and must be paired with `deinit()`.
- Reader slices (for example `readText()`, list views) are borrowed views into message memory.
  They are invalid after the owning `Message` is deinitialized.
- `rpc.Peer` owns in-flight question/answer tables, pending promise queues, and temporary payload copies.
  `Peer.deinit()` is guaranteed to release all retained runtime state, including unresolved pending work.
- Generated struct/interface readers are borrow-only wrappers over runtime readers.
  Generated builders mutate only their associated message arena.

## Concurrency Contract
- `rpc.Peer` is single-thread-affine; concurrent mutation is unsupported.
- Use one event-loop owner thread per peer/connection.
- Cross-thread interactions must be serialized onto the owner loop before calling `Peer` methods.

## Error Taxonomy
Errors are grouped by class for caller policy decisions:

- `DecodeError` (malformed/truncated/overflow wire data).
  Examples: invalid framing headers, segment/count limit violations, invalid tags.
  Policy: treat as peer/protocol failure; abort or close connection.
- `ProtocolError` (message is decodable but violates RPC semantics).
  Examples: unknown question/answer IDs, duplicate joins, conflicting third-party completion keys.
  Policy: send RPC exception/abort where possible, then clean up local state.
- `CapabilityError` (cap-table/target resolution failures).
  Examples: unknown capability, unresolved promise, invalid promised-answer transform.
  Policy: return exception to caller; avoid process crash.
- `ResourceError` (allocation/limits/backpressure).
  Examples: `OutOfMemory`, traversal/segment limits, queue pressure.
  Policy: fail operation and preserve allocator/runtime invariants.

## Compatibility Policy
- New error variants may be added.
- Existing successful behavior and existing error classes/reasons should not be silently repurposed.
- Any externally visible semantic change requires corresponding checklist entry and tests.
