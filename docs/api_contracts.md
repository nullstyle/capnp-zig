# API Contracts And Error Taxonomy

Updated: 2026-02-07

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

## Primitive Read/Write Default-Value Behavior (Schema Evolution)

The Cap'n Proto specification mandates that reading a primitive field past the end of a struct's data section returns the type's default value (zero for integers, false for booleans, empty string for text). This is not a bug — it is the mechanism that enables **schema evolution**: when a newer schema adds fields to a struct, messages serialized with an older schema (which has a shorter data section) are still readable; the new fields simply appear as their defaults.

Accordingly, the following `StructReader` methods return defaults on out-of-bounds access without signalling an error:

| Method | Default on OOB |
|---|---|
| `readU64(byte_offset)` | `0` |
| `readU32(byte_offset)` | `0` |
| `readU16(byte_offset)` | `0` |
| `readU8(byte_offset)` | `0` |
| `readBool(byte_offset, bit_offset)` | `false` |
| `readText(pointer_index)` | `""` |

Similarly, the following `StructBuilder` methods silently drop writes on out-of-bounds access (a builder allocated with an older/smaller schema ignores fields that do not fit):

| Method | Behavior on OOB |
|---|---|
| `writeU64(byte_offset, value)` | silent no-op |
| `writeU32(byte_offset, value)` | silent no-op |
| `writeU16(byte_offset, value)` | silent no-op |
| `writeU8(byte_offset, value)` | silent no-op |
| `writeBool(byte_offset, bit_offset, value)` | silent no-op |

### Strict Variants

For use cases where an out-of-bounds access indicates a real bug (e.g. protocol-internal parsing of a known-layout struct, or test assertions), each method has a `*Strict` counterpart that returns `error.OutOfBounds`:

- `readU64Strict`, `readU32Strict`, `readU16Strict`, `readU8Strict`, `readBoolStrict`
- `writeU64Strict`, `writeU32Strict`, `writeU16Strict`, `writeU8Strict`, `writeBoolStrict`

Generated code and normal application code should use the non-strict (default-returning) variants. Strict variants are intended for internal protocol parsing and debugging.

## Compatibility Policy
- New error variants may be added.
- Existing successful behavior and existing error classes/reasons should not be silently repurposed.
- Any externally visible semantic change requires corresponding checklist entry and tests.
