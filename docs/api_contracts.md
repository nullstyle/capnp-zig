# API Contracts And Error Taxonomy

Updated: 2026-08-11

## Scope
This document defines stability and failure-mode expectations for the public `capnp-zig` library surface:

- `message` wire-format APIs (`MessageBuilder`, `Message`, readers/builders).
- `rpc` runtime APIs (`wire`, `caps`, `promises`, `events`, `transport`,
  `peer`, `integration`, `generated`, `testing`).
- Generated APIs emitted by `capnpc-zig`.

The RPC facade is public-breaking in the current development line. Consumers
should import domain modules such as `rpc.wire.protocol`,
`rpc.caps.table`, `rpc.promises.pipeline`, `rpc.transport.tcp`,
`rpc.transport.quic`, and `rpc.integration.host_peer`. Removed top-level
compatibility aliases are not part of the supported surface. See
[`docs/rpc-migration-guide.md`](rpc-migration-guide.md) for the full old-name
to new-name mapping.

Internal helper behavior may change, but exported type semantics and error classes below are considered compatibility-sensitive.

## Ownership And Lifetime Contracts
- `MessageBuilder.toBytes()` / `toPackedBytes()` return allocator-owned buffers.
  Caller must free each returned buffer exactly once.
- `Message.init*()` copies/owns decode state and must be paired with `deinit()`.
- Reader slices (for example `readText()`, list views) are borrowed views into message memory.
  They are invalid after the owning `Message` is deinitialized.
- `rpc.peer.Peer` owns in-flight question/answer tables, pending promise queues, and temporary payload copies.
  `Peer.deinit()` is guaranteed to release all retained runtime state, including unresolved pending work.
- Generated struct/interface readers are borrow-only wrappers over runtime readers.
  Generated builders mutate only their associated message arena.

## Concurrency Contract
- `rpc.peer.Peer` is single-thread-affine; concurrent mutation is unsupported.
- Use one event-loop owner thread per peer/connection.
- Cross-thread interactions must be serialized onto the owner loop before calling `Peer` methods.

## Experimental L4 Join Lease Contract

- Raw `Peer` construction keeps `PeerTimeouts.join_timeout_ms = null`. TCP
  connect/serve and `WorkerPool` set 30 seconds unless the caller explicitly
  opts out with null.
- The first part fixes a partial Join's deadline; later parts cannot extend it.
  Relay and hosted-Accept phases receive new deadlines in their own peer clock
  domains.
- `Peer.sweepExpiredJoins()` counts detached Join phases. It does not alter the
  outbound-cancellation count returned by `checkDeadlines()`.
- `attachJoinNetwork()` and `detachJoinNetwork()` return
  `error.JoinNetworkInUse` while dependent state or a callback borrow exists;
  identical reattachment is a no-op.
- Result-path transport close preserves an already-committed pickup only when
  its distinct Accept host remains live. Accept-host close, expiry, explicit
  cleanup, or owner deinit cancels it. `detachTransport()` is non-terminal.
- Quota and timeout failures disclose only `"join unavailable"` remotely.
  Stats/events contain aggregate record, part, provision-byte, and inbound
  answer-ID data, never targets, provisions, keys, or addresses.

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

## Generated Schema-Evolution and Type-Fidelity Views

- Generated enum types remain exhaustive `enum(u16)` values. Typed field/list
  getters return `error.InvalidEnumValue` for an ordinal unknown to their schema.
- Structs and groups with enum slots expose `Reader.EnumOrdinals` and
  `Builder.EnumOrdinals` through `enumOrdinals()`. Their `u16` accessors apply
  enum-default XOR and therefore expose logical, not encoded, ordinals.
- Enum lists expose `getOrdinal()` / `setOrdinal()` alongside the typed APIs and
  existing `raw()` accessors.
- Generated union Readers expose infallible `whichOrdinal() u16`; typed
  `which()` remains strict. Builders intentionally do not expose a raw union-tag
  setter because a discriminant cannot safely initialize an unknown arm's
  storage.

Generated `hasXxx()` methods on Reader and Builder report structural presence
for Text, Data, struct, list, AnyPointer, and interface slots. A null or
out-of-layout slot is absent even when schema defaults make its getter return a
non-empty value. An explicitly encoded empty value is present. Union fields are
present only while their arm is active. Presence does not resolve or validate a
nonzero pointer.

`StructBuilder.isPointerNull()` follows the same out-of-layout-as-null rule as
`StructReader.isPointerNull()`. `StructBuilder.readUnionDiscriminant()` likewise
returns zero when the discriminant lies beyond the allocated data section.
Explicitly initialized zero-sized structs use the reference-compatible non-null
offset -1 struct pointer; an absent struct remains a zero pointer.

Pointer-kind and brand fidelity are additive compatibility views:

- The frozen `schema.Type` union retains exactly its existing tags and
  payloads. `schema.TypeMetadata` / `TypeExpression` and parallel parameter /
  brand fields carry the details that the legacy model erased.
- Constrained `AnyStruct`, `AnyList`, and bare `Capability` slots may expose a
  generated `pointerKinds()` view. Existing `AnyPointerReader` /
  `AnyPointerBuilder` accessors remain present. `AnyListReader` and the Builder
  shape wrappers can return their erased `raw()` view; AnyStruct and Capability
  Reader access is already concrete. Null lists retain empty-list semantics;
  wire-distinguishable non-null wrong-kind pointers fail, union guards still
  apply, and Builder getters reopen existing near or far pointers without
  replacing them. A layout-A double-far zero-offset struct tag is wire-identical
  to an empty inline-composite list and therefore cannot be distinguished.
- Finite concrete branded generic data-struct fields may expose a generated
  `brands()` view. Supported composition includes arbitrary-depth lists,
  enum/Text/Data/struct/interface terminals, concretely branded nested structs,
  inherited lexical bindings, generic struct applications as list terminals,
  and cross-file imported applications/terminals. The legacy erased target
  accessor remains the compatibility API.
- One allocation-free resolver shared by validation and codegen composes
  `.bind` / `.inherit`, checks lexical scope, exact arity and parameter indexes,
  and applies a 64-level cycle/depth bound. A valid unbound application retains
  erased behavior; a malformed graph or scalar generic binding is
  `InvalidSchema`.
- No specialized client is promised for generic interfaces or implicit generic
  RPC methods. Preserved schema metadata does not by itself imply generated
  specialization. Emitted applications are independently bounded by
  `CodegenBudget.max_brand_specializations` (4096 by default).
- The generated names `Brands`, `brands`, `PointerKinds`, and `pointerKinds`
  participate in normal generated-name validation; a schema collision is an
  error rather than a shadowed declaration.

Stable schema-aware entry points are additive:

- `validateMessageWithBrand(...)`
- `canonicalizeMessageWithBrand(...)`
- `canonicalizeMessageFlatWithBrand(...)`

Their existing counterparts use an empty root brand while honoring concrete
nested metadata. The schema-free `canonical.*` surface is unchanged.

## Compatibility Policy
- New error variants may be added.
- Existing successful behavior and existing error classes/reasons should not be silently repurposed.
- Any externally visible semantic change requires corresponding checklist entry and tests.
