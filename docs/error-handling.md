# Error Handling Guide

Central reference for all error types in capnpc-zig, when they occur, and how to handle them.

## Message Deserialization Errors

Returned by `Message.initUnvalidated()`, `Message.init()`, `Message.initPackedUnvalidated()`, and `Message.initPacked()` when parsing the segment table header from raw bytes.

| Error | When it occurs | What to do |
|---|---|---|
| `TruncatedMessage` | Input buffer is too short to contain the segment table header, or a declared segment extends past the end of the buffer. | Ensure the full message has been received before parsing. If streaming, buffer until complete. |
| `InvalidSegmentCount` | The segment count field overflows `u32` (e.g., `0xFFFFFFFF + 1`). | Reject the message as corrupt. |
| `SegmentCountLimitExceeded` | Segment count exceeds `max_segment_count` (512). | Reject the message. If you legitimately need more segments, this limit is compile-time in `Message`. |
| `InvalidMessageSize` | Arithmetic overflow when computing header or payload byte sizes, or a segment size is not representable. | Reject the message as corrupt or excessively large. |
| `UnexpectedEof` | Packed encoding ends mid-stream before a complete word or run is decoded. | The packed data is truncated; buffer more input or reject. |
| `Overflow` | Packed-encoding size arithmetic overflows `usize`. | The packed message claims an impossibly large unpacked size; reject it. |
| `OutOfMemory` | Allocator cannot satisfy the segment array or unpacked-buffer allocation. | Propagate to caller or retry with more memory. |

## Validation Errors

Returned by `Message.validate()`. Also returned by `Message.init()` and `Message.initPacked()` since they call `validate()` internally.

These errors protect against malicious or corrupt messages that pass header parsing but contain dangerous pointer graphs.

| Error | When it occurs | What to do |
|---|---|---|
| `TraversalLimitExceeded` | The cumulative number of words visited during the pointer-graph walk exceeds `traversal_limit_words` (default: 8M words = 64 MiB). A list whose elements occupy zero words (`List(Void)`, or a struct list of zero-width elements) is charged one word per element, so a very large one can trip this even though it allocates no content. | The message may contain amplification attacks (e.g., many far pointers to the same data). Reject it, or raise the limit via `ValidationOptions` if you expect legitimately large messages. |
| `NestingLimitExceeded` | Pointer-following depth exceeds `nesting_limit` (default: 64). | The message has excessively deep nesting, possibly crafted to cause stack overflow. Reject it, or raise the limit if your schema genuinely requires deep nesting. |
| `SegmentCountLimitExceeded` | Segment count exceeds `ValidationOptions.segment_count_limit` during validation. | Same as the deserialization variant but configurable per-call. |
| `InvalidPointer` | A pointer word has an unrecognized type tag, or a list/struct pointer is internally inconsistent. | The message is corrupt or was built by a buggy writer. Reject it. |
| `InvalidFarPointer` | A far pointer's landing pad has an invalid type, or a double-far pointer chain is malformed. | Reject the message. |
| `InvalidRootPointer` | The root pointer at segment 0, offset 0 is null, non-struct, or points outside the segment. | The message has no valid root struct. Reject it. |
| `InvalidInlineCompositePointer` | An inline-composite list's tag word is inconsistent with the list pointer's word count or element count. | The message is corrupt. Reject it. |
| `CannotUpgradeBitList` | A `List(Bool)` was read as a struct list. Every other element size may be decoded as a struct list under the list-upgrade rule; a bit list is the sole exception. | Not corruption — either a schema mismatch, or a peer that upgraded a `Bool` list, which the encoding forbids. Read the field as `List(Bool)`. |
| `OutOfBounds` | A struct or list's computed byte range extends past its segment boundary. | The message is corrupt. Reject it. |
| `InvalidSegmentId` | A far pointer references a segment ID that does not exist. | The message is corrupt. Reject it. |
| `EmptyMessage` | The message has zero segments. | The message is empty or was never initialized. |
| `PointerDepthLimit` | Internal depth counter reached zero during recursive pointer resolution (distinct from `NestingLimitExceeded`, which is the validation walk limit). | The pointer graph is too deep. Reject the message. |
| `ListTooLarge` | A list pointer's computed byte size overflows or exceeds addressable range. | Reject the message. |
| `InvalidTextPointer` | A text pointer does not use byte-sized elements, has zero length, or is missing its NUL terminator. | The message is corrupt. Reject it. |
| `InvalidUtf8` | `readTextStrict()` found a text blob that is not valid UTF-8. | The text data is corrupt. Use `readText()` if you want raw bytes without UTF-8 enforcement. |

### ValidationOptions

Defined in `Message.ValidationOptions` (see `src/serialization/message.zig`):

```zig
pub const ValidationOptions = struct {
    segment_count_limit: usize = max_segment_count,          // 512
    total_segment_words_limit: usize = max_total_words,      // 8M words
    traversal_limit_words: usize = 8 * 1024 * 1024,          // 64 MiB
    inline_composite_element_limit: usize = max_total_words, // 8M elements
    nesting_limit: usize = 64,
};
```

- `segment_count_limit` -- Maximum segments allowed. The default (512) matches `max_segment_count`.
- `traversal_limit_words` -- Total words the validator may visit. Prevents amplification attacks where a small message causes large memory reads. Default is 8M words (64 MiB).
- `nesting_limit` -- Maximum pointer-following depth. Prevents stack overflow from deeply nested structures. Default is 64.

## Struct Reader Errors

`StructReader` provides two access patterns for data-section fields:

**Default methods** (`readU64`, `readU32`, `readU16`, `readU8`, `readBool`): Return the type's zero value (0, false) when the requested offset falls outside the struct's data section. This is correct per the Cap'n Proto spec and enables schema evolution -- fields added in newer schemas read as defaults from older messages.

**Strict methods** (`readU64Strict`, `readU32Strict`, `readU16Strict`, `readU8Strict`, `readBoolStrict`): Return `error.OutOfBounds` when the offset is outside the data section. Use these for protocol-internal parsing where the field must exist.

Pointer-section methods (`readPointer`, `readText`, `readStruct`, `readListOf*`) return validation errors from the underlying pointer resolution:

| Error | When it occurs |
|---|---|
| `OutOfBounds` | Computed struct/list byte range exceeds the segment. |
| `InvalidPointer` | Pointer word has wrong type for the expected read (e.g., reading a struct pointer as a list). |
| `InvalidTextPointer` | Text pointer has wrong element size, zero length, or missing NUL terminator. |
| `InvalidUtf8` | From `readTextStrict()` only -- the text is not valid UTF-8. |
| `IndexOutOfBounds` | List element index exceeds the element count. |

## Builder Errors

Returned by `MessageBuilder` and `StructBuilder` methods during message construction.

| Error | When it occurs | What to do |
|---|---|---|
| `OutOfMemory` | The allocator cannot grow a segment or allocate a new one. | Propagate to caller. Consider pre-sizing segments for known message sizes. |
| `RootAlreadyAllocated` | `allocateStruct()` or `allocateRootStructInSegment()` is called when segment 0 already has data. | A message can only have one root struct. This is a logic error in the calling code. |
| `TooManySegments` | The builder exceeds `u32` max segments. | Practically unreachable. If hit, restructure to use fewer, larger segments. |
| `InvalidSegmentId` | A builder method references a segment ID that does not exist. | Logic error in calling code -- ensure the segment was created first. |
| `TextTooLong` | Text length exceeds `u32` max. | Split into multiple text fields or use Data instead. |
| `ElementCountTooLarge` | Inline-composite list element count exceeds `i32` max. | Split into smaller lists. |
| `ListTooLarge` | Computed list word count exceeds `u32`. | Split into smaller lists. |
| `InvalidMessageSize` | Overflow during `toBytes()` or `toSegments()` when computing total serialized size. | The built message is too large to serialize as a single framed message. |

## RPC Framing Errors

Returned by `Framer` (in `src/rpc/wire/framing.zig`) when parsing incoming RPC message frames.

| Error | When it occurs | What to do |
|---|---|---|
| `InvalidFrame` | Segment count overflows, exceeds `max_segment_count` (512), or frame header arithmetic overflows. | The peer sent a malformed frame. Log and close the connection. |
| `FrameTooLarge` | Total frame size exceeds `max_frame_words` (8M words = 64 MiB). | The peer sent an oversized frame. Reject and close the connection. Production deployments may lower `max_frame_words` for untrusted peers. |

## RPC Protocol Errors

Returned by protocol message decoders in `src/rpc/wire/protocol.zig`.

| Error | When it occurs | What to do |
|---|---|---|
| `InvalidMessageTag` | The RPC message discriminant is not a recognized `MessageTag` variant. | The peer sent an unknown message type. Send an `unimplemented` response or close the connection. |
| `InvalidDiscriminant` | A Cap'n Proto union discriminant (e.g., `CapDescriptor`, `Return`, `DisembargoContext`) has an unrecognized value. | The peer uses a newer protocol version with unknown union variants. Send `unimplemented` or ignore. |
| `UnexpectedMessage` | A typed accessor (e.g., `asBootstrap()`) is called on a message with a different tag. | Logic error in calling code -- check the tag before calling typed accessors. |
| `InvalidReturnTag` | A `Return`-specific accessor is called on a return message with a different sub-tag. | Logic error -- check `Return.tag` before calling sub-tag accessors. |
| `OutOfBounds` | A required list field is missing from the protocol message. | The message is incomplete. Reject it. |
| `MissingCallTarget` | A `Call` or `Disembargo` message has no target capability reference. | The peer sent an incomplete call. Reject with an exception. |
| `MissingPromisedAnswer` | A `PromisedAnswer` reference is required but absent. | Reject with an exception. |
| `MissingCapDescriptorId` | A `CapDescriptor` requires an ID (senderHosted, senderPromise, receiverHosted) but the field is missing. | Reject with an exception. |
| `MissingThirdPartyCapDescriptor` | A third-party cap descriptor is required but absent. | Reject with an exception. |

## RPC Capability Table Errors

Returned by `CapTable` in `src/rpc/caps/table.zig`.

| Error | When it occurs | What to do |
|---|---|---|
| `CapTableFull` | The capability table has reached its maximum entry count. | The connection has too many live capabilities. Release unused ones. |
| `CapabilityIndexOutOfBounds` | A capability descriptor references an index beyond the table size. | The peer sent an invalid capability index. Reject the message. |
| `RefCountOverflow` | Adding a reference would overflow the `u32` ref count. | Practically unreachable under normal operation. |
| `UnknownCapabilityId` | A capability ID is not found in the export or import table. | The peer referenced a capability that was never exported or has been released. |
| `RecursionLimitExceeded` | Pointer rewriting during cap table population exceeds the depth limit. | The message has excessively deep pointer nesting. Reject it. |
| `MissingCallbackContext` | An internal callback context is null when expected. | Internal error -- report as a bug. |

## RPC Peer Errors

Returned by the peer state machine in `src/rpc/peer/mod.zig` and its submodules under `src/rpc/peer/`.

### Connection Lifecycle

| Error | When it occurs | What to do |
|---|---|---|
| `PeerShuttingDown` | An operation is attempted on a peer that is shutting down. | Stop sending new requests. Drain pending responses and close cleanly. |
| `RemoteAbort` | The remote peer sent an `abort` message. | Log the exception reason from the abort message and close the connection. |
| `TransportNotAttached` | A send is attempted but no transport is connected. | Ensure the transport is attached before making calls. |

### Export and Import State

| Error | When it occurs | What to do |
|---|---|---|
| `UnknownExport` | A `release`, `resolve`, or `disembargo` references an export ID that does not exist. | The peer referenced a stale or invalid export. Send an exception. |
| `ExportIsNotPromise` | A resolve targets an export that is not a promise. | Protocol violation by the peer. Send an exception. |
| `PromiseAlreadyResolved` | A resolve targets a promise that was already resolved. | Duplicate resolve from the peer. Send an exception. |
| `PromiseUnresolved` | A disembargo or pipelined call targets a promise that has not yet resolved. | The peer is sending disembargoes too early. Buffer or reject. |
| `PromiseBroken` | A resolved promise resolved to `.none` (broken). | The capability is no longer available. Return an exception to the caller. |
| `PromisedAnswerMissing` | A pipelined read targets a return that has no results payload. | The call returned an exception or was redirected. Return an exception to the caller. |
| `RefCountOverflow` | Adding an export reference overflows `u32`. | Practically unreachable. |

### Question and Answer State

| Error | When it occurs | What to do |
|---|---|---|
| `QuestionIdExhausted` | The `u32` question ID space has wrapped around completely with no free IDs. | The connection has too many outstanding questions. Wait for some to complete. |
| `DuplicateQuestionId` | A new question or return reuses an ID that is still in-flight. | Protocol violation. Close the connection. |
| `UnknownQuestion` | A return references a question ID that was never asked. | The peer sent a return for an unknown call. Ignore or close. |
| `MissingQuestionId` | A `takeFromOtherQuestion` return has no question ID. | The peer sent an incomplete return. Reject. |

### Disembargo State

| Error | When it occurs | What to do |
|---|---|---|
| `MissingEmbargoId` | A disembargo message has no embargo ID field. | The peer sent an incomplete disembargo. Reject. |
| `UnknownDisembargoTarget` | A disembargo targets a capability type that is not `importedCap` or `promisedAnswer`. | Protocol violation. Reject. |
| `EmbargoIdExhausted` | The `u32` embargo ID space has wrapped with no free IDs. | Practically unreachable. |

### Three-Way Introduction (Level 3)

| Error | When it occurs | What to do |
|---|---|---|
| `MissingThirdPartyPayload` | A `provide`, `accept`, or third-party return is missing its required payload. | The peer sent an incomplete level-3 message. Reject. |
| `DuplicateProvideQuestionId` | A `provide` reuses a question ID that already has an active provide. | Protocol violation. Reject. |
| `DuplicateProvideRecipient` | A `provide` targets a recipient that already has an active provide for the same cap. | Protocol violation. Reject. |
| `DuplicateJoinQuestionId` | A `join` reuses a question ID that already has an active join. | Protocol violation. Reject. |
| `InvalidThirdPartyAnswerId` | A `thirdPartyAnswer` references an answer ID that is out of range. | Protocol violation. Reject. |
| `DuplicateThirdPartyAnswerId` | A `thirdPartyAnswer` reuses an answer ID that is already registered. | Protocol violation. Reject. |
| `ConflictingThirdPartyAnswer` | A `thirdPartyAnswer` conflicts with a previously registered answer for the same provide. | Protocol violation. Reject. |
| `DuplicateThirdPartyReturn` | A return for a third-party cap was already received. | Duplicate return. Ignore or reject. |
| `DuplicateThirdPartyAwait` | An `awaitFromThirdParty` return reuses a completion key that is already pending. | Protocol violation. Reject. |
| `UnexpectedForwardedTailReturn` | A forwarded tail-call return has an unexpected sub-tag. | Protocol violation. Reject. |
| `MissingResolveCap` | A `resolve` message has no capability descriptor. | The peer sent an incomplete resolve. Reject. |

### Join State

| Error | When it occurs | What to do |
|---|---|---|
| `MissingJoinKeyPart` | A `join` message has no key part or the key part pointer is null. | The peer sent an incomplete join. Reject. |
| `InvalidJoinKeyPart` | A join key part struct cannot be read or has invalid `partCount`/`partNum` fields. | The peer sent a malformed join key. Reject. |

## Schema Validation Errors

Returned by functions in `src/serialization/schema_validation.zig` when validating message content against a Cap'n Proto schema.

| Error | When it occurs | What to do |
|---|---|---|
| `InvalidSchema` | A schema node reference is invalid, a required sub-node is missing, a struct node lacks its `struct_node` payload, or a field's type does not match expectations. | The schema or message is inconsistent. If validating external input, reject it. If this occurs with your own schemas, check your `.capnp` files and re-run codegen. |
| `SchemaRecursionLimitExceeded` | Schema traversal depth exceeds the internal limit during type/field validation. | The schema has excessively deep type nesting. Simplify the schema or report as a bug if the depth is reasonable. |
| `SchemaCycleDetected` | A cycle is detected in the schema node graph during validation. | The schema is malformed (struct A contains struct B contains struct A without indirection through a pointer). Fix the `.capnp` schema. |
| `StructSizeTooSmall` | A struct's declared data or pointer section is too small to hold all its fields. | The schema and message are inconsistent. Re-run codegen to ensure they match. |
| `InvalidEnumValue` | An enum field's discriminant value exceeds the number of enumerants defined in the schema. | The message was built with a newer schema that has more enum values. Handle as an unknown enum or reject. |
| `InvalidListElementSize` | A list's wire element size does not match what the schema expects (e.g., a `List(UInt32)` encoded with byte-sized elements). | The message is corrupt or schema-mismatched. Reject. |
| `InvalidTextPointer` | A text field has zero length, wrong element size, or missing NUL terminator during schema validation. | The message is corrupt. Reject. |
| `OutOfBounds` | A struct or list field's byte range extends past the segment during schema-aware validation. | The message is corrupt. Reject. |
| `NonCanonicalSegments` | Canonicalization requires exactly one segment but the message has multiple. | Re-serialize as a single-segment message before canonicalizing. |
| `OffsetOverflow` | A field offset computation overflows during validation. | The schema declares an unreasonably large field offset. Reject. |
| `IndexOutOfBounds` | A list element index exceeds the list's element count during schema validation. | The message is corrupt. Reject. |

## Best Practices

### Always validate untrusted input

Use `Message.init()` or `Message.initPacked()` for any data received from the network, files, or other untrusted sources. These call `validate()` automatically:

```zig
const msg = try Message.init(allocator, received_bytes, .{});
defer msg.deinit();
const root = try msg.getRootStruct();
```

### Use unvalidated init only for trusted data

`Message.initUnvalidated()` and `Message.initPackedUnvalidated()` skip the pointer-graph walk. Use them only for messages you built yourself or received through a trusted internal channel:

```zig
// Safe: we just built this message
const bytes = builder.toBytes(allocator);
const msg = try Message.initUnvalidated(allocator, bytes);
```

### Tune ValidationOptions for your use case

The defaults are generous. For untrusted network peers, consider tightening:

```zig
const msg = try Message.init(allocator, data, .{
    .traversal_limit_words = 1024 * 1024,  // 8 MiB instead of 64 MiB
    .nesting_limit = 32,                    // shallower nesting
    .segment_count_limit = 16,              // fewer segments
});
```

For large trusted messages (e.g., code-generator requests), consider raising:

```zig
const msg = try Message.init(allocator, data, .{
    .traversal_limit_words = 64 * 1024 * 1024,  // 512 MiB
});
```

### Common error handling patterns

Catch and log with context:

```zig
const msg = Message.init(allocator, data, .{}) catch |err| {
    log.err("failed to parse message: {s}", .{@errorName(err)});
    return err;
};
```

Distinguish recoverable from fatal in RPC:

```zig
peer.handleMessage(frame) catch |err| switch (err) {
    error.RemoteAbort => {
        log.warn("peer aborted connection", .{});
        return;  // clean shutdown
    },
    error.PeerShuttingDown => return,
    error.OutOfMemory => return err,  // propagate
    else => {
        log.err("protocol error: {s}", .{@errorName(err)});
        try peer.sendAbort("protocol error");
        return;
    },
};
```

### Use strict readers for required fields

When parsing protocol-internal messages where a field must exist, use the `*Strict` variants so missing fields are caught early rather than silently returning zero:

```zig
const version = try reader.readU16Strict(0);  // error if field missing
const flags = try reader.readU32Strict(2);
```

For application messages where schema evolution is expected, use the default (non-strict) readers that return zero/false for missing fields.
