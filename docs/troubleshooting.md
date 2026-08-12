# Troubleshooting Guide

Common pitfalls when using capnp-zig, with wrong/right code examples.

For a complete error reference, see [error-handling.md](error-handling.md).

---

## Reader Lifetime

`StructReader` and text/data slices point directly into the `Message` buffer (zero-copy). Deiniting the `Message` frees the segment index and any owned backing data, invalidating all readers and slices obtained from it.

**Wrong** -- use reader after deiniting message:

```zig
var name: []const u8 = undefined;
{
    var msg = try Message.init(allocator, data, .{});
    defer msg.deinit(); // frees segment index here
    const reader = try PlayerInfo.Reader.init(&msg);
    name = try reader.getName(); // slice into msg's segment data
}
// name now points to freed memory!
std.debug.print("name: {s}\n", .{name});
```

**Right** -- keep the message alive for the duration of reader use:

```zig
var msg = try Message.init(allocator, data, .{});
defer msg.deinit();
const reader = try PlayerInfo.Reader.init(&msg);
const name = try reader.getName();
// msg is still alive, so name is valid
std.debug.print("name: {s}\n", .{name});
```

If you need data to outlive the message, copy it:

```zig
const name_owned = try allocator.dupe(u8, try reader.getName());
defer allocator.free(name_owned);
msg.deinit();
// name_owned is still valid
```

---

## Validation for Untrusted Input

`Message.init()` parses the segment table and performs a full pointer-graph validation walk. `Message.initUnvalidated()` skips the walk -- only use it for data you built yourself.

**Wrong** -- skip validation on network input:

```zig
// Attacker controls received_bytes and can craft pointer cycles,
// amplification attacks, or out-of-bounds pointers
var msg = try Message.initUnvalidated(allocator, received_bytes);
const root = try msg.getRootStruct(); // may follow malicious pointers
```

**Right** -- validate with appropriate limits:

```zig
var msg = try Message.init(allocator, received_bytes, .{
    .traversal_limit_words = 1024 * 1024, // 8 MiB instead of default 64 MiB
    .nesting_limit = 32,                  // tighter depth limit
    .segment_count_limit = 16,            // fewer segments
});
defer msg.deinit();
const root = try msg.getRootStruct(); // safe: pointers validated
```

See `ValidationOptions` in [error-handling.md](error-handling.md) for the full set of tunable limits.

---

## Union Discriminants

Generated union types use a `WhichTag` enum and a `which()` method. A newly allocated struct has all-zero data, so the first union arm (discriminant 0) is active by default. Accessing a different arm without checking returns data from the wrong field.

**Wrong** -- access union field without checking discriminant:

```zig
// ChatMessage.Kind has: normal=0, emote=1, system=2, whisper=3
const kind = chat_msg.getKind();
// BUG: assumes whisper is active, but discriminant might be 0 (normal)
const target = try kind.getWhisper();
```

**Right** -- switch on `which()`:

```zig
const kind = chat_msg.getKind();
switch (try kind.which()) {
    .normal => { /* handle normal */ },
    .emote => { /* handle emote */ },
    .system => { /* handle system */ },
    .whisper => {
        const target = try kind.getWhisper();
        // now safe to use target
    },
}
```

Note: `which()` returns `error.InvalidEnumValue` if the discriminant does not
match any known variant (e.g., the message was built with a newer schema). Use
`whichOrdinal()` to log or forward that unknown discriminant; continue to use
`which()` before accessing a known arm.

---

## Default Values vs Missing Fields (Schema Evolution)

`StructReader.readU32()`, `readU16()`, `readBool()`, etc. return the type's zero value (0, false) when the offset falls outside the struct's data section. This is correct per the Cap'n Proto spec -- it enables schema evolution so that fields added in newer schemas read as defaults from older messages.

**Wrong** -- assume missing fields produce errors:

```zig
const reader = try MyStruct.Reader.init(&msg);
// readU32 returns 0 for out-of-bounds, never errors
const version = try reader._reader.readU32(0);
if (version == 0) {
    // This branch fires both for "field is 0" AND "field doesn't exist"
    return error.MissingVersion;
}
```

**Right** -- use strict variants when the field must exist:

```zig
const reader = try MyStruct.Reader.init(&msg);
// readU32Strict returns error.OutOfBounds if the field is missing
const version = reader._reader.readU32Strict(0) catch |err| switch (err) {
    error.OutOfBounds => return error.IncompatibleSchema,
};
```

For application messages where schema evolution is expected, the default (non-strict) readers are correct -- zero/false is the intended default. See the "Use strict readers for required fields" section in [error-handling.md](error-handling.md).

Pointer getters have the same default-value ambiguity: an absent Text field and
a present empty Text field both read as `""`. Generated `hasXxx()` methods
separate those cases:

```zig
if (!reader.hasDisplayName()) {
    // The pointer slot is null, or is outside this older message's layout.
} else {
    const display_name = try reader.getDisplayName(); // may still be empty
}
```

Presence is not validation. A malformed nonzero pointer makes `hasXxx()` true
and still makes `getXxx()` fail. For union fields, `hasXxx()` is false unless
that arm is active, even if another arm left nonzero data in shared storage.

---

## Why a Generic Field Has No `brands()` View

Brand-aware generation is additive and deliberately conservative. The parser
always preserves the request's type parameters and bindings beside the frozen
`schema.Type` union, but generated typed `brands()` access appears only for a
finite concrete generic data-struct application. Supported wrappers compose
arbitrary-depth lists, enum/Text/Data/struct/interface terminals, concretely
branded nested structs, generic struct applications as list terminals,
inherited lexical bindings, and cross-file imported applications/terminals.

No view is emitted for a valid unbound or recursively infinite application, or
for generic interface/implicit RPC method specialization. That absence keeps
the historical erased behavior instead of promising an application the
generator cannot finitely emit. Use the field's unchanged erased Reader/Builder
accessor and inspect `schema.TypeMetadata` / `TypeExpression` when tooling needs
the original binding. Scalar generic bindings and malformed scope, arity,
parameter-index, cycle, or depth graphs are different: they fail with
`error.InvalidSchema`.

If an otherwise finite schema fails with `CodegenBudgetExceeded`, it may have
crossed the separate 4096-application default. Raise or lower it with
`max-codegen-brand-specializations=` or
`CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS` after reviewing the expected
generated-code size.

The related `pointerKinds()` view follows the same rule for constrained
`AnyStruct`, `AnyList`, and bare `Capability` slots. A plain unconstrained
`AnyPointer` has no narrower shape, so its legacy accessor is the intended API.

---

## Builder Lifecycle

`StructBuilder` holds a pointer back to the `MessageBuilder` that owns the segment data. Deiniting the `MessageBuilder` frees all segments, invalidating any outstanding `StructBuilder` references.

**Wrong** -- deinit builder while still using struct builder:

```zig
var struct_builder: PlayerInfo.Builder = undefined;
{
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit(); // frees segments here
    struct_builder = try PlayerInfo.Builder.init(&builder);
}
// struct_builder._builder.builder now points to freed MessageBuilder!
try struct_builder.setName("Alice"); // undefined behavior
```

**Right** -- keep builder alive, serialize, then deinit:

```zig
var builder = message.MessageBuilder.init(allocator);
defer builder.deinit();
var player = try PlayerInfo.Builder.init(&builder);
try player.setName("Alice");
// toBytes() copies data out -- the returned slice is independently owned
const bytes = try builder.toBytes();
defer allocator.free(bytes);
// builder can now be deinited safely; bytes is a separate allocation
```

Note: `toBytes()` returns an allocator-owned slice that is independent of the builder. The caller must free it.

---

## RPC Capability Ownership

`addExport()` registers a capability handler and returns its export ID. The `Export` struct contains a context pointer (`*anyopaque`) and a `CallHandler` function pointer. The handler context must outlive the peer, because inbound calls dispatch to it asynchronously.

**Wrong** -- export a handler whose context is stack-local:

```zig
fn setupPeer(peer: *Peer) !void {
    var handler = MyHandler{ .state = 42 };
    // BUG: handler lives on this stack frame
    _ = try peer.addExport(.{
        .ctx = @ptrCast(&handler),
        .on_call = MyHandler.handleCall,
    });
    // handler is destroyed when setupPeer returns, but peer
    // still holds a pointer to it
}
```

**Right** -- heap-allocate the handler so it outlives the peer:

```zig
fn setupPeer(peer: *Peer, allocator: std.mem.Allocator) !void {
    const handler = try allocator.create(MyHandler);
    handler.* = .{ .state = 42 };
    _ = try peer.addExport(.{
        .ctx = @ptrCast(handler),
        .on_call = MyHandler.handleCall,
    });
    // handler lives until you explicitly free it (after peer.deinit)
}
```

Similarly, `sendCall` takes a context pointer and a `QuestionCallback`. The context must remain valid until the callback fires (when the Return message arrives).

**Release messages**: When the remote peer is done with a capability, it sends a `Release` message that decrements the export's ref count. The peer handles this automatically via `peer_inbound_release`. On `peer.deinit()`, the peer sends best-effort `Release` messages for all remaining imports (via `releaseAllImports`). You do not need to send Release manually under normal operation, but you must ensure the transport is still attached when deinit runs if you want cleanup releases to reach the remote side.

---

## Retained RPC Results Do Not Finish Automatically

The Experimental `.result_lifetime = .retained` option deliberately leaves the
remote answer open after its terminal Return. Use a raw
`sendCall*WithOptions`, a generated `callXxxWithOptions`, or
`callXxxPipelinedWithOptions`, retain the returned question ID, and finish it
after the callback has run:

```zig
const question_id = try client.callLookupWithOptions(
    ctx,
    buildLookup,
    onLookupReturn,
    .{ .result_lifetime = .retained },
);

// After the terminal Return is callback-visible:
try peer.finishRetainedQuestion(question_id, false);
```

`error.RetainedQuestionPending` means the terminal Return has not arrived yet.
A failed Finish send leaves the record live, so retry the same call. If
`sendProvideFromRetainedAnswer` or
`resolvePromiseExportToThirdPartyFromRetainedAnswer` succeeded,
`error.RetainedQuestionAlreadyTransferred` is expected: the Level-3 coupling
now owns Finish, and manual cleanup would race it. `Peer.stats()` separates
caller-owned `retained_questions` from
`transferred_retained_questions`; a nonzero transferred gauge after the
coupling has ended usually means its control-frame send failed. Keep driving
`Peer.checkDeadlines()` so maintenance can retry it even when no outbound call
clock is configured.

Streaming fire-and-forget methods do not expose retained lifetime. Their
StreamClient calls stay automatic.

---

## L3 Vat Clock and Manual Transport Close

The high-level Experimental `rpc.peer.Vat` enables a 30-second parked-Accept
TTL by default. A finite TTL needs monotonic time, so supplying only a
deterministic entropy seed now returns `error.ParkClockUnavailable`.

```zig
var vat = try rpc.peer.Vat.init(allocator, .{
    .seed = deterministic_seed,
    .io = io, // value-stored monotonic fallback; seed still wins for entropy
});
defer vat.deinit();
```

Use `Options.clock` for a deterministic/test clock; it takes precedence over
`io`. Other Vat limit overrides retain the 30-second default; set
`.limits = .{ .park_ttl_ms = null }` only when you deliberately need the
compatibility behavior. Clearing the only custom clock on a finite-TTL Vat
returns `error.ParkClockUnavailable`; with `Options.io`, clearing it falls back
to the value-stored Io clock. Because parked deadlines are absolute in the
effective clock's domain, changing from Io to a custom clock, from a custom
clock to Io, or between distinct custom handles while parks are live returns
`error.ParkClockInUse`. The same guard applies during the clock callback that
samples a new park's deadline, before its accounting is committed. Reinstalling
the same handle is a no-op; drain or expire the parks before changing domains.
A raw `ProvisionIndex` remains opt-in and defaults to a null TTL.

Bound TCP transports report terminal close automatically. If your integration
feeds a detached `HostPeer` with `pushFrame`, it owns that signal:

```zig
// On EOF, reset, or explicit terminal socket close. Repeated calls are safe.
host_peer.notifyTransportClosed();
```

Do not substitute `detachTransport()`; detach is a non-terminal transport
handoff. Missing the close notification leaves the peer logically connected
and delays release of its parked/embargo-queued holder reservations until later
peer teardown. Active provider-owned provisions intentionally survive the
notification so a recipient may still pick up a capability after the provider
transport disconnects.

For diagnosis, inspect `vat.stats()` and `peer.stats().parked_accepts` /
`.parked_accept_bytes`. Resource and timeout events are redacted: they identify
only the park resource or inbound answer ID, never recipient tokens, embargo
bytes, addresses, or frame contents.

---

## Automatic Third-Party Results Need an Attached VatNetwork

The default for an inbound `Call.sendResultsTo = thirdParty` remains
`.reject`. To let the peer route the result automatically, attach the
Experimental network first and then select the policy:

```zig
peer.attachVatNetwork(vat_network);
peer.setThirdPartyResultPolicy(.vat_network);
```

The network and every peer it returns are borrowed and must outlive the route.
They must also share the same owner thread; this option does not turn a
single-thread-affine `Peer` into a cross-thread router. A missing network makes
the automatic setup fail instead of silently dispatching a call whose results
cannot be delivered.

With `.vat_network`, application handlers use their normal
`sendReturnResults()` or `sendReturnException()` path. Do not also call
`sendReturnResultsSentElsewhere()`; the runtime emits that source-side marker
after committing the result on the introduced peer. Capability-bearing results
are remapped through pinned cross-peer proxies, and calls pipelined on the
synthetic answer wait for and replay from its terminal result.

Treat any error reported while sending `ThirdPartyAnswer` as terminal for the
introduced connection. The frame has no acknowledgement, so a transport cannot
distinguish "not delivered" from "delivered, then the local write reported an
error". The automatic route rolls its local setup back; closing the connection
is what guarantees that a recipient which already adopted the answer drains
its pending await. Result Returns have a stronger proof: a newly-created
reentrant Finish proves consumption even when the send callback reports a
trailing error.

If the application already owns its routing, keep `.application` and its
existing manual `sendReturnResultsSentElsewhere()` contract. The manual mode
cannot infer or reconstruct the delivered result for pipelining. `.reject`,
`.application`, and `.vat_network` are Experimental L3 policies; only the first
is the default, and current automatic-route evidence is Zig-to-Zig rather than
reference-implementation interoperability. Current focused evidence covers
capability remap, pipeline-before-result, direct proxy use/release, early
Finish (including queued-child and parameter-cap drain), reentrant
source/target deinit, source/target transport close without deinit, pre- and
post-delivery send-failure boundaries, every allocation-failure index, and
distinct network/source/target allocators.
