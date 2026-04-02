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

Note: `which()` returns `error.InvalidEnumValue` if the discriminant does not match any known variant (e.g., the message was built with a newer schema).

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
