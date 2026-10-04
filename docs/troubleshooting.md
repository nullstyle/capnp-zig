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
generated-code size. A plugin run with `--output-dir=` (the build-step recipe)
ignores the environment variable; pass the argument with `codegen.addArg`.

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

---

## Consumer Build Pitfalls

These are build-graph problems a downstream package hits before any of its
code runs. Each one below was reproduced with a scratch consumer on tagged
Zig 0.17.0 (2026-10-03).

### One module root per binary

capnp-zig ships two module roots over one source tree: `capnpc-zig`
(`src/lib.zig`, the full surface) and `capnpc-zig-core` (`src/lib_core.zig`,
serialization and codegen only). With `-Dquic=true` (`.quic = true` as a
dependency option) the `capnpc-zig` module's root becomes `src/lib_quic.zig`
instead; the swap is at `build/modules.zig:42`. Zig requires every source file
to belong to exactly one module, so one compilation that imports two of these
roots fails:

```
src/serialization/message.zig:1:1: error: file exists in modules 'capnpc-zig' and 'capnpc-zig0'
src/serialization/message.zig:1:1: note: files must belong to only one module
src/lib.zig:8:29: note: file is imported here by the root of module 'capnpc-zig'
src/lib_core.zig:4:29: note: file is imported here by the root of module 'capnpc-zig0'
```

`capnpc-zig0` is the compiler's name for the second module, because both roots
import themselves as `capnpc-zig`. Two common ways to get here:

- Importing both `capnpc-zig` and `capnpc-zig-core` into one binary, directly
  or through a library module that picked the other one.
- Instantiating the dependency twice with different `quic` values, for example
  your `b.dependency("capnpc_zig", .{})` next to a library that asks for
  `.quic = true`. The notes then name `src/lib.zig` and `src/lib_quic.zig`.

**Fix:** pick one module for the whole binary: `capnpc-zig` if anything uses
RPC, `capnpc-zig-core` otherwise. Make every `b.dependency("capnpc_zig", ...)`
in the graph pass the same options (Zig reuses one instance per option set),
and pass the module down to library modules. A library that wraps capnp-zig
should forward `quic` from its own build options instead of hard-coding it.

### `capnpc-zig version skew` in a generated file

```
error: capnpc-zig version skew: this file was generated for codegen ABI 1, which needs the capnpc-zig 0.19.0 runtime or newer, but the imported runtime provides ABI 0. Upgrade the capnpc-zig dependency, or regenerate the file with the plugin that matches it.
```

The plugin that generated the file is newer than the capnp-zig runtime your
`build.zig.zon` pins. The usual cause is a `capnpc-zig` on `PATH` from a newer
checkout than the dependency. Every generated file checks the runtime's
`capnpc.codegen_abi` before it touches anything else, so this is the only
error you see instead of many failures inside the generated code.

**Fix:** move the dependency to the release the message names, or regenerate
with the plugin built from the release you pin. The reverse message,
"only supports ABI N and newer", means the runtime is newer than the generated
file: regenerate with the matching plugin.

### `hash mismatch ... N-V-__8AA...` on a pin that is correct

```
build.zig.zon:8:21: error: hash mismatch: manifest declares capnpc_zig-0.18.0-nUduFTdRNwBzlJgTt6x9lUwRGmpWzSVXrMSE-xFj_dND but the fetched package has N-V-__8AADdRNwBwMSFYBflRn85ej_SF0GhjIrwOallQrgFW
```

A hash that starts with `N-V-` is the form Zig gives a package with no
`build.zig.zon` (no name, no version). capnp-zig and quic both have one, so a
named package reported with an `N-V-` hash means the cache entry is bad, not
your pin.

**Cause (Zig 0.17.0):** a standalone `zig fetch <url>`, run outside a project
(for example to prime a cache or to read a hash), stores its recompressed
tarball at `<global cache>/p/<hash>.tar.gz` with the package nested one
directory too deep. The next `zig build` that needs that hash uses the cached
tarball, finds no manifest at its root, and computes an `N-V-` hash.
`zig fetch --save` inside a project, and `zig build` itself, write the cache
correctly. The fix belongs in Zig's build runner
(`lib/compiler/Maker/Fetch.zig`); see
[upstream/handoff-zig-fork-fetch-recompress-root.md](upstream/handoff-zig-fork-fetch-recompress-root.md).

**Confirm against a pristine cache before doubting the pin:**

```sh
ZIG_GLOBAL_CACHE_DIR="$(mktemp -d)" ZIG_LOCAL_PKG_DIR="$(mktemp -d)" zig build
```

If that builds, the pin is right and your cache is poisoned. Delete the bad
entry and the directory the failed fetch left, then let `zig build` fetch
again:

```sh
rm -f "$(zig env | sed -n 's/.*\.global_cache_dir = "\(.*\)",/\1/p')/p/<declared hash>.tar.gz"
rm -rf zig-pkg/N-V-__8AA...   # the exact hash the error printed
zig build
```

`zig env` prints the global cache directory (`.global_cache_dir`):
`$ZIG_GLOBAL_CACHE_DIR` if set, otherwise `~/.cache/zig` on Linux and macOS
(`$XDG_CACHE_HOME/zig` when that is set) and `%LOCALAPPDATA%\zig` on Windows.
To read a hash without touching your real cache, run `zig fetch` with a
throwaway `ZIG_GLOBAL_CACHE_DIR`.

### Where fetched packages live: `zig-pkg/` and `ZIG_LOCAL_PKG_DIR`

Zig 0.17.0 keeps each project's fetched dependency trees in `zig-pkg/` under
the build root, in addition to the compressed copies in the global cache's
`p/`. Two overrides exist, both read by the build runner
(`lib/compiler/Maker.zig`):

- `ZIG_LOCAL_PKG_DIR=<dir>` (listed by `zig env`), or
- `zig build --pkg-dir <dir>`.

Point them at a fresh directory, together with a fresh `ZIG_GLOBAL_CACHE_DIR`,
for the pristine check above. Without the pkg-dir override the check is not
pristine: Zig uses any tree the project's `zig-pkg/` already holds under the
declared hash without hashing it again. `zig-pkg/` is a build artifact: keep
it out of version control.
