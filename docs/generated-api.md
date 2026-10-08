# Generated readers, builders, and generic views

The APIs below shipped in v0.19.0. Use matching generator and runtime
revisions. The generated `brands()` and `Apply()` views and runtime support in
`capnpc.generated_helpers` and `capnpc.generic` are Experimental. They extend the existing generated
APIs. This guide describes the supported operations and their ownership rules.
[What is frozen in generated code](#what-is-frozen-in-generated-code) says
which generated declarations are a frozen contract.

## Build, inspect, and read a message

Generate the following schema as a module named `example` using the
[build integration guide](build-integration.md). This example uses the default
full API profile; compact output retains `Reader.wrap()` and `Builder.wrap()`.

```capnp
@0xcb999b000d00abc1;
struct Person {
  name @0 :Text;
  age @1 :UInt16 = 30;
}
struct Box(T) { value @0 :T; }
struct Link(T) {
  value @0 :T;
  next @1 :Link(T);
}
struct Root {
  people @0 :List(Box(Text));
  head @1 :Link(Text);
  person @2 :Person;
}
```

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const Root = @import("example").Root;

pub fn main(init: std.process.Init) !void {
    var message = capnpc.message.MessageBuilder.init(init.gpa);
    defer message.deinit();
    var root = try Root.Builder.init(&message);
    var person = try root.initPerson();
    try person.setName("Ada");
    try person.setAge(41);
    std.debug.assert(try person.getAge() == 41);
    try person.clearAge();
    std.debug.assert(try person.getAge() == 30);

    var people = try root.brands().initPeople(1);
    var first = try people.get(0);
    try first.setValue("Ada");
    var head = try root.brands().initHead();
    try head.setValue("first");
    var tail = try head.initNext();
    try tail.setValue("second");

    var storage = capnpc.generated_helpers.ReaderStorage.init(init.gpa);
    defer storage.deinit();
    const reader = try root.asReader(&storage);
    std.debug.assert(std.mem.eql(u8, "Ada", try (try reader.getPerson()).getName()));
    std.debug.assert(std.mem.eql(u8, "Ada", try (try (try reader.brands().getPeople()).get(0)).getValue()));
    std.debug.assert(std.mem.eql(u8, "second", try (try (try reader.brands().getHead()).getNext()).getValue()));
}
```

`asReader(&storage)` validates the current pointer graph and returns a generated
Reader borrowing the message builder's bytes. `ReaderStorage` owns a segment
index, not a serialized copy. Keep the storage at a stable address and keep both
owners alive. Rebinding or deinitializing the storage, or **any mutation of the
message builder**, invalidates its readers and borrowed slices. Obtain another
reader after mutation. Serializing into an independently owned `Message` is the
alternative when the reader must survive later builder changes.

## Mutable field access

Ordinary generated Builders provide getters for scalar, enum, Text, Data,
struct, list, AnyPointer, and capability fields. Getters apply defaults and guard
union arms. Text/Data slices borrow builder storage and expire on mutation.
Struct and list getters reopen existing values; `initXxx()` deliberately replaces
them. Mutable pointer defaults are copied into the destination message before
modification. Reopening smaller struct or struct-list fields grows their layout
while retaining unknown data and pointer sections.

Typed struct/list copy setters deep-copy their source. Self-copy is supported,
and allocation failures preserve the old destination pointer. Generated and
wire-decoded list readers retain `source_list` provenance, preserving unknown
physical struct fields even when copying through older primitive, Text, or
pointer views. Manually constructed readers with `source_list = null` copy only
the values they represent. `clearXxx()`
restores the schema default; clearing a union field also selects that arm.
`which()` returns the declared union tag or an error for an unknown ordinal;
`whichOrdinal()` preserves its raw value. An inactive union getter reports
`WrongUnionMember`. Use `enumOrdinals()` when forwarding unknown enum ordinals.

Builder mutators that can allocate spell their error sets so a consumer can pin
them. `initXxx` of a struct or list field, the Text and Data `setXxx` and the
capability setters return `message.BuildError`; copy setters, which also read
their source, return `message.CopyError` (a superset). Scalar `setXxx`,
`clearXxx`, `setXxxNull` and the `initXxx` of an AnyPointer, AnyStruct, AnyList
or interface field never allocate and keep their precise inferred sets: a
scalar setter's set is empty, nulling a pointer can only report a bad pointer
slot, and `initXxx` of such a field only returns a handle to the slot (the
allocation happens later, through the handle).
`setXxxServer` and the generic `Apply`/`brands()` views keep inferred sets.
Both named sets are listed in
[supported-surface.md](supported-surface.md#error-contract).

Growing a list can replace its storage. Reacquire previously obtained element
and nested builders afterward. This also applies when an older primitive or
pointer list is viewed through an evolved struct-list field. Larger unknown
sections survive compatible mutation and copying. Boolean lists cannot be
promoted to struct lists.

Generated Text getters validate UTF-8 and the wire NUL terminator. Generated
Text-list readers use `message.StrictTextListReader`, including nested list and
generic views. Malformed values report an error when read. The existing
low-level `TextListReader` API retains its compatibility behavior.

## Concrete generic applications

`brands()` supplies typed views for concrete data applications in both full and
compact profiles. It covers direct `List(Box(Text))` fields, nested lists and
generic applications, groups and inherited lexical bindings, imported types,
and finite recursive application graphs such as `Link(Text)`. Recursive lists
and alternating applications such as `Alternating(Text, Data)` also retain their
concrete field types. Pointer defaults, union guards, and layout growth apply to
these views. The ordinary erased accessors and each view's `raw()` remain
available.

Generation reuses an existing concrete wrapper for a recursive reference and
charges distinct application expansions against the specialization budget.
Traversal and generic binding depth remain bounded. Unbound parameters stay
erased; this does not add language support for the reference compiler's
unsupported `List(T)` declaration form. `Apply()` additionally exposes caller-selected
bindings for data and RPC types, as described below. Use [binary reflection](reflection.md)
when tooling needs the complete original brand expression.

## Generated RPC paths

Result pipelines can follow non-union struct fields and groups to capability
fields. Struct navigation adds a pointer-field transform and returns a fallible
nested view; group navigation adds no pointer transform. Recursive paths reuse
wrapper types, with at most 64 operations before `PipelineDepthLimit`. Union
arms are omitted because the future discriminant is unavailable.

Inherited methods with the same name receive declaring-interface suffixes, for
example `callPingFromFirst()` and `callPingFromSecond()`, with matching pipelined
calls and server VTable members. An interface's own `callPing()` keeps its name.
Diamond inheritance is deduplicated, and calls retain the original declaring
interface ID and method ordinal. An interface ID suffix disambiguates normalized
qualified-name collisions.

## Passing capabilities back

A peer's export ids and its import ids are separate spaces, and both start at
0: an import id is the id the remote chose for its export. So a peer that
exports capabilities and also imports them soon holds export N and import N at
once. A generated `Client` records which space its `cap_id` names in the
Experimental `origin` field (`rpc.peer.ClientOrigin`):

- Bootstrap and `resolveX` return Clients with `.imported`. `setXClient`
  writes them as the remote's own capability (`receiverHosted`), so a
  capability passed back to the vat that issued it reaches that vat's object,
  never a local export with the same id.
- `resolveX` returns a Client with `.exported` when the capability is one of
  this peer's own exports that came back home: a `receiverHosted`
  descriptor, or a `receiverAnswer` whose answer has already returned an
  export. Its `callX` runs the export's handler through the local loopback,
  its `release()` does nothing (it owns no import), and `setXClient` writes
  the export (`senderHosted`, or `senderPromise` while it is an unresolved
  promise export). It is valid while the export lives; written into a call
  or a Return after the export went, it fails that send with
  `error.UnknownExport`. Capabilities in the params and results of its
  calls arrive as this peer's own: the handler's
  `resolveX` gives a local Client for one of the peer's exports and an
  import Client for one of its imports. That import Client owns a loopback
  reference, so its `release()` sends no `Release` (see "Loopback calls
  with capabilities" in docs/supported-surface.md). Three kinds of call fail
  on it instead of going somewhere wrong: a call whose params carry a
  promise on one of the peer's own questions, written with
  `setXCapability` (`error.LoopbackPromisedCapabilityUnsupported`; the
  handler would read it as one of the peer's answers), `callXPipelined`
  (`error.LocalCapabilityPipelineUnsupported`; a local call's question never
  reaches the wire, so a pipelined call would go to the remote, which has
  no such answer), and a `StreamClient` streaming call
  (`error.LocalCapabilityStreamingUnsupported`).
- `resolveX` still fails with `error.UnexpectedCapabilityType` for a null
  capability, and for a `receiverAnswer` that resolves to an import: a
  Client for that import would own no reference, so its `release()` would
  spend one that someone else holds. A `receiverAnswer` whose answer has not
  returned fails with `error.PromiseUnresolved`.
- `Client.init(peer, id)` leaves `.unspecified`. `setXClient` then writes a
  bare id, and the outbound encoder picks a local export over an import with
  the same id, as before 0.23.0. Code that wraps its own export ids in
  `Client.init` keeps working. Code that wraps an import id it read from an
  `InboundCapTable` should set `.origin = .imported`.

`setXCapability` and the `set` of a `List(Interface)` builder take a raw
`message.Capability`, so they always write a bare id.

## Typed generic RPC applications

For a schema `interface Service(T) { echo @0 (value :T) -> (value :T); }`,
choose a concrete pointer codec when applying the generated type:

```zig
const capnp = @import("capnpc-zig");
const generated = @import("example");
const TextService = generated.Service.Apply(.{ .T = capnp.generic.Text });
const DataService = generated.Service.Apply(.{ .T = capnp.generic.Data });
```

`TextService.Echo.Params.Builder.setValue()` accepts Text, and the corresponding
result getter returns validated Text. `DataService` carries arbitrary bytes.
Their generated Reader/Builder types are distinct and can be used at the same
time. `Apply()` is available in full and compact output. The original `Service`
and `TextService.Raw` expose the existing erased API.

`TextService.Client.init(peer, cap_id).callEcho(ctx, build, callback)` accepts
compile-time build and callback functions using the typed method's `BuildFn`
and `Callback` signatures. The callback's `response.unwrap()` yields the typed
Results Reader; `response.raw` retains every ordinary response arm. The typed
adapter delegates to the existing call wrapper, which owns the question and
callback context. Borrowed results and capability tables have the same callback
lifetime as ordinary calls. Typed clients' `raw` member provides existing
options and lifecycle operations; a typed view does not acquire another
capability reference.

Create a server with `TextService.ServerAdapter(.{ .echo = handle }).init(ctx)`
and register it with `adapter.exportServer(peer)`. The handler receives typed
Params and Results. Keep the adapter and context alive while exported. Omitted
handlers return `Unimplemented`.

Method-local parameters are selected by the caller. For
`identity @0 [T] (value :T) -> (value :T)`, obtain method signatures from
`Factory.Apply(.{}).Identity.Apply(.{ .T = capnp.generic.Text })` and call
`client.callIdentity(.{ .T = capnp.generic.Text }, ctx, build, callback)`.
Named generic parameter/result structs are supported too. Server dispatch for
these methods uses the original erased signature: Cap'n Proto sends no runtime
type argument tags.

Imported and multiply inherited interfaces retain their branded ancestor types,
original interface IDs, and method ordinals. Equivalent diamonds share the same
typed method. When a valid schema inherits the same interface with conflicting
bindings, ambiguous typed shorthand methods are omitted; choose an explicit
application with `client.asAncestor(Ancestor.Apply(bindings))`. The raw inherited
method remains available.

For a result such as `Box(Service(Text))`, `callGetServicePipelined()` returns a
typed Results Pipeline. `pipeline.getBox().getValue()` (with `try` at each step)
reaches a specialized service client before the parent reply. Recursive generic
struct paths reuse their application type and enforce the same 64-transform
limit. Union paths remain unavailable before the discriminant is known.

Pointer bindings include `generic.Text`, `Data`, `AnyPointer`,
`Capability(Interface)`, `Struct(Type, data_words, pointer_words)`, and
`List(Element)`. Scalar and enum codecs are list elements, not valid standalone
bindings. Applied data Builders expose typed getters/setters and `raw()`;
struct initializers use `initXxx()`, list initializers use `initXxx(count)`.
`asReader(&ReaderStorage)` follows the borrowed-storage contract above. Pointer
defaults, union guards, and constrained pointer checks still apply.

## Copying capabilities between messages

Ordinary generated copy setters preserve a capability's numeric wire index.
They do not move its entry between RPC capability tables. When forwarding a
payload between different peers, use the Experimental
`destination_peer.clonePayloadAcrossPeers()` seam with the source peer and
inbound capability table. It remaps descriptors through proxy exports and
retains or pins the source capability as required. The caller owns the returned
proxy-ID list and must clean up unreferenced proxies if delivery is abandoned.
The automatic redirected-result flow performs that ownership bookkeeping,
including invocation, pipelining, release, and allocation-failure cleanup.

## Schema names that generated code uses for locals

Generated functions name their parameters, locals and captures plainly:
`self`, `value`, `msg`, `ctx`, `peer` and so on. Zig rejects a local that has
the name of a declaration in an enclosing container, and a schema names some
of those declarations: its constants and annotations, and the aliases of the
files it imports. So the generator checks each file it generates:

- A constant or annotation that a generated local would shadow takes a
  trailing underscore. `const ctx :UInt32 = 1;` beside an interface gives
  `pub const ctx_: u32 = 1;`.
- An import alias that a generated local would shadow takes the next free
  numeric suffix. An import of `user_ctx.capnp` gives
  `pub const user_ctx_2 = @import("user_ctx.zig");`.
- A doc comment on the renamed declaration says why.

The check is per container. A declaration that no local in its scope shadows
keeps its name, and a file with no collision does not change. Types are never
renamed, because other files name them. The type-valued locals in generated
code have quoted names with a space (`@"client adapter"`), which no schema
name can match.

## What is frozen in generated code

The plugin's output has its own freeze gate, apart from the library's
`docs/api-snapshot.txt`. `zig build check-generated-shape` runs the plugin on a
committed corpus of 26 CodeGeneratorRequests (`tests/generated_shape/requests/`)
in three profiles: full with reflection, compact, and `--no-reflection`. It
walks every public declaration of the generated files and renders each as one
line, `<profile>.<file>.<path>: <signature>`, into one of two files:

- `docs/generated-shape.txt` is **Stable and frozen**. A removed or changed
  line breaks code that consumers generate. It needs a Stable `### Breaking`
  CHANGELOG entry (not tagged `(Experimental)`) with a Migration paragraph,
  and a minor bump.
- `docs/generated-shape-experimental.txt` is Experimental. It must match the
  tree, but its lines can change in any minor release.

CI runs the gate on Linux, macOS and Windows. At release time,
`just check-release-drift` classifies the drift in both files (see
[RELEASING.md](../RELEASING.md)). The gate also fails when a corpus entry's
generated code does not compile, and it names the entry.

### The Stable families

A generated declaration is Experimental unless one of these families covers
it:

- Reader: `get*`, `has*`, `which`, `init` and `wrap`.
- Builder: `get*`, `set*`, `init*`, `has*`, `clear*`, `which` and `wrap`.
- Enums (schema enums, `WhichTag` and `Method`) and their enumerants.
- Constants, with their values: scalar, Text and Data schema constants, and the
  wire constants `interface_id`, method `ordinal` and `is_streaming`. A struct,
  list or AnyPointer constant pins its name and its `get()` signature. Its value
  bytes are private, so the gate cannot see them.
- `Client`: `init`, `release`, `fromBootstrap` and `call*`.
- `PipelinedClient`: `call*`.
- `Server` and its fields, the `VTable` fields, and the `Method` enum.
- `Response` and `BootstrapResponse`: the union, its variants and `unwrap`.
- The `Handler`, `Callback`, `BootstrapCallback` and `BuildFn` typedefs.

A container that declares a Stable member (a struct, its `Reader`, an
interface's `Client`) is Stable too.

Generated signatures spell the runtime's types and error sets. A Stable line
names only runtime declarations that are Stable in `docs/api-snapshot.txt`,
and generated types whose own line is Stable. So a change to a Stable runtime
error set, such as `message.BuildError`, moves Stable generated lines too.

### What stays Experimental

Everything no family covers stays Experimental, and so do the family members
whose signature names an Experimental type:

- `capnpSchema` (it names `reflection.SchemaRef`);
- `callXWithOptions` on `Client` and `PipelinedClient` (`rpc.peer.CallOptions`);
- the deferred-handler `VTable` fields, `x_deferred` (the generated
  `ReturnSender`);
- a streaming method's `Response` variants and `unwrap`
  (`rpc.generated.stream.StreamResult`);
- `callXPipelined`, which returns the generated `XPipeline`.

Other Experimental generated surface includes `StreamClient`, `brands()` and
the `Brands` views, `Apply()` and its instances, `asReader()`, `whichOrdinal()`,
`enumOrdinals()`, `pointerKinds()`, `CAPNP_SCHEMA_REQUEST`,
`CAPNP_SCHEMA_MANIFEST_JSON` and annotation metadata. Public implementation
detail is not frozen either: `CallContext`, `callBuild`, `pointer_indexes` and
the `_reader` / `_builder` fields.

### What the gate cannot see

- **Behavior.** The gate pins names, signatures and wire constants. A body
  change that keeps a signature is green. `just check-generated` and the
  golden files in `tests/golden/` cover generated bodies.
- **Schemas outside the corpus.** The gate sees only the declarations that the
  corpus schemas produce. A generated feature that no corpus schema exercises,
  for example `nestedLists()`, is not pinned.
- **Generic signatures.** A signature with an `anytype` parameter pins only its
  arity, and a generic function's inferred error set stays opaque.

`zig build generated-shape` rewrites both files. `zig build generated-shape --
--dump <file>` lists every line with its tier and kind. The tier rules and the
census of features the corpus must cover are in `tools/generated_shape.zig`.
