# Generated readers, builders, and generic views

The APIs below are unreleased additions. Use matching generator and runtime
revisions. The generated `brands()` and `Apply()` views and runtime support in
`capnpc.generated_helpers` and `capnpc.generic` are Experimental. They extend the existing generated
APIs. This guide describes the supported operations and their ownership rules.

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
