# Generated readers, builders, and generic views

The APIs below are unreleased additions. Use matching generator and runtime
revisions. The generated `brands()` views and runtime support in
`capnpc.generated_helpers` are Experimental. They extend the existing generated
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
unsupported `List(T)` declaration form. Generic interface clients and
method-local generic parameters remain erased. Use [binary reflection](reflection.md)
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

These are concrete struct/group paths. A capability hidden behind a generic
parameter, such as `Box(Service).value :T`, does not gain a specialized pipeline
accessor. Parameterized interface and method APIs remain erased.
