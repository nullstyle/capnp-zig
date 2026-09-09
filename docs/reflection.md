# Binary schema reflection

Reflection is an unreleased, Experimental addition. Both `capnpc-zig` and
`capnpc-zig-core` export `reflection`; use matching generator and runtime
revisions. Existing typed accessors remain available.

## Generated descriptors

Each generated module exports `CAPNP_SCHEMA_REQUEST`, an unpacked binary
`CodeGeneratorRequest` containing the original schema Nodes sorted by ID. It
includes every Node supplied by the compiler, including imported types, groups,
constants, annotations, and generic brands. Requested-file records, source
comments, and compiler version are omitted. Copying the raw Nodes preserves
fields that the parsed Zig schema model does not expose.

Structs, groups, enums, interfaces, and ordinary/group struct Reader and Builder
types expose `capnpSchema`, a `reflection.SchemaRef` containing their ID and
binary bundle. Load one registry and reuse it for types from the same request.
The bundle includes dependencies even when their Zig modules are not generated.
Some compiler references can name Nodes absent from the request; resolving one
returns `SchemaNotFound`.

The JSON export-name manifest remains separate. It describes expected external
serde symbol names and does not contain the field/layout information used by
reflection. Neither a JSON codec nor dynamic RPC dispatch is added here.

## Read and build by field name

Given this schema, generate a module and bind it as `person` using the
[build integration guide](build-integration.md):

```capnp
@0x9eb32e19f86ee174;
struct Person {
  name @0 :Text;
  age @1 :UInt32 = 30;
}
```

This complete program constructs a message dynamically and reads it through
both dynamic and generated APIs:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const Person = @import("person").Person;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const registry = try Person.capnpSchema.load(allocator);
    defer registry.deinit();
    const person_schema = try (try Person.capnpSchema.resolve(registry)).asStruct();
    const name_field = try person_schema.field("name");
    std.debug.assert((try (try name_field.type()).proto()) == .text);

    var builder = capnpc.message.MessageBuilder.init(allocator);
    defer builder.deinit();
    const person = try capnpc.reflection.DynamicStruct.Builder.init(person_schema, &builder);
    try person.set("name", .{ .text = "Ada" });
    try person.set("age", .{ .uint32 = 37 });
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    var message = try capnpc.message.Message.init(allocator, bytes, .{});
    defer message.deinit();
    const dynamic = try capnpc.reflection.DynamicStruct.Reader.init(person_schema, &message);
    const typed = try Person.Reader.init(&message);
    std.debug.assert(std.mem.eql(u8, (try dynamic.get("name")).text, try typed.getName()));
    std.debug.assert((try dynamic.get("age")).uint32 == try typed.getAge());
}
```

`DynamicStruct.Builder.init` allocates a root; wrap an existing generated
builder with `.{ .schema = person_schema, .builder = typed._builder }` when
modifying a message already under construction. A dynamic Reader can likewise
wrap an existing `message.StructReader`.

## Inspect the schema

- `Registry.get(id)` resolves a Node. `Schema.proto()` exposes the parsed
  `schema.Node`; `Schema.raw()` exposes its original wire `schema::Node`.
- `Schema.asStruct()` provides `fields()`, `field(name)`, and
  `fieldByIndex(index)`. Fields expose their parsed/raw descriptors,
  `explicitOrdinal()`, `hadExplicitDefault()`, `type()`, and `groupSchema()`.
- Enum views provide `ordinal(name)` and `name(ordinal)`. Unknown wire ordinals
  remain available as `u16` values and have no declared name.
- Interface views expose own methods and `superclass(index)`. Method views
  expose parameter and result struct schemas; follow superclasses explicitly
  to inspect inherited methods.
- Parsed Nodes retain annotations and constant values. Use `raw()` for schema
  fields not represented by the parsed model.

Follow `Field.type()`, `Type.listElement()`, and `Type.asStruct()` to preserve a
field's generic application. `Type.asInterface()` and interface superclass and
method views retain their corresponding brand context. Looking up a generic
Node directly by ID gives its declaration with unbound parameters. The shared
resolver bounds recursion to 64 levels and rejects malformed bindings; valid
unbound parameters remain AnyPointer. Method-local implicit generic parameters
also retain their erased representation.

## Dynamic values and mutation

`get(name)` returns a tagged `reflection.Value`. Numeric values use their
schema-declared widths; reads and writes apply XOR defaults. Text, Data, lists,
and structs apply pointer defaults. Enum values preserve unknown ordinals.
Interface values are optional capability-table indices: `null` differs from
index zero.

`which()` returns the active union field, or null for an unknown discriminant;
`whichDiscriminant()` exposes its raw value. Reading an inactive field returns
`InactiveUnionField`. `has()` follows C++ non-null presence rules: active scalar
fields count as present, while pointer fields require a non-null pointer.
`hasNonDefault()` additionally checks scalar storage against its default.

`set()` validates the value type and concrete generic application, writes the
field, and selects its union arm. `clear()` restores the default and also
selects that arm. `initStruct()`, `initGroup()`, and `initList()` initialize field
values; `getStruct()` and `getList()` reopen them. Mutable pointer defaults are
cloned before modification so one message cannot change another's defaults.
Text writes require valid UTF-8, and constrained AnyPointer fields check their
wire pointer shape.

Dynamic lists expose indexed scalar, enum, struct, pointer, and nested-list
access. Incorrect value types return `TypeMismatch`; invalid indexes fail
without changing the list. Missing fields and schemas report `FieldNotFound`
and `SchemaNotFound`. Reflection's error sets remain Experimental.

Builders also provide `getScalar(name)`, `has(name)`, `hasNonDefault(name)`,
`which()`, and `whichDiscriminant()`. Scalar queries do not require borrowed
storage. For pointer reads use `asReader(&storage)`, where `storage` is a
`generated_helpers.ReaderStorage` owned by the caller:

```zig
var storage = capnpc.generated_helpers.ReaderStorage.init(allocator);
defer storage.deinit();
const before = try person.asReader(&storage);
std.debug.assert(std.mem.eql(u8, "Ada", (try before.get("name")).text));
try person.set("name", .{ .text = "Grace" });
// Mutation invalidates before and its slices. Rebind before reading again.
const after = try person.asReader(&storage);
std.debug.assert(std.mem.eql(u8, "Grace", (try after.get("name")).text));
```

Keep storage at a stable address. Its reader and slices borrow builder buffers;
any mutation or rebind invalidates them. Dynamic lists have the same explicit
storage conversion and indexed `getScalar()`. Reacquire element/nested builders
through the list after list growth.

## Resource limits and copying

`Registry.initWithOptions(allocator, bytes, options)` and
`SchemaRef.loadWithOptions(allocator, options)` accept these defaults:

| Option | Default | Accounting |
|---|---|---|
| `max_input_bytes` | 64 MiB | Checked before any input copy or allocation |
| `max_memory_bytes` | 128 MiB | Registry state and live arena backing allocations, including parsed descriptors and lazy defaults |
| `max_nodes` | 65,536 | Checked before descriptor parsing |
| `validation` | `Message.ValidationOptions{}` | Wire traversal, nesting, and logical collection limits, also applied to materialized defaults |

The existing `init()` and `load()` use these defaults. Input, memory, and node
limits report `SchemaInputLimitExceeded`, `SchemaMemoryLimitExceeded`, and
`SchemaNodeLimitExceeded`. A backing allocator failure remains `OutOfMemory`.
The lazy default cache retains stable reader identity and is subject to the same
memory budget. Synchronize shared registry access that can populate this cache.
Wire validation occurs at initialization or explicit validation; ordinary reads
do not charge a new per-access traversal budget.

`generated_helpers.CopyOptions` bounds new copy operations. Defaults are
8,388,608 work units, 8,388,608 reachable output words, 64 MiB additional live
backing allocation, and 64 levels of nesting. Work counts pointer visits, data
words, and logical list elements, including zero-width Void elements. Shared
targets are charged for every incoming edge because copying expands them.
Allocation accounting includes temporary snapshots and capacity growth beyond
the destination's storage at entry. Limit failures preserve the destination's
reachable value; unused allocations in the builder may remain until teardown.

Use `setPointerWithOptions`, `setStructWithOptions`, and
`getStructWithOptions` for bounded low-level helpers. Dynamic struct/list
Builders carry `copy_options`, inherited by child views. Aggregate list growth
and replacement use one budget, including existing sibling elements. Failed
struct/group replacement, growth, and self-copy preserve fields, physical null
presence, unknown sections, and union selection.
Group copies copy known group fields and their discriminant while retaining the
destination parent's unrelated/unknown fields; a group has no independent
allocation whose entire source parent could be assigned.

These are wire-copy operations: capability indices retain their source-table
meaning. They do not transfer ownership into a different RPC table. The explicit
RPC seam is `rpc.caps.table.payload_remap.clonePayloadWithRemappedCapsWithOptions`;
its mapper supplies destination identities and owns staged leases. Existing
`Peer.clonePayloadAcrossPeers` supplies proxy/pin ownership and release handling.
Encode its origin-tagged intermediate pointers with the ordinary RPC outbound
capability-table encoder before serialization. Mapping failure restores the
destination pointer; the caller must roll back any staged mapper effects.

## Schema evolution and lifetime rules

Registries own a copy of their input bytes and the parsed graph. The caller may
modify or release the input after `Registry.init` succeeds. Registry copies are
borrowed handles: call `deinit()` exactly once. Schema views borrow their
registry, and dynamic views also borrow their message or message builder; keep
both owners alive. Explicit brands passed to `Schema.asStructWithBrand()` also
borrow the caller's binding slices.

The registry lazily caches stable readers for schema-owned pointer defaults.
Sharing a registry between threads requires synchronization around that cache.
`Registry.defaultPointer()` accepts registry-owned values; clone a returned
reader before modifying its data. Use serialized messages or the explicit
`ReaderStorage` borrowing contract above when reading a mutable builder.

Ordinary struct copies preserve unknown fields. Reopening a smaller struct with
a newer schema expands its storage while preserving its contents. Struct lists
also expand when a newer schema or assigned element requires a larger physical
layout. Compatible byte, integer, pointer, and Void lists can evolve into
struct lists; Boolean lists report `TypeMismatch`, matching the reference
format's restriction.

A dynamic list handle follows its replacement storage after expansion.
Previously acquired element and nested builders still refer to the old storage
and must be reacquired through the list. Reading length and rejecting an invalid
index do not expand it. A failed expansion preserves the original reachable
list; temporary allocations remain owned by the message builder until teardown.

## Generation controls

The plugin emits binary metadata by default in full and compact API profiles.
`--no-reflection` omits it; `--no-manifest` independently omits the JSON manifest.
Binary metadata increases generated source size, and reflection-enabled shape
sharing preserves each type's distinct schema identity. To pass explicit plugin
options, use a saved or piped `CodeGeneratorRequest`, as shown in the
[build guide](build-integration.md#reflection-metadata-and-runtime-versions).

Programmatic `Generator.init(allocator, nodes)` retains its existing output
until `try generator.setSchemaRequest(bytes)` supplies the original unpacked
request corresponding to those Nodes. This Experimental setter validates and
encodes the metadata immediately, owns the result, and preserves the previous
metadata if the replacement fails. Its input may be released after success.
`setEmitReflection(false)` disables emission independently of the JSON manifest.
Existing Stable generator signatures are unchanged.

## Conformance

The reflection suite runs the same generated-code consumer natively and under
WASI. C++ independently compares each embedded Node's complete canonical bytes
with the compiler request, loads the schema graph, and reads messages written
natively through the dynamic API. Focused tests also cover invalid schema layouts, input ownership,
struct/list evolution, retained unknown fields, and failed list-expansion
rollback. The double-far list writer emits the reference-compatible layout;
legacy Layout A remains readable.

The mutation corpus adds 32 independently replayed C++ cases for generated and
dynamic writes, defaults, unions, copies, near/single-far/double-far messages,
and preserved unknown fields. Each emitted case includes initial/final messages
and a TSV operation/expectation record. `test-reflection-oracle-ablation`
deliberately changes an expectation and requires the oracle to reject it.
`test-reflection-wasi` and `test-reflection-cpp` are mandatory in the provisioned
Linux conformance job. Local success does not establish hosted platform results.
