# Getting Started: Serialization with capnpc-zig

This guide walks you through defining a Cap'n Proto schema and using the generated Zig code to serialize and deserialize messages.

> **Prefer to read code?** A complete, runnable version of everything below lives
> at [`examples/serialization_demo.zig`](../examples/serialization_demo.zig)
> (schema: [`examples/addressbook.capnp`](../examples/addressbook.capnp)). It
> builds an address book, writes it in both the plain and packed encodings, and
> reads each back zero-copy — exercising structs, an enum, lists, a union, and
> Text/Data fields. Run it with `zig build example-serialization`. Like the
> snippets here, it wires the runtime through the `capnpc-zig-core` module.

Every Zig block in this guide is compile-gated:
`tests/docs/serialization_getting_started_snippets_test.zig` compiles and runs
each one against the code the plugin generates from the schemas shown here,
through the `capnpc-zig-core` module (`zig build test-docs-snippets`), and
`zig build docs-smoke` fails if a block differs from that file.

## Prerequisites

- **Tagged Zig 0.17** on `PATH` (`mise install` provides the pinned version)
- **A Cap'n Proto schema compiler** to turn the schema into a
  `CodeGeneratorRequest`. CI verifies the plugin with the Cap'n Proto 2.0-dev
  WASM compiler that capnp-zig pins; native `capnp` 1.x (`brew install capnp`,
  `apt install capnproto`) is **unverified** with this plugin. See
  [The schema compiler](build-integration.md#the-schema-compiler).
- **No `capnpc-zig` install.** Your `build.zig` builds the plugin from the
  capnp-zig package you pin (step 3).

## 1. Define Your Schema

Create `schema/addressbook.capnp`. Its first line is a unique file ID, such
as `@0xf02316ceb4253eb9;`; generate your own with `capnp id`. Then add the
structs:

<!-- verbatim: examples/addressbook.capnp -->
```capnp
struct AddressBook {
  people @0 :List(Person);
}

struct Person {
  id @0 :UInt32;
  name @1 :Text;
  email @2 :Text;
  phones @3 :List(PhoneNumber);
  # Raw bytes — e.g. a tiny avatar thumbnail. Exercises the Data path.
  avatar @4 :Data;

  # Exactly one employment status is active at a time (unnamed union).
  union {
    unemployed @5 :Void;
    employer @6 :Text;
    school @7 :Text;
    selfEmployed @8 :Void;
  }

  struct PhoneNumber {
    number @0 :Text;
    type @1 :PhoneType;
  }

  enum PhoneType {
    mobile @0;
    home @1;
    work @2;
  }
}
```

This is the schema of the runnable example,
[`examples/addressbook.capnp`](../examples/addressbook.capnp).

Key points:
- Fields have ordinals (`@0`, `@1`, ...) that define their position in the binary layout
- Structs, enums, lists, and unions compose naturally
- A union holds exactly one of its fields at a time

## 2. Add capnpc-zig as a Dependency

Pin a tagged release. `zig fetch --save` downloads the tag's tarball and
records its `.url` and `.hash` in your `build.zig.zon`:

```bash
zig fetch --save https://github.com/nullstyle/capnp-zig/archive/refs/tags/v0.23.0.tar.gz
```

That adds an entry like this (the command fills in the hash; do not
hand-write it):

```zon
.dependencies = .{
    .capnpc_zig = .{
        .url = "https://github.com/nullstyle/capnp-zig/archive/refs/tags/v0.23.0.tar.gz",
        .hash = "capnpc_zig-0.23.0-nUduFUsRTgCn5wyHqoKC0ZXgBemZlU7y6bWBmH3RpW9U",
    },
},
```

A tag never moves, so the pin is reproducible; bump it deliberately and read
the CHANGELOG when you do. To build against a local checkout instead, use
`.path = "../capnp-zig"` in place of `.url` and `.hash`. If a fetch or build
fails on a hash you did not change, see
[Consumer build pitfalls](troubleshooting.md#consumer-build-pitfalls).

In your `build.zig`, take the runtime module from the dependency:

<!-- verbatim: tests/package_consumer/codegen/build.zig -->
```zig
// The runtime. Generated code imports it as "capnpc-zig".
const capnpc_dep = b.dependency("capnpc_zig", .{
    .target = target,
    .optimize = optimize,
});
const capnpc_core = capnpc_dep.module("capnpc-zig-core");
```

> Serialization-only code uses `capnpc-zig-core`; code that also uses RPC uses
> the full `capnpc-zig` module. See
> [supported-surface.md](supported-surface.md#modules--which-to-import) for the
> canonical module-choice rule.

## 3. Generate Zig Code

Compile the schema to a `CodeGeneratorRequest` and commit it next to the
schema, so `zig build` needs no schema compiler. The command below is native
`capnp`; [The schema compiler](build-integration.md#the-schema-compiler) shows
the same step with the CI-verified WASM compiler.

```bash
capnp compile -o- --src-prefix=schema schema/addressbook.capnp > schema/addressbook.request.bin
```

Then let your build run the `capnpc-zig` plugin from the same pinned package,
never a PATH binary. A plugin from another revision can emit code your runtime
does not compile.

> **This recipe needs capnp-zig v0.19.0 or later.** It passes the plugin's
> `--output-dir=` flag, which v0.18.0 does not have. A v0.18.0 plugin ignores
> the flag and writes `addressbook.zig` into your project root. The build then
> fails because `capnp-gen/addressbook.zig` is not found.

These lines are an excerpt of the `build.zig` that `zig build package-preflight`
runs from the release archive:

<!-- verbatim: tests/package_consumer/codegen/build.zig -->
```zig
const capnpc_host = b.dependency("capnpc_zig", .{
    .target = b.graph.host,
    .optimize = .ReleaseSafe,
});
const codegen = b.addRunArtifact(capnpc_host.artifact("capnpc-zig"));
codegen.setStdIn(.{ .lazy_path = b.path("schema/addressbook.request.bin") });
const gen_dir = codegen.addPrefixedOutputDirectoryArg("--output-dir=", "capnp-gen");

const addressbook = b.createModule(.{
    .root_source_file = gen_dir.path(b, "addressbook.zig"),
    .target = target,
    .optimize = optimize,
    .imports = &.{.{ .name = "capnpc-zig", .module = capnpc_core }},
});
```

Import both into your executable as `capnpc-zig` and `addressbook`. The
generated `addressbook.zig` contains `Reader` and `Builder` types for each
struct, plus Zig enums for each Cap'n Proto enum. It lives in the build cache,
so it always matches your pinned runtime. Codegen is quiet by default.

If you commit the generated file instead, add the `gen` and `gen-check` steps
from the [canonical build.zig](build-integration.md#canonical-buildzig) and run
`zig build gen-check` in CI. It fails, with the diff, when the checked-in copy
differs from the pinned plugin's output.

## 4. Build a Message

Every generated struct has a `Builder` type for writing and a `Reader` type for reading. Here's how to build a `Person` message:

<!-- verbatim-file: tests/docs/getting_started/build_person.zig -->
```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;
const addressbook = @import("addressbook");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // 1. Create a MessageBuilder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // 2. Initialize the root struct
    var person = try addressbook.Person.Builder.init(&builder);

    // 3. Set fields
    try person.setId(1);
    try person.setName("Alice Smith");
    try person.setEmail("alice@example.com");

    // 4. Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // `bytes` now holds the framed Cap'n Proto message: write it to a file
    // or a socket, or read it back as in step 5.
}
```

### How field setters work

- **Primitives** (`setId`) write directly into the struct's data section — no allocation
- **Text/Data** (`setName`, `setEmail`) allocate space in the message segment and write a pointer
- All setters return `!void` — text/data setters can fail on allocation; primitive setters are infallible but return `!void` for API consistency

## 5. Deserialize and Read

Reading is zero-copy — the `Reader` accesses bytes directly from the message buffer:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// 1. Parse the framed message (`.{}` uses the default validation limits)
var msg = try message.Message.init(allocator, bytes, .{});
defer msg.deinit();

// 2. Get a typed Reader for the root struct
const person = try addressbook.Person.Reader.init(&msg);

// 3. Read fields
const id = try person.getId(); // u32
const name = try person.getName(); // []const u8, points into msg's bytes
const email = try person.getEmail(); // []const u8

// The values step 4 wrote
std.debug.assert(id == 1);
std.debug.assert(std.mem.eql(u8, name, "Alice Smith"));
std.debug.assert(std.mem.eql(u8, email, "alice@example.com"));
```

### Important: Reader lifetimes

The slices returned by `getName()` and `getEmail()` point directly into
`bytes`, the buffer the `Message` was parsed from. Keep both the `Message` and
`bytes` alive as long as you need the data.

## 6. Enums

Cap'n Proto enums generate standard Zig enums backed by `u16`:

**Schema:**
<!-- verbatim: examples/addressbook.capnp -->
```capnp
enum PhoneType {
  mobile @0;
  home @1;
  work @2;
}
```

**Generated** (in `examples/addressbook.zig`; `capnpSchema` is the
[reflection](reflection.md) handle):
<!-- verbatim: examples/addressbook.zig -->
```zig
pub const PhoneType = enum(u16) {
    Mobile = 0,
    Home = 1,
    Work = 2,
    pub const capnpSchema = capnpc.reflection.SchemaRef{ .id = 0xf484ec504318ef92, .encoded_request = _capnp_file.CAPNP_SCHEMA_REQUEST };
};
```

**Usage** (`phone` is a `Person.PhoneNumber.Builder`):
<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// Writing
try phone.setType(.Mobile);

// Reading (Readers and Builders both have getType)
const phone_type = try phone.getType(); // a PhoneType
const label = switch (phone_type) {
    .Mobile => "mobile",
    .Home => "home",
    .Work => "work",
};
```

Generated enums stay exhaustive: `getType()` returns
`error.InvalidEnumValue` if a newer peer sends an enumerant this schema does
not know. A proxy that needs to preserve that value can use the parallel raw
ordinal view without changing normal typed application code:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const ordinal = try phone.enumOrdinals().getType();
try forwarded_phone.enumOrdinals().setType(ordinal);
```

The ordinal accessors return logical `u16` values, so enum defaults are applied
for you. Enum lists have the same view (see [Enum lists](#enum-lists)).

## 7. Lists

The address book has one kind of list, a list of structs. The other field
shapes in the rest of this guide come from a second small schema,
`tests/docs/schema/guide.capnp`, which the snippet test generates and compiles
the same way:

<!-- verbatim-file: tests/docs/schema/guide.capnp -->
```capnp
@0x8184fd32ded3382c;

# Field shapes for docs/getting-started-serialization.md that the address
# book does not have.

struct Profile {
  scores @0 :List(UInt32);
  tags @1 :List(Text);
  colors @2 :List(Color);
  matrix @3 :List(List(UInt16));
  address @4 :Address;
  values @5 :AnyList;
}

struct Address {
  street @0 :Text;
  city @1 :Text;
  zipCode @2 :UInt32;
}

enum Color {
  red @0;
  green @1;
  blue @2;
}

struct Shape {
  color @0 :Color;

  union {
    circle @1 :Float64; # radius
    rectangle :group {
      width @2 :Float32;
      height @3 :Float32;
    }
  }
}
```

In the snippets that use it, `builder` is a `Profile.Builder` and `reader` is a
`Profile.Reader`.

### Primitive lists

Writing:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// Init the list with a count, then set each element
const scores = try builder.initScores(3); // List(UInt32), 3 elements
try scores.set(0, 100);
try scores.set(1, 95);
try scores.set(2, 87);
```

Read it back:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const scores = try reader.getScores();
var total: u32 = 0;
for (0..scores.len()) |i| {
    total += try scores.get(@intCast(i));
}
```

### Struct lists

With the address book, where `person` is a `Person.Builder`:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// init returns a typed list builder
const phones = try person.initPhones(2);
var phone0 = try phones.get(0);
try phone0.setNumber("555-1234");
try phone0.setType(.Mobile);
var phone1 = try phones.get(1);
try phone1.setNumber("555-5678");
try phone1.setType(.Work);
```

And reading, where `person` is a `Person.Reader`:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const phones = try person.getPhones();
var work: usize = 0;
for (0..phones.len()) |i| {
    const phone = try phones.get(@intCast(i));
    if (try phone.getType() == .Work) work += 1;
}
```

### Text lists

Writing:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const tags = try builder.initTags(2);
try tags.set(0, "zig");
try tags.set(1, "capnproto");
```

Reading:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const tags = try reader.getTags();
const tag = try tags.get(0); // []const u8
```

### Enum lists

Enum lists read and write typed values with `get` and `set`. To forward an
enumerant this schema does not know, use the raw ordinals:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const colors = try reader.getColors();
const first_ordinal = try colors.getOrdinal(0);
const forwarded_colors = try builder.initColors(colors.len());
try forwarded_colors.setOrdinal(0, first_ordinal);
```

Existing typed getters/setters and enum-list `raw()` accessors remain
available.

### Nested lists

For a field such as `matrix :List(List(UInt16))`, generated Readers and
Builders keep the original raw pointer-list methods and add a typed recursive
view. Typed construction:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const rows = try builder.nestedLists().initMatrix(2);
const first = try rows.init(0, 3);
try first.set(0, 10);
try first.set(1, 20);
try first.set(2, 30);
try rows.setNull(1);
```

Typed reading:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const rows = try reader.nestedLists().getMatrix();
const first = try rows.get(0);
std.debug.assert(try first.get(1) == 20);

// A null inner-list pointer reads as an empty list, but remains observable.
std.debug.assert(try rows.isNull(1));
std.debug.assert((try rows.get(1)).len() == 0);
```

The same shape recurses for deeper schemas: a
`List(List(List(Text)))` reader uses repeated `get(index)`, and its builder uses
repeated `init(index, count)`. `initXxxInSegment` and each nested
`initInSegment` support explicit segment placement. Enum terminals retain
typed `get` / `set` plus ordinal forwarding, and struct terminals return their
generated Reader/Builder types.

Compatibility is additive. Existing `getMatrix()` / `initMatrix()` still
return `message.PointerListReader` / `message.PointerListBuilder`, and every
typed recursive wrapper has `raw()`. If a referenced struct layout cannot be
resolved during generation, that terminal falls back to the raw struct-list
API; an unresolved enum ID becomes an ordinal `u16` list.

## 8. Nested Structs

With `profile` a `Profile.Builder`:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// initAddress allocates a nested struct in the message
var address = try profile.initAddress();
try address.setStreet("123 Main St");
try address.setCity("Springfield");
try address.setZipCode(62704);
```

And reading, with `profile` a `Profile.Reader`:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const address = try profile.getAddress();
const street = try address.getStreet();
```

## 9. Unions

Cap'n Proto unions use a discriminant field to track which variant is active.
The address book's `Person` has one (`unemployed`, `employer`, `school`,
`selfEmployed`); this section uses the guide schema's `Shape`, whose
`rectangle` arm is a group.

**Schema:**
<!-- verbatim: tests/docs/schema/guide.capnp -->
```capnp
struct Shape {
  color @0 :Color;

  union {
    circle @1 :Float64; # radius
    rectangle :group {
      width @2 :Float32;
      height @3 :Float32;
    }
  }
}
```

**Generated:** the plugin emits, for `Shape`:

- `Shape.WhichTag`, an enum with one tag per arm: `.circle` and `.rectangle`.
- `Shape.Rectangle`, with its own `Reader` and `Builder`, for the group arm.
- On `Shape.Reader`: `which()`, `whichOrdinal()`, `getColor()`,
  `getCircle()`, and `getRectangle()`, which returns `!Rectangle.Reader`.
- On `Shape.Builder`: `setColor()`, `setCircle()`, and `initRectangle()`,
  which returns a `Rectangle.Builder`. Each union setter and init also sets
  the discriminant.

**Usage.** Here `builder` is a `MessageBuilder` and `msg` a `Message`, as in
sections 4 and 5. Writing a circle:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
var shape = try Shape.Builder.init(&builder);
try shape.setColor(.Red);
try shape.setCircle(5.0); // sets the discriminant to .circle
```

Writing a rectangle (a group arm):

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
var shape = try Shape.Builder.init(&builder);
try shape.setColor(.Blue);
var rect = shape.initRectangle(); // sets the discriminant to .rectangle
try rect.setWidth(10.0);
try rect.setHeight(20.0);
```

Reading:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// Always check which() first
const shape = try Shape.Reader.init(&msg);
return switch (try shape.which()) {
    .circle => blk: {
        const radius = try shape.getCircle();
        break :blk std.math.pi * radius * radius;
    },
    .rectangle => blk: {
        const rect = try shape.getRectangle();
        const w = try rect.getWidth();
        const h = try rect.getHeight();
        break :blk w * h;
    },
};
```

### Union Default-Arm Semantics

Generated unions follow the Cap'n Proto discriminant value directly:

- A newly initialized struct starts with discriminant `0`, so the first union arm is active by default.
- A message that never explicitly set a union arm may still report that first arm at read time.
- Always branch on `which()` before calling arm-specific getters.
- Always call `setXxx()` or `initXxx()` before writing fields for a non-default arm.

This avoids subtle bugs where application code assumes a union arm was explicitly set when it was only the implicit zero/default discriminant.

## 10. Packed Encoding

Cap'n Proto supports a packed encoding that compresses zero bytes, which is common in sparse messages:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
// Serialize to packed format
const packed_bytes = try builder.toPackedBytes();
defer allocator.free(packed_bytes);

// Deserialize from packed format
var msg = try message.Message.initPacked(allocator, packed_bytes, .{});
defer msg.deinit();
```

Packed encoding is useful when sending messages over the network or storing them on disk. Typical compression ratios are 2-4x for sparse messages.

## 11. Schema Evolution

Cap'n Proto is designed for safe schema evolution. You can:

- **Add new fields** to the end of a struct (with new ordinals)
- **Read old messages** with new code — new fields return their default value (0, false, "")
- **Read new messages** with old code — unknown fields are silently ignored

This works because readers return type defaults for any field that falls outside the struct's data section. No versioning metadata is needed.

For pointer fields, use the generated `hasXxx()` method when the distinction
between absent and present-but-empty matters. This returns `null` for an
absent email:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
if (person.hasEmail()) {
    return try person.getEmail(); // present, but may still be ""
}
return null;
```

`hasXxx()` is generated on Readers and Builders for Text, Data, struct, list,
AnyPointer, and interface fields. It is a structural check: null pointers and
fields outside an older struct layout are absent; explicitly encoded empty
values are present. A non-null schema default does not make a null slot present,
and inactive union arms report false. Pointer getters still perform their normal
validation.

For unions, typed `which()` remains exhaustive. Use `whichOrdinal()` when an
older proxy must observe an arm introduced by a newer schema; it returns the
raw logical `u16` discriminant and never turns an unknown arm into a known one.

### Pointer-kind and brand sidecars

Code generated before pointer-kind fidelity exposed `AnyPointer`, `AnyStruct`,
`AnyList`, and bare `Capability` fields through the same erased
`AnyPointerReader` / `AnyPointerBuilder` accessor. Those accessors are still
present. For constrained slots, the parallel `pointerKinds()` view preserves
the schema promise without making old callers change. This copies the
guide schema's `values :AnyList` field as a list of `UInt32`:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
const any_list = try reader.pointerKinds().getValues();
const words = try any_list.getU32List();

const list_slot = try builder.pointerKinds().initValues();
const output = try list_slot.initU32List(words.len());
for (0..words.len()) |i| try output.set(@intCast(i), try words.get(@intCast(i)));
```

`AnyListReader.raw()` and each Builder shape wrapper return the erased
AnyPointer view; the Reader's AnyStruct and Capability results are already the
concrete runtime readers. A null `AnyList` behaves as an empty list while
`isNull()` preserves the structural distinction. Builder `getXxx()` methods
reopen a value without replacing it; `initXxx()` and capability setters are the
explicit replacement operations. Unconstrained `AnyPointer` has no narrower
promise and therefore receives no extra shape accessor.

The same behavior holds inside groups and unions and in both codegen profiles.
An inactive union arm returns `error.WrongUnionMember`; a non-null pointer with
a wire-distinguishable wrong kind fails instead of being reinterpreted.
Cap'n Proto's layout-A double-far zero-offset struct tag is identical to an
empty inline-composite list tag, so that one empty representation is inherently
ambiguous. Builder getters also reopen existing single-far AnyStruct and
double-far AnyList pointers, so a segment boundary does not force replacement.

Generic schema fidelity follows the same additive rule. The request parser
keeps node and method parameters, brand scopes/bindings, named-type,
superclass/method, and annotation-use brands, plus AnyPointer parameter kinds
in `schema.TypeMetadata` / `TypeExpression`, while the frozen `schema.Type`
union remains unchanged. Generated annotation constants intentionally retain
their legacy id/value projection; tooling that needs an annotation-use brand
should inspect the parsed request. One allocation-free internal resolver is
shared by validation and codegen: it composes `.bind` / `.inherit`, validates
lexical scope, exact arity and parameter indexes, and rejects a cycle or depth
beyond 64 as `error.InvalidSchema`.

A finite concrete generic data-struct field may generate a `brands()` view
whose per-field wrapper reopens the existing target through `getXxx()`,
materializes it through the Builder's `initXxx()`, and supports arbitrary-depth
lists, enum/Text/Data/struct/interface terminals, nested branded structs, and
inherited lexical bindings. Generic struct applications may themselves be list
terminals, including cross-file imported applications and terminals. Reader and
Builder views preserve pointer defaults (recursively materializing them before
Builder mutation), group/union guards, enum forwarding, null structs/lists, and
near/far reopening. Every wrapper retains `raw()` for the legacy target.

This is not general Zig generic specialization. The generator deliberately
omits `brands()` for valid unbound, recursively infinite, or otherwise
non-finite applications. Generic interface clients and implicit generic RPC
methods stay erased. In those cases, keep using the erased accessor and inspect
the schema metadata if tooling needs the original type expression.
Names that would collide with the generated `Brands` / `PointerKinds` surface
are rejected during generation rather than emitted ambiguously.

Schema-aware validation and canonicalization have additive concrete-root entry
points:

<!-- verbatim: tests/docs/serialization_getting_started_snippets_test.zig -->
```zig
try capnpc.schema_validation.validateMessageWithBrand(
    &msg,
    nodes,
    root_node,
    root_brand,
    .{},
);
```

`canonicalizeMessageWithBrand()` and
`canonicalizeMessageFlatWithBrand()` take the same root brand. Existing entry
points use an empty root brand but still honor concrete nested metadata. Valid
unbound parameters keep erased compatibility; malformed brand graphs return
`error.InvalidSchema`; scalar generic bindings are likewise invalid. The
schema-free `canonical.*` API is unaffected.

Generated specialization is separately bounded by
`CodegenBudget.max_brand_specializations` (default 4096). Maintainers invoking
the plugin directly can set `max-codegen-brand-specializations=` or
`CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS`. With `--output-dir=`, as in the
build recipe above, only the argument applies.

## Quick Reference

| Schema Type | Zig Read Type | Zig Write Method |
|---|---|---|
| `Bool` | `bool` | `setBoolField(bool)` |
| `UInt8..UInt64` | `u8..u64` | `setField(u32)` |
| `Int8..Int64` | `i8..i64` | `setField(i32)` |
| `Float32/Float64` | `f32/f64` | `setField(f32)` |
| `Text` | `[]const u8` | `setField([]const u8)` |
| `Data` | `[]const u8` | `setField([]const u8)` |
| `List(T)` | typed list reader | `initField(count)` |
| `List(List(T))` | raw pointer-list accessor, or typed `nestedLists().getField()` | raw `initField(count)`, or typed `nestedLists().initField(count)` |
| `AnyStruct` / `AnyList` / bare `Capability` | erased accessor, or constrained `pointerKinds()` view | erased accessor, or `pointerKinds()` get/init/set |
| finite concrete branded data struct | erased target accessor, or supported `brands()` view | erased target accessor, or `brands()` get/init |
| `struct` | `StructName.Reader` | `initField()` |
| `enum` | `EnumName`, or forwarding `u16` via `enumOrdinals()` | `setField(.Variant)` or `enumOrdinals().setField(value)` |
| `union` | check `which()`; inspect unknown arms with `whichOrdinal()` | `setVariant()`/`initVariant()` |
