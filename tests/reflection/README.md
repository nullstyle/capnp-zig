# Reflection and wire conformance

Run from the repository root through the pinned Zig toolchain:

```sh
mise exec -- zig build test-reflection --summary all
mise exec -- zig build test-reflection-wasi --summary all
mise exec -- zig build test-reflection-cpp --summary all
```

The native gate is part of `zig build test` and `test-serialization`. It needs
only Zig. The WASI gate requires Wasmtime and executes the same reflection
consumer, plus the six registry tests, without granting filesystem access. The
explicit C++ gate requires `c++`, `pkg-config`, and Cap'n Proto development
headers/libraries; requesting it fails if these tools are missing. Its native
compiler uses the same C++ exception runtime as the installed Cap'n Proto
library. The oracle supports both KJ 1.x and KJ 2.x.

`generate.zig` feeds the checked-in `request.bin` to the project's actual
`Generator` API and emits fresh bindings into the Zig build cache. These bindings
are compiled for each target; they are not checked-in golden output. The
reference compiler request and its complete source corpus live together here.
Ordinary tests do not invoke `capnp`, Deno, another checkout, or the network.

The fixture corpus covers binary embeds, scalar and pointer defaults,
annotations, imported types, groups, unions, generic brands, nested enums,
interfaces and all scalar widths. `helper_names_test.zig` exercises legal names
that previously collided with generated helper views, including enum ordinals,
nested lists, constrained AnyPointer views and union groups.

`consumer.zig` verifies typed-to-dynamic and dynamic-to-typed reads and writes,
nonzero scalar XOR defaults, independent mutable copies of pointer defaults,
union activation and rejection, unknown enum ordinals, interface metadata,
constrained pointers, and null capabilities versus capability-table index zero.
It also grows physically smaller children written with an older schema.

`registry_test.zig` verifies layout validation before offset arithmetic, union
bounds and ownership after the caller overwrites and releases
input bytes. The consumer additionally checks malformed framing and duplicate
IDs. Native allocator checks guard both successful and rejected loads.

`list_evolution_test.zig` covers smaller inline struct elements, preservation of
unknown data/pointers, byte/16-bit/32-bit/64-bit/pointer/Void list upgrades, empty
lists, nested lists, double-far source lists, and replacement with a larger
source element. Existing list handles follow their updated parent pointers;
invalid indexes and length reads leave serialized bytes unchanged. Copying a
virtual one-byte struct into a standalone field retains the byte. Both populated
and empty Boolean lists are rejected because the reference format does not
permit this upgrade. `list_failure_test.zig` forces cloning failures after
replacement allocation and verifies that the original reachable list survives.
Unreachable allocation bytes are allowed after failure.

`generic_list_test.zig` separately covers the runtime metadata API's synthetic
bound `List(T)` descriptors and their pointer-list representation. The reference
compiler rejects `List(AnyPointer)`, so these are explicitly constructed metadata
cases, not additions to the valid schema corpus or C++ oracle.

`wire_test.zig` independently inspects pointer words emitted by the double-far
struct-list writer. It verifies LIST-kind landing descriptors, in-content tags,
empty and zero-width lists, source/landing/content aliases, preserved sentinels,
and reopened mutation. The serialization suite retains manually encoded legacy
Layout A reader fixtures separately from corrected writer expectations.

The optional `oracle.c++` compares every embedded canonical `schema::Node` with
its original request node, loads descriptors with `SchemaLoader`, and decodes
all emitted values and evolved lists dynamically. It checks unknown data and
pointer fields and the exact unsupported Boolean-list diagnostic. Native output
files are recorded under `.zig-cache/.../reflection-output/`; the build summary
shows the producing step.

## Updating the request fixture

Change files under `schemas/`, then use a reference Cap'n Proto compiler to
produce a temporary replacement request:

```sh
capnp compile --no-standard-import -Ischemas/include --src-prefix=schemas -o- \
  schemas/values.capnp schemas/nested/brands.capnp schemas/shared/common.capnp \
  schemas/reflection.capnp schemas/helper-names.capnp > request.new
```

Run that command from this directory. Replace `request.bin` only after successful
compilation, then run all three gates. Keep the complete request and all source
files together; do not rebuild unrelated golden bindings. The initial fixture
was compiled by the reference Cap'n Proto 2.0 development compiler used by
capnpc-wasm.
