# Large Schema Dedup Assessment

Date: 2026-02-12  
Repo: `capnpc-zig`

## Scope

Outside-in assessment of generated code size and compiler pressure for very large schemas, with focus on dedup opportunities in generated Zig output.

Primary stress input used:

- One `.capnp` file containing 50,000 structs.
- Each struct shape: `struct S<N> { a @0 :UInt32; b @1 :UInt32; c @2 :UInt32; }`

## Reproduction Commands

### Generate 50k schema and compile with plugin

```bash
TMP=$(mktemp -d)
mkdir -p "$TMP/out"
{
  echo '@0x8f0b598d00f04001;'
  i=0
  while [ $i -lt 50000 ]; do
    printf 'struct S%d { a @0 :UInt32; b @1 :UInt32; c @2 :UInt32; }\n' "$i"
    i=$((i+1))
  done
} > "$TMP/huge.capnp"

capnp compile --src-prefix "$TMP" -I "$TMP" \
  -o /Users/nullstyle/Downloads/capnpc-zig/zig-out/bin/capnpc-zig:"$TMP/out" \
  "$TMP/huge.capnp"
```

### Compiler-pressure harness (recursive decl traversal)

This is intentionally stronger than normal usage. It forces semantic traversal of all generated declarations to measure scaling behavior.

```bash
BASE=/Users/nullstyle/Downloads/capnpc-zig/tmp_perf_rec_10000
mkdir -p "$BASE/out" "$BASE/.zig-cache" "$BASE/.zig-global-cache"

# ... generate huge.capnp similarly ...

capnp compile --src-prefix "$BASE" -I "$BASE" \
  -o /Users/nullstyle/Downloads/capnpc-zig/zig-out/bin/capnpc-zig:"$BASE/out" \
  "$BASE/huge.capnp"

cat > "$BASE/harness.zig" <<'EOF'
const std = @import("std");
const huge = @import("huge");

test "ref all decls recursive" {
    @setEvalBranchQuota(100_000_000);
    std.testing.refAllDeclsRecursive(huge);
}
EOF

zig test \
  --dep huge -Mroot="$BASE/harness.zig" \
  --dep capnpc-zig -Mhuge="$BASE/out/huge.zig" \
  -Mcapnpc-zig=/Users/nullstyle/Downloads/capnpc-zig/src/lib_core.zig \
  -O Debug
```

## Raw Measurements

For the 50k-struct generated file:

- File size: `129,001,676` bytes (`~123 MiB`)
- Line count: `3,600,013`
- Struct declaration count: `50,000`
- Per-struct block size: exactly `71` lines each
- Total bytes in struct blocks: `121,138,890` (`~93.9%` of file)
- Manifest line bytes: `7,812,519` (`~6.06%` of file)

Observed repetition counts (50k case):

- `const EnumListReader = ...` emitted: `50,000` times
- Reader `init(msg: *const message.Message)` emitted: `50,000` times
- Builder `init(msg: *message.MessageBuilder)` emitted: `50,000` times
- `raw ^ @as(u32, 0)` emitted: `150,000` times
- `@bitCast(value)) ^ @as(u32, 0)` emitted: `150,000` times

Byte contribution by pattern (50k case):

- Manifest JSON literal: `7,812,519` bytes (`6.06%`)
- Per-struct list helper aliases: `30,000,000` bytes (`23.26%`)
- Reader init signature lines: `2,950,000` bytes (`2.29%`)
- Builder init signature lines: `3,050,000` bytes (`2.36%`)
- Reader wrap signature lines: `2,950,000` bytes (`2.29%`)
- Builder wrap signature lines: `3,100,000` bytes (`2.40%`)
- Numeric read `raw` temp lines: `7,350,000` bytes (`5.70%`)
- Numeric read XOR lines: `6,750,000` bytes (`5.23%`)
- Numeric write XOR lines: `10,200,000` bytes (`7.91%`)
- Numeric write lines: `7,050,000` bytes (`5.47%`)

## Scaling Data

Codegen time (`capnp compile` invoking plugin):

- 10k structs (`~25M` file): `5.64s`
- 20k structs (`~49M` file): `13.92s`

Recursive decl-check harness (`zig test`, `refAllDeclsRecursive`):

- 5k structs (`~12M` file): `11.93s`
- 10k structs (`~25M` file): `21.59s`
- 20k structs (`~49M` file): `42.21s`

This indicates roughly linear-to-superlinear growth in compile pressure as declaration volume rises.

## Phase 1 Implementation Status (2026-02-12)

Implemented:

- Conditional list-helper alias emission in `src/capnpc-zig/struct_gen.zig` (scan struct/group fields and emit only required helper aliases).
- Zero-default fast paths in `src/capnpc-zig/struct_gen.zig` for numeric and enum getters/setters (skip XOR/temp code when default bits are zero).

Validation:

- `zig build test-codegen` passes after updating affected golden snapshots.
- Added targeted assertions in `tests/serialization/codegen_test.zig`:
  - no list-helper aliases for structs without list fields
  - no `^ @as(..., 0)` emission for zero-default numeric/enum fields

Measured impact after Phase 1 (same 50k synthetic schema):

- File size: `78,701,676` bytes (from `129,001,676`, delta `-50,300,000`, `-39.0%`)
- Line count: `2,700,013` (from `3,600,013`, delta `-900,000`, `-25.0%`)
- `typed_list_helpers` matches: `0` (from `400,000`)
- `^ @as(u32, 0)` matches: `0` (from `300,000` in this schema)
- Per-struct block size: `53` lines (from `71`)

Measured timing deltas (10k synthetic schema):

- Codegen (`capnp compile` invoking plugin): `4.63s` (from `5.64s`, `-17.9%`)
- Recursive decl harness (`zig test`, `refAllDeclsRecursive`): `20.25s` (from `21.59s`, `-6.2%`)

## Phase 2 Implementation Status (2026-02-12)

Implemented:

- Manifest emission gating in `src/capnpc-zig/generator.zig` via `Generator.setEmitSchemaManifest(bool)`.
- Plugin/runtime control in `src/main.zig`:
  - CLI token support: `--no-manifest`, `no-manifest`, `no_manifest`, `manifest=off/false/0`.
  - Environment variable support (recommended with `capnp compile`): `CAPNPC_ZIG_NO_MANIFEST=1`.

Notes:

- Cap'n Proto does not forward plugin CLI args in a generally usable way for this plugin invocation pattern, so env var control is the reliable path for outside-in usage.

Measured impact after Phase 2 (50k synthetic schema, on top of Phase 1):

- Default (manifest on): `78,701,676` bytes, `2,700,013` lines.
- `CAPNPC_ZIG_NO_MANIFEST=1`: `70,889,069` bytes, `2,700,008` lines.
- Delta from manifest gating alone: `-7,812,607` bytes (`-9.93%` vs Phase 1 default).

## Phase 3 Implementation Status (2026-02-12)

Implemented:

- Optional struct API profile in generator:
  - `full` (default): emits root `Reader.init()` and `Builder.init()`.
  - `compact`: omits those root init convenience methods, keeps `wrap(...)`.
- Wiring:
  - `src/capnpc-zig/generator.zig`: `Generator.setApiProfile(...)`.
  - `src/main.zig`: runtime/env control.
    - `CAPNPC_ZIG_API_PROFILE=compact|full`
    - `CAPNPC_ZIG_COMPACT_API=1|0` (boolean override)
    - direct token parsing for manual plugin invocation (`compact-api`, `profile=compact`, etc.).

Measured impact after Phase 3 (50k synthetic schema):

- `full` + manifest: `78,701,676` bytes, `2,700,013` lines.
- `compact` + manifest: `61,901,676` bytes, `2,200,013` lines.
- `compact` + no manifest: `54,089,069` bytes, `2,200,008` lines.
- Delta from compact profile alone (vs full+manifest): `-16,800,000` bytes (`-21.35%`) and `-500,000` lines.

## Phase 4 Implementation Status (2026-02-12)

Implemented:

- Optional shape sharing in `src/capnpc-zig/generator.zig`:
  - `Generator.setShapeSharing(bool)` enables reuse of the first emitted struct declaration when a later struct has an identical generated body.
  - Later matches emit `pub const X = Canonical;` aliases.
- Runtime/plugin control:
  - `CAPNPC_ZIG_SHAPE_SHARING=1` to enable.
  - token parsing for direct invocation (`shape-sharing`, `share-shapes`, etc.).

Measured impact after Phase 4 (50k synthetic schema):

- Full (manifest on, no shape sharing): `78,701,676` bytes, `2,700,013` lines.
- Full + shape sharing: `9,003,070` bytes, `100,065` lines.
- Shape sharing + compact + no manifest: `1,190,127` bytes, `100,050` lines.
- Alias count in 50k synthetic: `49,999`.

10k timing sample (`refAllDeclsRecursive` harness):

- Default: codegen `4.85s`, recursive decl test `20.81s`.
- Shape sharing enabled: codegen `5.64s`, recursive decl test `3.17s`.

Notes:

- Current shape sharing is exact-body matching; it targets output size and Zig compile pressure, not plugin codegen throughput.
- Because body matching currently generates full struct text before deciding to alias, plugin-side codegen time may increase in some cases.

## Ranked Dedup Opportunities

## 1) Conditional list helper alias emission (high impact, low risk)

Current source:

- `src/capnpc-zig/struct_gen.zig:145`

Issue:

- Every generated struct emits 8 typed-list helper aliases regardless of whether list fields exist.

Impact:

- ~30 MB in 50k file (~23.26%).

Action:

- Emit only the aliases actually used by that struct.
- Consider file-level shared aliases instead of per-struct aliases.

Status:

- Implemented (2026-02-12).

## 2) Zero-default numeric fast path (high impact, low risk)

Current source:

- `src/capnpc-zig/struct_gen.zig:249`
- `src/capnpc-zig/struct_gen.zig:1631`
- `src/capnpc-zig/struct_gen.zig:1710`

Issue:

- Numeric default handling emits XOR-default code even when default literal is zero.
- Produces repetitive `raw ^ 0` and `stored ^ 0` with temporaries.

Impact:

- Large repeated method-body text; measurable byte concentration in XOR-related lines.

Action:

- If default literal is `0`, use direct read/write path and skip XOR/temp locals.

Status:

- Implemented (2026-02-12).

## 3) Manifest emission strategy (medium-high impact, moderate risk)

Current source:

- `src/capnpc-zig/generator.zig:208`

Issue:

- Full schema manifest serialized into one giant JSON string constant per file.

Impact:

- ~7.8 MB single literal in 50k case (~6.06%).

Action options:

- Add generator mode to omit manifest (`-Dno-manifest` or plugin arg).
- Emit compact binary manifest and convert to JSON on demand at runtime.
- Split manifest into chunks to reduce giant single-line tokenization overhead.

Status:

- Implemented as runtime option (`CAPNPC_ZIG_NO_MANIFEST=1`) and generator API toggle.

## 4) Optional minimal API surface (medium impact, moderate risk)

Current source:

- Reader/Builder wrappers emitted in:
  - `src/capnpc-zig/struct_gen.zig:171`
  - `src/capnpc-zig/struct_gen.zig:176`
  - `src/capnpc-zig/struct_gen.zig:902`

Issue:

- Every struct always gets full `Reader`/`Builder` convenience wrappers (`init`, `wrap`, etc.).

Impact:

- Large repeated boilerplate footprint across many structurally similar declarations.

Action:

- Add generation profile:
  - `full` (current behavior)
  - `compact` (reduced convenience wrappers, explicit lower-level accessors)

Status:

- Implemented (2026-02-12) as opt-in compact profile.

## 5) Shape-based implementation sharing (highest upside, highest complexity)

Issue:

- Structs with identical field layout and types still emit fully duplicated implementations.

Action concept:

- Generate shared generic implementation by shape key.
- Emit per-struct thin type wrappers delegating to shared implementation.

Risks:

- More complex generated API internals.
- Potentially harder diagnostics and larger migration/testing surface.

Status:

- Implemented (2026-02-12) as an opt-in exact-body sharing mode.

## Additional Observations

- The 50k file has exact structural repetition: each struct block is identical except type name.
- The outside-in plugin path is functional for large inputs after stdin cap removal.
- For compile benchmarking, `refAllDeclsRecursive` required branch quota increase.

## Suggested Execution Order

1. Conditional list helper aliases.
2. Zero-default numeric fast path.
3. Manifest gating/compaction option.
4. Minimal API generation mode.
5. Shape-based shared implementation prototype behind feature flag.

## Validation Plan Per Step

- Add focused golden/codegen tests to assert reduced output patterns.
- Track generated byte size deltas on synthetic 5k/10k/20k schemas.
- Track compile-time deltas using recursive decl harness and existing test suite.
- Ensure generated API compatibility in default mode unless a compact mode is explicitly selected.
