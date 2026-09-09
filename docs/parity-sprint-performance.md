# Parity sprint performance receipt

Measured on 2026-09-08 (Alaska), starting 2026-09-09 01:52:36 UTC, against the
pre-sprint revision `68ad72f4d4b0739e4fa1a205c92aef840a38e702`. The benchmark
completed all 16 generation/consumer configurations and 60 runtime samples.
Both source trees stayed unchanged throughout the run, and each runtime case
produced the same checksum across versions and repetitions.

The main costs are larger generated RPC source and the preflight graph traversal
required by bounded dynamic copying. Ordinary scalar/text reads and generated
copies remain close to baseline. This is a local performance receipt, not a
claim of equivalent performance or maturity across language implementations.

## Reproduce

Run from the repository root with the pinned tools. Prepare the baseline source
without modifying the working tree or vendor references:

```sh
mkdir -p .zig-cache/parity-sprint/baseline
git archive 68ad72f4d4b0739e4fa1a205c92aef840a38e702 src | tar -x -C .zig-cache/parity-sprint/baseline
mise exec -- python3 tools/reflection_performance.py --baseline-root .zig-cache/parity-sprint/baseline --output .zig-cache/parity-sprint/performance
```

The runner uses three repeated samples by default, builds the same benchmark
source against both runtimes, executes every generated consumer, and rejects a
run if its source trees or benchmark change.
`mise exec -- zig build bench-reflection -Doptimize=ReleaseSafe` runs the current runtime cases alone.
[The durable JSON receipt](parity-sprint-performance.json) contains all samples,
CPU and elapsed timings, checksums, environment details, and source/fixture
hashes. Reproduction writes commands, requests, generated files, and executables
under the chosen cache directory.

## Measurement contract

The host was an Apple M5 Max with 18 cores and 128 GiB RAM, running macOS 26.6.2.
Both versions used Zig `0.17.0-dev.1683+5ceec001b`, `ReleaseSafe`, and unstripped
binaries. Schema requests were produced by Cap'n Proto 1.5.0; the reference
schema gitlink was `f8498184d20664406fc61bb91411a0d86e726e71`.

Native and downstream conformance builds ran concurrently. Elapsed timings
therefore include scheduling and resource contention; process CPU medians are
preferred for the runtime comparison. CPU times do not remove cache, thermal,
or shared-memory effects. These three-sample observations do not establish a
statistical regression threshold. These numbers are review measurements, not CI timing gates or hosted-platform
acceptance criteria.

“First” means the first process invocation for that configuration. OS caches
were not purged. Only the first configuration launches the newly built generator
for the first time; later configurations can reuse cached code pages. First
consumer compilation uses a fresh local Zig cache and the shared default global
cache; repeats invoke the identical command. These are reproducible startup and
repeat measurements, not an isolated cold-cache experiment.

The generation matrix uses `helper-names.capnp` (6,304-byte request) and the
pinned reference `capnp/test.capnp` (185,840-byte request). Both full and compact
profiles are checked with reflection on and off. Source size sums generated Zig
files. Binary size measures a minimal one-field consumer that loads the registry
when reflection is enabled; it does not instantiate every generated RPC API.
Unstripped binary sizes also include debug/path information.

## Generated source and consumer size

Values are **baseline → current**, in bytes.

| Schema / profile / reflection | Generated source | Consumer binary |
|---|---:|---:|
| small / full / on | 49,402 → 49,402 | 610,936 → 611,384 |
| small / full / off | 23,066 → 23,066 | 529,240 → 529,240 |
| small / compact / on | 47,386 → 47,386 | 610,936 → 611,400 |
| small / compact / off | 21,050 → 21,050 | 529,240 → 529,240 |
| large / full / on | 2,789,748 → 3,216,677 | 709,432 → 709,880 |
| large / full / off | 2,041,390 → 2,468,319 | 529,240 → 529,240 |
| large / compact / on | 2,728,335 → 3,155,264 | 709,432 → 709,896 |
| large / compact / off | 1,979,977 → 2,406,906 | 529,240 → 529,240 |

Small-schema source is unchanged. The large schema adds 426,929 bytes
(+15.3% to +21.6%, depending on profile/reflection) as generated interfaces and
methods gain concrete generic applications. Reflection-enabled consumer binaries
add 448–464 bytes; plain consumer binaries are unchanged. The source growth remains a cost for
consumers that parse or distribute the generated API, even when unused methods
do not contribute to the final executable.

## Generation and compilation timing

Values are **baseline → current**. Repeat values are medians of three process
invocations. First-run generator elapsed times are retained because startup can
be substantially longer than CPU time.

| Schema / profile / reflection | Generation first elapsed, ms | Generation repeat elapsed, ms | Generation repeat CPU, ms |
|---|---:|---:|---:|
| small / full / on | 230.66 → 234.21 | 3.38 → 4.70 | 2.37 → 2.73 |
| small / full / off | 3.50 → 3.33 | 3.34 → 3.02 | 2.06 → 2.03 |
| small / compact / on | 3.24 → 3.37 | 2.94 → 2.72 | 2.01 → 1.86 |
| small / compact / off | 3.24 → 3.47 | 3.12 → 2.92 | 2.01 → 1.90 |
| large / full / on | 15.91 → 16.05 | 20.35 → 16.08 | 14.46 → 14.54 |
| large / full / off | 11.78 → 13.18 | 11.39 → 13.38 | 9.81 → 11.68 |
| large / compact / on | 14.98 → 15.99 | 14.48 → 16.52 | 12.98 → 14.96 |
| large / compact / off | 12.37 → 13.33 | 12.24 → 13.41 | 10.59 → 11.73 |

| Schema / profile / reflection | Compile first elapsed, s | Compile repeat elapsed, s | Compile repeat CPU, s |
|---|---:|---:|---:|
| small / full / on | 5.707 → 7.289 | 5.867 → 5.764 | 6.019 → 5.927 |
| small / full / off | 5.119 → 4.700 | 5.107 → 4.689 | 5.238 → 4.894 |
| small / compact / on | 5.700 → 5.523 | 5.722 → 5.382 | 5.903 → 5.564 |
| small / compact / off | 4.811 → 4.697 | 5.058 → 4.681 | 5.248 → 4.880 |
| large / full / on | 6.518 → 5.623 | 5.701 → 5.534 | 5.843 → 5.731 |
| large / full / off | 4.670 → 4.727 | 4.896 → 4.705 | 5.061 → 4.900 |
| large / compact / on | 6.322 → 5.599 | 6.171 → 5.476 | 6.284 → 5.690 |
| large / compact / off | 5.084 → 4.823 | 5.168 → 4.594 | 5.318 → 4.761 |

Large-schema repeated generation CPU increases by 0.5%–19.1%, consistent with
emitting more source; repeated consumer compilation does not show a consistent
regression in this run. First small/full/reflection consumer compilation rises
from 5.707 to 7.289 seconds elapsed, while its repeat CPU median falls slightly.
Concurrent load and the small sample prevent treating isolated first-run changes
as stable compile-time regressions.

## Runtime latency and allocations

Reads fetch a UInt64 and a non-null Text length per record. Copy cases assign a
list of 4 or 4,096 structs from an independently serialized message; each struct
has a scalar and Text. They measure the existing default generated setter and
the dynamic setter, which now performs bounded copying. They are not equivalent
resource-policy APIs. Setup allocations are excluded. Read allocation counts
instrument the Message allocator; these cases do not materialize schema defaults.
Registry cases load a 416-byte self-contained descriptor or the 29,376-byte
checked-in reflection corpus, with one untimed warm-up.

CPU values are median **nanoseconds per operation**, baseline → current. Reads
count one record as an operation; copies count one whole list; registry loads
count one complete initialization and teardown. Allocations count allocator
calls, and bytes are cumulative successful allocation/growth, **not peak live
memory**. Runtime samples alternate baseline/current.

| Operation | Size | CPU ns/op | Change | Alloc calls/op | Allocated bytes/op |
|---|---:|---:|---:|---:|---:|
| registry-small | 416 | 858.00 → 866.00 | +0.9% | 2.166 → 2.166 | 2,680.84 → 2,658.07 |
| registry-corpus | 29,376 | 28,220.00 → 26,620.00 | -5.7% | 3.000 → 3.000 | 134,360.00 → 134,464.00 |
| generated-read | 4 | 10.75 → 10.75 | +0.0% | 0.000 → 0.000 | 0.00 → 0.00 |
| dynamic-read | 4 | 723.83 → 718.47 | -0.7% | 0.000 → 0.000 | 0.00 → 0.00 |
| generated-read | 4,096 | 10.49 → 10.30 | -1.8% | 0.000 → 0.000 | 0.00 → 0.00 |
| dynamic-read | 4,096 | 725.53 → 706.75 | -2.6% | 0.000 → 0.000 | 0.00 → 0.00 |
| generated-copy | 4 | 948.00 → 948.00 | +0.0% | 5.000 → 5.000 | 3,760.00 → 3,760.00 |
| dynamic-copy | 4 | 2,554.00 → 2,719.00 | +6.5% | 2.000 → 2.000 | 1,808.00 → 1,808.00 |
| generated-copy | 4,096 | 218,666.67 → 218,400.00 | -0.1% | 7.000 → 7.000 | 397,328.00 → 397,328.00 |
| dynamic-copy | 4,096 | 105,833.33 → 159,100.00 | +50.3% | 3.000 → 3.000 | 198,616.00 → 198,616.00 |

The material runtime regression is the large dynamic copy: 105.8 µs becomes
159.1 µs (+50.3%). Its new source traversal bounds work and expanded output before
publication, including shared-target amplification. The disjoint-source path
avoids a redundant snapshot, so allocation calls and bytes match the baseline.
Self/overlapping sources still need snapshots for correctness and are covered by
conformance tests, not by this disjoint-copy timing case. The small dynamic copy
adds 6.5%. These are explicit costs of the new resource contract, not evidence of
unchanged copy speed.

Generated copies and ordinary reads remain near baseline in these cases.
Dynamic field-name lookup remains much slower than generated accessors in
absolute terms (roughly 0.7 µs versus 0.01 µs per measured record read). The
results do not cover every scalar, pointer default, group operation, schema
shape, alias pattern, or transport workload.
