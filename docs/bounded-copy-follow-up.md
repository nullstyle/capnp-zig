# Bounded copy follow-up

The release candidate retains the allocation-free preflight traversal and its
measured cost. The [performance receipt](parity-sprint-performance.md) reports
the 4,096-record dynamic copy increasing from 105.8 to 159.1 microseconds
(+50.3%), with the same three allocation calls and 198,616 allocated bytes per
copy. Those three-sample measurements ran alongside conformance builds; they
are review evidence, not a stable timing threshold. No new timing was taken
while the current native and Wasm gates were loading the machine.

Preflight bounds expanded output and work before destination allocation or
publication. It charges shared targets once per incoming edge because copies
duplicate them, and charges logical elements even for zero-width lists. These
guarantees remain part of the candidate's copying contract. The correctness
review also found that double-far struct cloning discarded the landing pad's
explicit target offset, and that zero tags lost empty-struct presence. The
candidate corrects those cases with deterministic native/WASI regressions and
a structured generated/dynamic copy fuzz target.

One unmeasured optimization proposal is to batch fixed data-word and output-word
charges for already validated inline-composite lists in
`src/serialization/copy_budget.zig`. Checked multiplication could replace
repeated identical charges and per-element reader construction. A list with no
pointer fields could then finish preflight without visiting each record after
charging every logical element. Lists containing pointers must still inspect
every pointer slot and recursively charge each edge. This is a proposed
experiment, with no claimed speedup and no optimization in this candidate.

Any experiment must preserve checked arithmetic, exact limit boundaries, depth
accounting, repeated shared-target charges, rejection of cyclic expansion,
far-pointer target offsets, empty-struct presence, and unchanged destinations on
failure. Batching must also address which error is returned when several limits
are exceeded; charging all fixed words before visiting a child can change that
ordering. Deduplicating visited targets or dropping preflight would change the
resource contract.

After correctness gates complete and the machine is quiet, compare disposable
baseline and candidate exports using at least seven alternating process samples.
Cover scalar-only, scalar-plus-Text, pointer-only, and zero-width lists at 4 and
4,096 elements, plus shared-target and depth/limit boundary cases. Record
preflight-only attribution and whole `DynamicStruct.Builder.set` latency, CPU
time, allocations, and independent content checksums. Report sample distributions
and reject any correctness or resource-limit regression before considering the
optimization. The review is complete as this prioritization decision; the
optimization remains future work.
