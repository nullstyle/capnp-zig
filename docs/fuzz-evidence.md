# Fuzz evidence and replay

Run from the repository root with the pinned toolchain:

```sh
mise exec -- zig test tools/fuzz_evidence.zig
mise exec -- zig run tools/fuzz_evidence.zig -- --runs=10000
```

The runner discovers every `test "fuzz: ..."` declaration in
`tests/fuzz/fuzz_targets.zig`, `tests/reflection/fuzz_test.zig`, and
`tests/fuzz/generated_rpc_test.zig`. There are currently 16 targets. Each runs
separately with an exact build filter, a fixed seed, and a five-minute deadline.
Success requires an exited-zero child and exactly one report naming that
target, with a positive `Runs: before -> after` delta of at least the requested
count. Compilation time, a timeout, an empty report, a different target's
report, and a failing oracle never count as fuzz activity.

Each invocation creates `.zig-cache/fuzz-evidence/run-<timestamp>/` containing a
manifest, per-target JSON receipts, raw stdout/stderr logs, and a summary.
When Zig reports a saved crashing input, the runner also copies it into that
target's receipt directory before another target can overwrite Zig's crash file.
Receipts record the exact argument vector, seed, elapsed time, measured run
delta, process termination, and failure reason. The manifest records the
toolchain, platform, Git revision and dirty status, and target source hashes.
If the process times out before returning its captured output, the log records
that error and the receipt fails; it does not manufacture a partial report.

The pinned Zig stores replay inputs in **`.zig-cache/f/`** and coverage metadata
in **`.zig-cache/v/`**. Preserve both with the receipts before rerunning a
failure. The nightly workflow uploads all three directories even on failure.
This describes the configured gate, not an assertion that a remote CI run has
completed.

## Replay a target

Use the receipt's exact `argv` from the same checkout and pinned Zig version,
or select the target through the runner:

```sh
mise exec -- zig run tools/fuzz_evidence.zig -- \
  --runs=10000 --seed=0x6ca9b3d1 --filter='generated streaming lifecycle'
```

Keep the saved `.zig-cache/f/` corpus when reproducing. A seed by itself does
not describe the existing corpus or guarantee the same scheduling. Restore the
recorded source revision and its patch when the manifest reports a dirty tree.
Avoid running the same target twice concurrently because Zig locks its corpus.

For a fixed regression, copy the receipt's `crash_input_path` (or the
`.zig-cache/f/crash` path printed by Zig), or a relevant input from the target's
hashed directory under `.zig-cache/f/`, into a fixture next to its test. Add that input
to the target's `std.testing.fuzz` options:

```zig
try std.testing.fuzz({}, lifecycle, .{
    .corpus = &.{@embedFile("regressions/stream-lifecycle.bin")},
});
```

Run the target's ordinary build step **without `--fuzz`** to replay the explicit
corpus deterministically. For generated lifecycle tests, that step is
`mise exec -- zig build test-fuzz-generated-rpc -j1 --summary all`.
Other exact step/filter pairs are in the receipt. Ordinary replay is a
regression check and does not produce evidence of coverage-guided activity.

## Minimize and retain a failure

Preserve the original failing input and receipt first. Work on a copy of the
fixture with the deterministic replay above. Remove chunks, then shorten or
zero individual values, keeping only changes that reproduce the **same
assertion, error, or crash**. For structured lifecycle targets, reduce the
operation sequence and payload sizes while retaining the failing order.
Reject candidates that merely introduce malformed input or a different error.
Commit the reduced fixture with a named public-API regression and a note naming
the original target and receipt. No separate minimizer CLI is assumed here.

The runner's own tests ablate report names, counts, process failures, and report
presence. They also launch an actual successful non-fuzz child and an injected
failing child to verify that neither can be accepted as positive evidence.

## Local sprint evidence

The initial run at baseline `68ad72f4d4b0739e4fa1a205c92aef840a38e702`
with sprint changes present used Zig `0.17.0-dev.1683+5ceec001b` on
macOS/aarch64 and seed `0x6ca9b3d1`. Its receipts are at
`.zig-cache/fuzz-evidence/run-1788917974606776000/`:

- All 13 original targets passed with 10,002–13,839 new executions each.
- Generated streaming lifecycle passed with 12,465 new executions.
- Both reflection targets failed in build configuration before executing. This
  exposed a filter slice lifetime bug; those failures remain recorded as
  failures. Corrective runs after the fix passed: bounded registry loading had
  16,245 executions (`run-1788918294922349000`) and dynamic mutation had 10,027
  executions (`run-1788918296094020000`).

Those initial receipts labeled `.zig-cache/v` as the corpus path. The actual
inputs were retained in `.zig-cache/f`; the runner and archive configuration
now record and preserve the two paths separately. A generated lifecycle rerun
after the transport-teardown ownership fix passed with 10,269 executions in
`run-1788918488819996000`, with the corrected corpus metadata.
These finite local runs provide bounded regression
evidence; they do not establish long-running production exposure.


## Final committed campaign

All **16/16 targets passed** at clean revision
`86106c226f197d26f280442598155898e7fb1fb1` (`dirty: false`), with
**163,818 measured executions** in total and 10,002–11,609 new executions
per target. The sum of per-target command durations was **271.426 seconds**;
this includes compilation and execution, and excludes runner setup. The final
generated streaming lifecycle target completed 10,078 executions.

This campaign used Zig `0.17.0-dev.1683+5ceec001b` on
macos/aarch64, seed `0x6ca9b3d1`, and a floor of
10,000 executions per target. The complete manifest, exact per-target receipts,
and summary are preserved in [`parity-sprint-fuzz.json`](parity-sprint-fuzz.json).
Raw logs and the original receipts are under
`.zig-cache/fuzz-evidence/run-1788919344839469000/`. These are local results;
remote CI completion is not implied.
