# RPC Layering Sweep

## Scope

Checked `src/rpc/level0..level3/**/*.zig` for upward imports (`levelN` importing `levelM` where `M > N`).

## Fixed

- Moved promised-answer shared logic from `level1` to `common`:
  - `src/rpc/common/promise_pipeline.zig`
  - `src/rpc/common/promised_answer_copy.zig`
- `src/rpc/level0/cap_table.zig` now imports `../common/promise_pipeline.zig` instead of `../level1/promise_pipeline.zig`.
- Removed `level1` compatibility re-export shims for promised-answer utilities.

## Sweep Results (Remaining Upward Imports)

No remaining upward imports inside `src/rpc/level0..level3`.

`host_peer` and `worker_pool` were moved under `src/rpc/integration/` because they are integration adapters that intentionally compose level-2 transport/runtime with level-3 peer orchestration.
