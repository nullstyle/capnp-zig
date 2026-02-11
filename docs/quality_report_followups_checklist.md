# Quality Report Follow-Ups Checklist

Updated: 2026-02-08
Scope: Remaining non-size-reduction follow-ups from `QUALITY_REPORT.md`.

- [x] Add transport failure-path coverage
  - Add tests for EOF close signaling, close-callback error propagation, and write completion when callback context is missing.
- [x] Add schema evolution runtime coverage
  - Add generated runtime tests proving forward/backward compatibility when fields are added with defaults.
- [x] Refresh docs for current hardening status
  - Record these follow-ups in project docs so current coverage is visible without reading commit history.
- [x] Reduce localized RPC duplication
  - Consolidate repeated return-frame send plumbing in `src/rpc/level3/peer/return/peer_return_dispatch.zig` without changing behavior.

Additional pass (requested after first follow-up batch):
- [x] Optimize `MessageBuilder` allocation behavior
  - Pre-reserve root segment writes, collapse text writes into a single contiguous append path, and pre-size `toBytes()` output/header writes.
- [x] Add allocation-aware benchmark metrics
  - Emit `allocs_per_iter` and allocation-byte metrics in benchmark JSON output and enforce allocation-count baselines via `zig build bench-check`.
- [x] Continue RPC duplication consolidation
  - Centralize promised-answer op slice copying and route `CapTable.noteReceiverAnswerOps()` through the shared owned-copy constructor.
