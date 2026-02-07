# Progress Report: QUALITY_REPORT.md Response (Since Last Commit)

Date: 2026-02-07
Repository: capnpc-zig
Scope: Work completed since last commit, focused on findings and recommendations in `QUALITY_REPORT.md`.

## Summary

We have addressed the highest-priority correctness and safety issues first, then continued decomposing RPC `peer.zig` into focused modules while adding tests for each extracted slice. Full test and check runs are green.

## Completed Work

### P0: Critical correctness and safety

1. Reader overflow fixes (`src/reader.zig`)
- Added checked arithmetic for:
  - `segment_count_minus_one + 1`
  - total segment-word accumulation
  - related header/size computations
- Added/validated message-size guardrails (`max_total_words`) before payload allocation.

2. Inline composite multiplication overflow (`src/message.zig`)
- Updated inline-composite expected-word calculations to widened multiplication with bounds checks.

3. Regression coverage for above
- Added/updated malformed-segment and overflow behavior tests in:
  - `src/reader.zig`
  - `tests/message_test.zig`
  - `tests/rpc_framing_test.zig`

### P1: RPC monolith decomposition (`peer.zig`) with tests

Extracted modules already in use and exported through `src/rpc/mod.zig`:

- `src/rpc/peer_dispatch.zig`
- `src/rpc/peer_control.zig`
- `src/rpc/payload_remap.zig`
- `src/rpc/peer_promises.zig`
- `src/rpc/promised_answer_copy.zig`
- `src/rpc/peer_inbound_release.zig`
- `src/rpc/peer_call_targets.zig`
- `src/rpc/peer_third_party_pending.zig`
- `src/rpc/peer_return_dispatch.zig`
- `src/rpc/peer_third_party_returns.zig`
- `src/rpc/peer_forward_orchestration.zig`
- `src/rpc/peer_forward_return_callbacks.zig`
- `src/rpc/peer_return_frames.zig`
- `src/rpc/peer_embargo_accepts.zig`
- `src/rpc/peer_join_state.zig`
- `src/rpc/peer_provides_state.zig`

#### Latest completed extraction slices (this update)

1. Forwarded-return logic split from `peer_control`
- Added `src/rpc/peer_forwarded_return_logic.zig`
- `peer_control.handleForwardedReturn(...)` now delegates to that module.

2. Capability/import lifecycle extraction
- Added `src/rpc/peer_cap_lifecycle.zig` for:
  - `releaseImport`
  - `noteExportRef`
  - `releaseExport`
  - `storeResolvedImport`
  - `releaseResolvedImport`
  - `releaseResultCaps`
- Wired all corresponding `Peer` methods through this module.

3. Call-target orchestration extraction
- Added `src/rpc/peer_call_orchestration.zig` for:
  - call target routing (`imported` vs `promised`)
  - imported target dispatch plan execution
  - resolved exported call orchestration
- Wired `Peer.handleCall*` paths through this module.

4. Return orchestration extraction
- Added `src/rpc/peer_return_orchestration.zig` for:
  - question fetch/missing handling branch
  - inbound cap init/deinit lifecycle
  - accept-from-third-party orchestration
  - regular return orchestration handoff
- `Peer.handleReturn(...)` now delegates through this module via thin adapters.

### P2/P3 items completed in this stream

1. Docs build step wired
- `build.zig` includes docs emission/install step.
- `Justfile` includes `just docs`.
- `README.md` includes docs command usage.

2. README staleness fixed
- Removed stale framing that code generation is only a future enhancement.

### Testing hardening

Expanded direct helper tests and OOM/error-path coverage across extracted RPC modules and host/encode surfaces, including use of `std.testing.checkAllAllocationFailures` in multiple RPC paths.

## Current Validation Status

Latest successful runs:

- `zig build test-rpc --summary all` (65/65)
- `just test` (188/188)
- `just check`

## Current Decomposition Snapshot

Line counts (runtime + tests in file):

- `src/rpc/peer.zig`: 6018
- `src/rpc/peer_control.zig`: 1128
- `src/rpc/peer_forward_return_callbacks.zig`: 468
- `src/rpc/peer_return_orchestration.zig`: 311
- `src/rpc/peer_forwarded_return_logic.zig`: 304
- `src/rpc/peer_promises.zig`: 253
- `src/rpc/payload_remap.zig`: 236
- `src/rpc/peer_cap_lifecycle.zig`: 232
- `src/rpc/peer_call_orchestration.zig`: 224
- `src/rpc/peer_call_targets.zig`: 205
- `src/rpc/peer_inbound_release.zig`: 185
- `src/rpc/peer_third_party_pending.zig`: 160
- `src/rpc/peer_return_dispatch.zig`: 137
- `src/rpc/peer_third_party_returns.zig`: 135
- `src/rpc/peer_dispatch.zig`: 37

Note: `peer.zig` still contains a large inline test suite, so line count is only a rough indicator of decomposition progress.

## Priority Mapping vs QUALITY_REPORT.md

Addressed:

- P0 #1: reader overflow bugs
- P0 #2: message multiplication overflow
- P1 #4: `peer.zig` split (in progress, substantial extraction completed)
- P1 #5: allocation size limits in reader path
- P2 #8: OOM/error-path coverage expansion
- P2 #9: stale README codegen note
- P3 #11: docs generation build step
- P3 #13: duplication reduction (partial; promised-answer copy helper and orchestration extractions)

Intentionally not addressed:

- P1 #3 GitHub Actions CI (user does not use GitHub CI)

Still open / partially open:

- Further `peer.zig` decomposition of remaining high-complexity control/state paths.
- Additional comments in complex message/RPC hot paths.
- Broader transport/platform coverage expansion.
- Packaging/versioning/performance follow-ups from lower priorities.
