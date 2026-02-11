# rpc_peer_from_peer_zig Test Triage

Date: 2026-02-08

## Context

`tests/rpc/level3/rpc_peer_from_peer_zig_test.zig` was extracted from inline tests formerly in `src/rpc/level3/peer.zig`.
When imported into `tests/rpc/level3/rpc_peer_test.zig`, `zig build test-rpc --summary all` currently reports multiple failures and memory-safety issues.

The module is currently **not imported** by `tests/rpc/level3/rpc_peer_test.zig`, so active CI/local `test-rpc` remains green.

## Current blockers observed when module is enabled

### Behavioral failures
- `forwarded payload remaps capability index to local id`
- `forwarded payload converts none capability to null pointer`
- `forwarded payload encodes promised capability descriptors as receiverAnswer`
- `forwarded return forwards awaitFromThirdParty to caller`
- `handleCall supports sendResultsTo.thirdParty for local export target`
- `handleResolvedCall forwards sendResultsTo.thirdParty when forwarding promised target`
- `handleReturn replays buffered thirdPartyAnswer return when await arrives later`

### Runtime/allocator failures
- Segfault in `handleCall supports sendResultsTo.yourself for local export target`
- OOM harness failures with leaks/double-free in:
  - `peer queuePromiseExportCall path propagates OOM without leaks`
  - `peer embargo accept queue path propagates OOM without leaks`
  - `peer forwardResolvedCall third-party context path propagates OOM without leaks`

## Recommended next decomposition/fix sequence

1. Fix forwarded payload test setup assumptions first (the three `forwarded payload ...` tests).
2. Fix sendResultsTo forwarding semantics (`handleResolvedCall ... thirdParty`, `handleCall ... thirdParty`).
3. Fix third-party await/return replay semantics (`forwarded return ... awaitFromThirdParty`, `handleReturn replays buffered ...`).
4. Address OOM cleanup paths and double-free/leak regressions.
5. Re-enable this module in `tests/rpc/level3/rpc_peer_test.zig` only after all above pass.
