# capnpc-zig Security & Correctness Audit Report (Round 4 - Remediation Update)

**Date:** 2026-02-12  
**Scope:** Full codebase — serialization, codegen, RPC (levels 0-3), WASM ABI  
**Status:** Round 4 findings remediated and validated.

---

## Summary

| Severity | Open Count |
|----------|------------|
| Critical | 0 |
| High | 0 |
| Medium | 0 |
| Low | 0 |
| Test Gaps | 0 |

All previously reported Round 4 findings have been fixed and covered by tests.

---

## Remediated Findings

### M1 (Resolved): Outbound capability side effects committed before send success

**Implemented changes:**
- Added staged outbound side-effect tracking in `src/rpc/level0/cap_table.zig` via `OutboundCapEffects`.
- Added transactional encode APIs:
  - `encodeCallPayloadCapsWithEffects`
  - `encodeReturnPayloadCapsWithEffects`
  - `commitOutboundCapEffects`
- Updated call-send paths in `src/rpc/level3/peer/call/peer_call_sender.zig` to:
  - stage side effects during encode,
  - rollback on encode/send error,
  - commit only after successful send.
- Updated return-send path in `src/rpc/level3/peer.zig` (`sendReturnResults`) to use the same transactional commit model.
- Added rollback path for prebuilt return frames:
  - `rollbackOutboundReturnCapRefsForPeer` in `src/rpc/level1/peer_return_send_helpers.zig`
  - used by `sendPrebuiltReturnFrame` in `src/rpc/level3/peer.zig`.

### M2 (Resolved): `Finish.requireEarlyCancellationWorkaround` parsed but not enforced

**Implemented changes:**
- Wired `require_early_cancellation` into finish handling in `src/rpc/level3/peer.zig`.
- Added early-cancel behavior for queued promised-target calls when workaround is disabled:
  - `cancelQueuedPendingQuestion`
  - `cancelQueuedPendingQuestionInMap`
- Behavior now diverges as intended:
  - workaround `false`: queued undelivered promised call is canceled on Finish.
  - workaround `true`: queued call is preserved (deferred-cancel behavior).
- Updated protocol field comment in `src/rpc/level0/protocol.zig` to reflect implemented semantics.

### L1 (Resolved): `waitStreaming()` callback overwrite in release builds

**Implemented changes:**
- Replaced debug-assert-only guard in `src/rpc/level2/stream_state.zig` with runtime behavior.
- Second waiter now receives explicit error `error.StreamDrainAlreadyPending` and does not replace the first waiter.

---

## Test Gap Closure

### T1 (Resolved): send-failure rollback coverage for outbound cap side effects

Added regression tests in `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig`:
- `sendCall rolls back outbound cap effects when send fails`
- `sendReturnResults rolls back outbound cap effects when send fails`
- `sendPrebuiltReturnFrame rolls back outbound refs when send fails`

### T2 (Resolved): behavioral coverage for `requireEarlyCancellationWorkaround`

Added regression tests in `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig`:
- `handleFinish cancels queued promised call when early-cancel workaround is disabled`
- `handleFinish keeps queued promised call when early-cancel workaround is enabled`

### Additional hardening coverage

Added test in `src/rpc/level2/stream_state.zig`:
- `StreamState: second waiter gets explicit error without replacing first waiter`

---

## Validation

Commands run after remediation:

- `just check`
- `just test`

Result:

- Build/test pipeline passed.
- `654/654` tests passing.

