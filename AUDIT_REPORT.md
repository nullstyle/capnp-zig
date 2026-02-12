# capnpc-zig Security & Correctness Audit Report (Round 5 - Remediation Update)

**Date:** 2026-02-12  
**Scope:** Remediation of Round 5 deep-audit findings across RPC capability classification, queued-call duplicate detection, and level2 lifecycle/shutdown behavior.  
**Status:** All Round 5 findings addressed with regression coverage.

---

## Summary

| Severity | Open Count | Fixed This Round |
|----------|------------|------------------|
| Critical | 0 | 0 |
| High | 0 | 1 |
| Medium | 0 | 3 |
| Low | 0 | 1 |
| Test Gaps | 0 | 3 |

---

## Remediations

### H1 (Fixed): Export/import ID collision could misclassify outbound capabilities

**Files:**
- `src/rpc/level0/cap_table.zig`
- `src/rpc/level3/peer.zig`
- `src/rpc/level3/peer/peer_cap_lifecycle.zig`
- `tests/rpc/level0/rpc_cap_table_encode_test.zig`

**Fix:**
- Added explicit export identity tracking in `CapTable` (`exports` map).
- Updated outbound classification precedence to prefer local export identity (`senderHosted` / `senderPromise`) before import-hosted classification.
- Wired peer export lifecycle to register and clear export IDs in `CapTable` (`noteExport` on add, `clearExport` on final release).

**Regression tests added:**
- `encode outbound cap table prefers local export classification over import id collisions`
- `encode outbound cap table prefers local promised export over import id collisions`

---

### M1 (Fixed): Duplicate inbound question detection checked wrong key space for promised queues

**Files:**
- `src/rpc/level3/peer.zig`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig`

**Fix:**
- Reworked duplicate-question detection to scan queued pending-call frames for actual queued call `question_id` values.
- Removed dependency on `pending_promises.contains(question_id)` for duplicate inbound question detection.

**Regression test added:**
- `queued promised target key does not trigger duplicate question id for a distinct queued call id`

---

### M2 (Fixed): Listener accept loop re-armed after close request

**Files:**
- `src/rpc/level2/runtime.zig`

**Fix:**
- `Listener.onAccept()` now gates all `queueAccept()` re-arm calls behind `!close_requested`.
- Added close-request handling for accepted sockets so shutdown does not leak accepted fds while draining in-flight accepts.

**Regression test added:**
- `listener onAccept does not re-arm when close was requested`

---

### M3 (Fixed): `onTransportClose` callback order lifetime hazard

**Files:**
- `src/rpc/level2/connection.zig`

**Fix:**
- Preserved `on_error` then `on_close` ordering for compatibility/cleanup behavior.
- Added explicit safety contract and debug enforcement: `deinit()` is forbidden during `on_error` callback execution.
- Centralized error-callback invocation through guarded helpers to enforce the contract consistently.

**Validation tests updated/added:**
- `connection onTransportClose reports error then close` (retained expected behavior)
- `connection onTransportClose invokes close callback on clean close`

---

### L1 (Fixed): Early-cancel queue cleanup left empty pending buckets

**Files:**
- `src/rpc/level3/peer.zig`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig`

**Fix:**
- `cancelQueuedPendingQuestionInMap()` now removes empty pending buckets and deinitializes list storage when queues drain to zero.

**Regression behavior assertion updated:**
- `handleFinish cancels queued promised call when early-cancel workaround is disabled` now asserts bucket removal (`!pending_promises.contains(...)`).

---

## Test Gap Closure

### T1 (Closed)
Covered by collision regression tests in `tests/rpc/level0/rpc_cap_table_encode_test.zig`.

### T2 (Closed)
Covered by duplicate-question promised-queue regression in `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig`.

### T3 (Closed)
Covered by listener close/re-arm regression in `src/rpc/level2/runtime.zig` test block.

---

## Validation

Commands run during remediation:
- `zig build test-rpc-level0 --summary all`
- `zig build test-rpc-level2 --summary all`
- `zig build test-rpc-level3 --summary all`
- `just test`

Results:
- All targeted suites passed.
- Full suite passed: `658/658`.
