# capnpc-zig Security & Correctness Audit Report (Round 6 - Outside-In Remediation Update)

**Date:** 2026-02-12  
**Scope:** Remediation of Round 6 outside-in findings across public `Peer` APIs, shutdown lifecycle behavior, export/promise state consistency, and callback lifetime hardening.  
**Status:** All Round 6 findings addressed.

---

## Summary

| Severity | Open Count | Fixed This Round |
|----------|------------|------------------|
| Critical | 0 | 0 |
| High | 0 | 1 |
| Medium | 0 | 3 |
| Low | 0 | 1 |
| Test Gaps | 0 | 4 |

---

## Remediations

### H1 (Fixed): `sendBootstrap()` leaked question state on send/build failure

**Files:**
- `src/rpc/level3/peer.zig:719`
- `src/rpc/level3/peer.zig:723`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:238`

**Fix:**
- Added bootstrap send rollback parity with call send paths.
- `sendBootstrap()` now performs `errdefer removeQuestion(question_id)` after question allocation.

**Regression test added:**
- `sendBootstrap rolls back question when send fails`

---

### M1 (Fixed): Detached-mode `shutdown(on_complete)` could drop callback

**Files:**
- `src/rpc/level3/peer.zig:750`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:154`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:176`

**Fix:**
- `completeShutdown()` now invokes shutdown callback regardless of attached transport.
- Transport close remains conditional on transport presence.
- Callback is nulled before invocation to prevent duplicate completion callbacks.

**Regression tests added:**
- `peer shutdown callback fires for detached peer with no transport`
- `peer detached shutdown callback fires after outstanding questions drain`

---

### M2 (Fixed): `resolvePromiseExportToExport()` accepted unknown target export IDs

**Files:**
- `src/rpc/level3/peer.zig:771`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:2592`

**Fix:**
- Added explicit target validation: resolution now fails with `error.UnknownExport` when `export_id` is not present in local `exports`.

**Regression test added:**
- `resolvePromiseExportToExport rejects unknown target export id`

---

### M3 (Fixed): `addExport` / `addPromiseExport` could leave ghost cap-table export IDs on failure

**Files:**
- `src/rpc/level3/peer.zig:670`
- `src/rpc/level3/peer.zig:674`
- `src/rpc/level3/peer.zig:687`
- `src/rpc/level3/peer.zig:691`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:259`
- `tests/rpc/level3/rpc_peer_from_peer_zig_test.zig:282`

**Fix:**
- Added rollback symmetry for export registration:
  - `errdefer self.caps.clearExport(id)` in both export creation paths.
- Guarantees `CapTable.exports` is reverted when later insertion/promise-marking steps fail.

**Regression tests added:**
- `addExport rolls back cap table export identity when insertion fails`
- `addPromiseExport rolls back cap table export identity when insertion fails`

---

### L1 (Fixed): `Connection` error-callback lifetime guard is now runtime-enforced in all builds

**Files:**
- `src/rpc/level2/connection.zig:40`
- `src/rpc/level2/connection.zig:99`
- `src/rpc/level2/connection.zig:209`

**Fix:**
- Promoted `deinit()`-during-`on_error` guard from debug-only to all build modes.
- `invokeErrorCallback()` now always marks in-error-callback scope, and `deinit()` always panics when called from that scope.

**Impact:**
- Misuse now fails fast in production builds instead of relying solely on debug-mode detection.

---

## Test Gap Closure

Closed this round:
- T1: bootstrap rollback on send failure
- T2: detached shutdown callback semantics
- T3: promise-export resolve target validation
- T4: export-creation rollback symmetry under allocator failure

---

## Validation

Commands run during remediation:
- `zig build test-rpc-level2 --summary all`
- `zig build test-rpc-level3 --summary all`
- `just test`

Results:
- `test-rpc-level2` passed (`103/103`).
- `test-rpc-level3` passed (`240/240`).
- Full suite passed (`664/664`).
