# Handoff Context: QUALITY_REPORT Response Work

Date: 2026-02-07
Repository: capnpc-zig
Audience: Parallel contributors sharing current refactor state before next slices.

## Goal of this handoff

Capture current decomposition boundaries, test expectations, and remaining high-priority work so parallel efforts can proceed without re-deriving context.

## What is stable now

1. P0 safety fixes are in place and validated
- `src/reader.zig` checked arithmetic and size guards
- `src/message.zig` widened inline-composite multiplication checks

2. `peer.zig` runtime paths are now partially orchestrated via extracted modules
- Call target orchestration: `src/rpc/peer_call_orchestration.zig`
- Return orchestration: `src/rpc/peer_return_orchestration.zig`
- Cap/import lifecycle: `src/rpc/peer_cap_lifecycle.zig`
- Forwarded return logic: `src/rpc/peer_forwarded_return_logic.zig` (delegated from `peer_control`)

3. Prior extracted modules are still wired and green
- dispatch, promises, call-target planning, third-party pending/returns, return dispatch, inbound release, forwarding helpers, payload remap, etc.

## Key wiring points (for fast navigation)

- Module exports: `src/rpc/mod.zig`
- Primary orchestrator still in place: `src/rpc/peer.zig`
- Delegation anchors:
  - `Peer.handleCall`: calls `peer_call_orchestration.routeCallTarget`
  - `Peer.handleCallImportedTarget`: calls `peer_call_orchestration.dispatchImportedTargetPlan`
  - `Peer.handleResolvedExportedForControl`: calls `peer_call_orchestration.handleResolvedExportedCall`
  - `Peer.handleReturn`: calls `peer_return_orchestration.handleReturn`
  - `peer_control.handleForwardedReturn`: delegates to `peer_forwarded_return_logic.handleForwardedReturn`

## Test/verification contract

Minimum required verification before sharing additional slices:

1. `zig build test-rpc --summary all`
2. `just test`
3. `just check`

Current status at handoff time:

- `test-rpc`: pass (65/65)
- `just test`: pass (188/188)
- `just check`: pass

## Remaining high-priority decomposition slices

Recommended next slices, in priority order:

1. Third-party adoption/await completion control flow extraction from `peer.zig`
- Scope: pending await/answer reconciliation and adopted-answer return handling adapters that still live in `peer.zig`
- Goal: reduce dense control-flow and map transitions still centralized in `Peer`

2. Join/provide completion slice extraction
- Scope: residual provide/join completion orchestration and cleanup adapters still centralized in `peer.zig`
- Goal: isolate protocol state transitions from object-method boilerplate

3. Forward/tail-question coordination tightening
- Scope: remaining forwarded-tail question bookkeeping paths still split across `peer.zig` and helper modules
- Goal: one focused orchestration module for forward+tail lifecycle

4. Peer callback adapter consolidation
- Scope: many one-line adapter methods in `peer.zig` introduced during extraction
- Goal: reduce adapter surface and make handoff points more uniform without changing behavior

## Collaboration guardrails

- Preserve behavior: refactor-only slices should avoid protocol semantics changes.
- Keep tests with each slice: add focused tests in extracted module and keep end-to-end suite green.
- Avoid broad rewrites: prefer one slice per commit to keep bisectability and review clarity.

## Out-of-scope for this branch slice

- GitHub Actions CI setup (explicitly not used in this workflow)
- Packaging/versioning/perf initiatives from lower-priority report items
