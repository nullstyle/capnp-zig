# Interop/E2E Migration Status

Updated: 2026-02-06
Owner: `capnp-zig` contributors

## Objective
Keep one canonical interoperability gate where Zig is always the system under
test and failure is surfaced by `just e2e` (and therefore `just ci`).

## Current Gate
- Harness root: `tests/e2e/`
- Orchestrator: `tools/e2e_runner.zig`
- Directions:
  - Zig client -> reference server
  - Reference client -> Zig server
- Scenarios:
  - `game_world`
  - `chat`
  - `inventory`
  - `matchmaking`
- Required backends:
  - `cpp`
  - `go`
  - `python`
  - `rust`

## Commands
- Full interop gate: `just e2e`
- Skip rebuilds for local iteration: `just e2e-skip-build`
- Unified local confidence gate: `just ci`

## Completed Migration
- Consolidated orchestration into a Zig-native runner.
- Replaced bash-heavy backend control with per-backend `Justfile` contracts.
- Added dockerized backend execution to reduce host-machine setup drift.
- Removed legacy interop artifacts and paths from active build wiring.

## Follow-On Hardening
- Expand scenario depth per backend while keeping shared expectations stable.
- Continue tightening timeout, retry, and diagnostics behavior in the runner.
- Add longer-duration soak profiles as a separate non-default suite.
