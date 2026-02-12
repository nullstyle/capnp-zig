# Full-Spectrum Codebase Assessment Guide

This guide defines a repeatable way to assess a codebase across correctness, safety, protocol behavior, lifecycle management, code generation, FFI, and test quality.

It is designed for audits like `AUDIT_REPORT.md`: broad coverage, concrete findings, fix prioritization, and verification closure.

## Goals

- Find defects across the full stack, not only in one subsystem.
- Rank issues by impact and exploitability.
- Convert findings into concrete fixes and regression tests.
- Exit with an auditable report and green validation.

## Prerequisites

- Working build and test environment.
- Ability to run full and focused test suites.
- Architecture context for major modules and boundaries.
- Clear severity rubric agreed before review.

## Severity Rubric

- `Critical`: memory safety or remote compromise class issues with realistic trigger conditions.
- `High`: high-impact correctness/safety issues (for example UAF, protocol corruption, cross-boundary state corruption).
- `Medium`: meaningful correctness, lifecycle, or state consistency bugs under narrower conditions.
- `Low`: defense-in-depth gaps, sharp edges, non-fatal robustness issues.
- `Test Gap`: missing coverage that materially increases regression risk.

## Assessment Workflow

### 1) Define Scope and Contract

- Define assessment targets: parsing/serialization, runtime/protocol state machines, transport/lifecycle, codegen, FFI/WASM, error paths, tests.
- Define out-of-scope areas explicitly.
- Define completion criteria:
  - No open `High`/`Critical`.
  - `Medium` fixed or explicitly accepted.
  - `Low` triaged.
  - Test gaps addressed for fixed issues.

### 2) Build a System Map

- Map module boundaries and ownership boundaries.
- Identify trust boundaries:
  - External input boundaries (wire bytes, schema input, host calls).
  - Cross-language or ABI boundaries.
  - Async callback boundaries.
- Record high-risk surfaces first:
  - Parsers and pointer traversal.
  - Reference counting and release flows.
  - Shutdown/close and callback reentrancy.
  - Generated code emitters and path/name escaping.

### 3) Run Parallel Review Tracks

Use multiple focused tracks instead of one linear pass.

1. Serialization and parsing safety.
2. Protocol core and capability/reference lifecycle.
3. Transport/runtime close and stream behavior.
4. Peer orchestration and message routing invariants.
5. Code generator correctness and escaping rules.
6. FFI/WASM pointer/lifetime safety.
7. Error handling and failure atomicity.
8. Test quality and coverage gaps.

### 4) Apply a Consistent Review Lens

For each module/function:

- Input safety:
  - Are all external offsets/lengths bounds-checked before read/write?
  - Are integer casts and arithmetic overflow-safe?
- Lifetime and ownership:
  - On every fallible path, who owns memory/resources?
  - Are `defer`/`errdefer` used so ownership cannot leak on early return?
- Reentrancy and callback safety:
  - Can callbacks destroy the object currently executing?
  - Are callbacks captured to locals before invocation where needed?
- State and protocol invariants:
  - Are IDs unique where required?
  - Are unknown IDs rejected?
  - Are transition rules and shutdown sequencing enforced?
- Failure atomicity:
  - Does a failed update leave state partially mutated?
  - Prefer atomic update patterns like `getOrPut` over remove-plus-put.
- Release behavior:
  - Is correctness dependent on `debug.assert` only?
  - Release builds must enforce critical safety checks too.

### 5) Use Fast Discovery Queries

Use targeted scans to find risky constructs quickly, then inspect in context.

```sh
rg -n "assert|debug.assert|@intCast|@truncate|@ptrCast|@alignCast|orelse unreachable|\\.\\?" src tests
rg -n "deinit|close|shutdown|release|ref_count|pending|queue|callback|on_error|on_close" src tests
rg -n "errdefer|defer|fetchRemove|getOrPut|put\\(" src tests
```

Discovery queries are triage tools, not proof. Every candidate must be reasoned in local context.

### 6) Record Findings in a Strict Format

Each finding should include:

- ID (`H1`, `M4`, `L12`, `T3`).
- Severity.
- File and line reference.
- Trigger condition.
- Impact and why it matters.
- Recommended fix direction.
- Status (`open`, `fixed`, `accepted`).

Use direct language and concrete failure modes.

### 7) Fix in Priority Order

1. `Critical`/`High`.
2. `Medium`.
3. `Low` hardening and consistency.
4. Test gaps for changed behavior.

Patch strategy:

- Keep fixes local and explicit.
- Prefer invariants that are easy to review.
- Avoid compatibility layers that hide incorrect behavior if the surface is not yet public.

### 8) Add Regression Tests Per Bug Class

For each fixed issue, add targeted tests by behavior class:

- Lifecycle and callback order tests.
- Overflow/underflow/truncation tests.
- OOM/failure-path rollback tests.
- Protocol invalid-input and duplicate-ID tests.
- Recursion-depth and traversal tests.
- FFI double-free, reinit, and pointer range tests.
- Codegen identifier/path escaping edge tests.

### 9) Validate in Layers

- Run focused suite for touched subsystem.
- Run full compile-only check.
- Run full test suite.
- Confirm no new failures and no expected regressions.

### 10) Publish the Audit Report

Recommended structure:

1. Scope, date, and method.
2. Summary table by severity.
3. Findings grouped by severity.
4. Test coverage gaps.
5. Implementation notes and prioritization.
6. Closure section with validation commands and results.

## Common Defect Patterns to Watch For

- Callback-triggered use-after-free.
- Missing idempotency on close paths.
- Underflow/overflow on counters and sizes.
- OOM paths that partially mutate state.
- Map/list operations that drop existing data on failure.
- Bounds checks done by convention but not in low-level helper itself.
- Release-mode behavior diverging from debug-mode assumptions.
- API surfaces that are technically valid but easy to misuse.

## Exit Criteria

Assessment is complete when all are true:

- No open `Critical` or `High` findings.
- `Medium` findings resolved or explicitly accepted with rationale.
- `Low` findings triaged.
- New tests cover fixed bug classes and key prior gaps.
- Full verification passes:
  - build/check passes
  - full test suite passes

## Optional Operational Cadence

- Day 1: scope, map, discovery scans, first-pass findings.
- Day 2: high/medium fixes and focused tests.
- Day 3: low hardening, gap closure tests, full validation, final report.

For large codebases, run tracks in parallel and merge findings into one canonical report format.
