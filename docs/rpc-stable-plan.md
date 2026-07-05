<!--
Planned 2026-07-04. Status: PROPOSED, not started. Sequenced AFTER the L3 three-party
origination feature (which was pulled ahead of this freeze so the freeze captures the L3
surface). Generated via a draft(4)/judge(3)/synthesize planning pass over a scouted backlog.
-->

# RPC Stable v0.3.0 — Final Sprint Plan

**Theme:** Sequence by irreversibility — land every wire-behavior change first, freeze the public surface as the keystone (api-snapshot CI gate hard-gated on the wire fixes being green), then tag Stable only once conformance and scale evidence are green. We promote exactly the surface we can stand behind, and no more.

---

## Scope decision (settled before the sprint starts)

**In:** the two irreversible surfaces — emitted wire bytes and the public API — plus the minimum evidence to defend a "production-worthy" claim.

**Explicitly out, with reasons:**

- **Level-3 three-party ORIGINATION** — pre-decided OUT. It is an L (~500–1000 LOC) *feature*, not a maturity gap; receive-only inbound handlers already exist. Including it would risk the freeze without making the frozen surface more trustworthy. Deferred to a future feature sprint.
- **#3 forwarded `takeFromOtherQuestion`/`resultsSentElsewhere` intermediary degrade** (`peer_forwarded_return_logic.zig:90-100`) — proxy-topology-only, M-sized, off the default two-party critical path. It lives in a module that **stays Experimental**, so it is not covered by the freeze guarantee. Defer to v0.3.1.
- **Cross-impl e2e as a hosted-CI lane** — ground truth: hosted runners **cannot** run the Linux-container matrix. This is an infra project, not a freeze task. We keep the local-Docker matrix as the conformance gate and **document the limitation honestly in stability.md** rather than sink sprint time on a known-infeasible spike.
- **100/1000-peer and 100k-in-flight extreme scale** — the soak harness caps at 8 workers; lifting it to that extreme is its own scale-engineering effort. This sprint delivers a **≥100-peer baseline with percentiles + a memory-growth curve**, not a four-nines scale proof.
- **Server-behavior knobs** (deadlines / idle-reaping / drain / backpressure / handler-abort) — already COMPLETE and wired with sensible defaults. At most a **docs/defaults-contract pass** (folded into D1), not engineering.
- **Windows QUIC in CI** — platform-parity gap, not a Stable-blocking correctness gap. Separate track.
- **Benchmark trend/history persistence** — V1/V2 produce committed baselines + regression gating; longitudinal dashboards are a v0.3.1 nicety.

---

## Work items — lanes, sizes, and precedence

Three lanes. **Lane V (evidence) runs in parallel from day 1** because it touches `tools/`, never the frozen surface — a scale overrun cannot squeeze the freeze. **Lane W (wire) must fully land before Lane F (freeze).** The api-snapshot gate (F4) is the point of no return and is hard-gated on W1–W3 being green.

### Lane V — Evidence (parallel, starts Day 1, touches `tools/` only)

| # | Item | Size | Notes |
|---|------|------|-------|
| **V1** | RPC round-trip benchmark (`bench-rpc`): p50/p99/max latency + calls/sec, wired into `bench-check` regression gating with a committed baseline | M | No RPC perf baseline exists today — benches are serialization-only (`bench-ping-pong`/`bench-packed`). **Prove the gate works by introducing an intentional local regression and confirming red** (grafted from scale-first S3). |
| **V2** | Extend `tools/soak_rpc.zig`: emit p50/p99/max latency **and a periodic memory-growth curve** (RSS/allocator sampled over time, not just a final leak check); raise worker ceiling to **≥100 concurrent peers** + a high-in-flight mode via CLI flags | M | Today soak reports counts only, max 8 workers. **Define the pass criterion up front: "flat within allocator noise over run duration," asserted programmatically in the soak summary** (grafted from scale-first) — no eyeballing a slope. |

### Lane W — Wire behavior (MUST land before the freeze; W1 first, alone)

| # | Item | Size | File:line | Notes |
|---|------|------|-----------|-------|
| **W1** | **#4 reflected-loopback**: force forward mode to `yourself` when `target == .imported` in `forwardResolvedCall` (`forwardModeForSendResults`) | M | `peer_forward_orchestration.zig:37-42`; `mod.zig:2172-2229` | **Headline + single most irreversible change** — it alters emitted bytes (`takeFromOtherQuestion` → `yourself`/`resultsSentElsewhere`) and makes reflected-to direct handlers run inline. Medium blast radius (tail-call ordering). **Land first and alone**, run full soak (chaos+deadline) + local cross-impl matrix before anything stacks on it. |
| **W2** | **#1**: echoed `Unimplemented(Disembargo)` surfaces a **connection-level error** instead of a stuck embargo | S | `bootstrap.zig:27-30`; `mod.zig:3574` | Silent hangs are exactly the wire behavior you must not freeze. |
| **W3** | **#2**: `hasKnownDisembargoTarget` stops over-accepting export-OR-import id; accept only the correct id space | S | `mod.zig:3757` | Cheap, wire-adjacent; lands with W2 as the disembargo cluster. |

### Lane F — The freeze (only after W1–W3 are green)

| # | Item | Size | Notes |
|---|------|------|-------|
| **F1** | Narrow leaked public error sets to **named sets in `errors.zig`**: `releaseImport` (**`anyerror!void` at `mod.zig:2825`**), the `sendCall` family (inferred `!u32` at `mod.zig:1993`/`2036`/`2130`/`2175`), `Connection.init` (`connection.zig:178`) and `Connection.enableWake` (`connection.zig:218`) | M | **Pilot on single-site `releaseImport` first** to size blast radius before touching the `sendCall` family (which fans into `peer_call_sender` + callback dispatch). **KEEP `anyerror` (correct)** on user-callback typedefs: `CallBuildFn`/`QuestionCallback`/`CallHandler`/`SaveHandler`/`RestoreHandler`. |
| **F2** | Public/test boundary: move `Peer.test_hooks` (24 methods, **`mod.zig:4429`**) and the ~104 `rpc.testing` internals **off the frozen public surface** behind an explicitly-Internal facade | M | Mechanical re-point of 38 rpc test files behind `rpc.testing`. Largest api-snapshot delta — do it first among F1–F3. CI leak-check confirms no Internal testing entry is reachable from `src/lib.zig`. |
| **F3** | Canonicalize shape: **one** public Peer constructor + limits path (demote `initDetached`/`initDetachedWithLimits`, `mod.zig:538/548/553` — **deprecate, don't delete**); **one** primary transport-attach path (demote `attachTransport`/`attachTransportBinding`, `mod.zig:653/697/712`); give `Connection.Options` a documented `default()`/defaults contract; **wrap `SocketFd`** so raw POSIX `i32` (`runtime.zig:10`, `connection.zig:181`) is not baked into the frozen signature | M | `ClientSession.connect`/`ServerSession.accept` stay the primary consumer entry points and must compile unchanged. Downstream call-site churn (examples, e2e mains) is in-scope cleanup, **not** a surface change. |
| **F4** | **KEYSTONE**: regenerate `docs/api-snapshot.txt`, review line-by-line, and gate CI on it — scoped to the promoted-module symbols, failing loudly only on frozen-contract deltas | S | The snapshot is **1781 lines** (small and diffable — no giant-file noise). This is the point of no return: after F4 lands, nothing in scope may alter the public surface. Turns "is it frozen?" into mechanical red/green. |

### Lane E — Conformance evidence (needs W1)

| # | Item | Size | Notes |
|---|------|------|-------|
| **E1** | Prove W1: **empirically verify go's and rust's actual failure modes in the local Docker matrix, THEN de-SKIP** the `.go` and `.rust` `resolveDisembargoSkip` arms (`e2e_runner.zig:104-105`) | M | **Critical nuance (verified in-repo):** go and rust fail *differently* — `SKIP(go-capnp-cannot-parse-takeFromOtherQuestion)` vs `SKIP(capnp-rpc-hangs-on-takeFromOtherQuestion)`. The fix must make go's direction **stop needing** `takeFromOtherQuestion`, not merely switch mode for rust. **Do not delete the skip arms on faith** — verify each direction goes green first, or document the exact remaining direction with its reason. |
| **E2** | Add priority cross-impl scenarios to the local-Docker matrix: **basic cap-in-params both directions** + **disconnect-mid-call** | M | Named conformance-backlog gaps expressible with the existing scenario harness. Broadens what the local matrix proves before freeze. |

### Lane D — Declare (strictly last)

| # | Item | Size | Notes |
|---|------|------|-------|
| **D1** | Docs pass (server-behavior defaults contract) + flip `stability.md` RPC rows to Stable + CHANGELOG + **tag v0.3.0** | S | Only valid once F4 is frozen AND V1/V2/E1/E2 are green. Validate the tag with a real `zig fetch`. |

---

## Sequence (with hard precedence)

- **Phase 0 (Day 1, parallel):** Kick off **V1 + V2** in a background lane — they touch `tools/`, never the frozen surface, so they run the whole sprint without blocking. Front-loading the long-pole evidence means a bad baseline surfaces before the tag, not at it.
- **Phase 1 — WIRE FIRST (must precede all freeze work):** **W1 → W2 → W3.** W1 is highest-risk; land it **first and alone** with full soak (chaos+deadline) + local cross-impl matrix before W2/W3 pile on. *Rationale: these three change emitted bytes; post-freeze they are breaking changes forever, so zero of them may slip past the freeze keystone.*
- **Phase 2 — THE FREEZE (only after Phase 1 is green):** **F1 + F2 + F3** may proceed in parallel among themselves (pilot F1 on `releaseImport`; F2 goes first for the big snapshot delta), then converge into **F4**. F4 is hard-gated on W1/W2/W3 being green — this structurally prevents freezing a bug.
- **Phase 3 — CONFORMANCE EVIDENCE:** **E1 → E2**, both after W1. E1 de-SKIPs only after empirical per-direction verification.
- **Phase 4 — DECLARE:** **D1** strictly last, gated on F4 frozen + V1/V2/E1/E2 green.

**One-line precedence rule:** *W1–W3 before F4; F4 before D1; V1/V2 parallel throughout; E1 needs W1.*

---

## Freeze mechanics

**Modules promoted to Stable this sprint** (in `docs/stability.md`), once F1–F4 land:
- `wire/protocol.zig`, `wire/framing.zig`
- `caps/table.zig`
- `Peer` public entry points (narrowed), `Connection` (narrowed), and the `ClientSession.connect` / `ServerSession.accept` consumer surface

**Modules that STAY Experimental** (one-line reason each in stability.md):
- **QUIC transport** — platform-parity + Windows CI gap
- **persistence vat-restore interface** — restore path still settling
- **forwarded/three-party internals** — #3 proxy degrade + L3 origination out of scope; not covered by the freeze guarantee
- **events** — names still growing

**The gate:** `docs/api-snapshot.txt` (1781 lines) regenerated at F4, reviewed line-by-line, enforced by a CI diff gate scoped to promoted-module symbols. It fails on any unreviewed public-surface change to a Stable module. The error-set half of the gate: a grep of the promoted surface shows **zero `anyerror` and zero inferred error unions** except the five intentionally-kept user-callback typedefs.

---

## The "earn production" verification bar

The Stable/production-worthy claim is not defensible on a green compile. All three must exist and be green at tag time:

1. **RPC-level performance evidence** — `bench-rpc` reports p50/p99/max round-trip latency + calls/sec against a **committed baseline**, and `bench-check` **demonstrably** fails on a regression (proven with an intentional local regression).
2. **Scale evidence** — soak runs at **≥100 concurrent peers** + high-in-flight mode, completing **leak-clean** with the memory-growth curve **programmatically asserted flat** within allocator noise over the run.
3. **Conformance evidence** — the local-Docker cross-impl matrix is green including **go-client AND rust-client `resolve_disembargo` de-SKIPped** (or the exact remaining direction documented with reason), plus the two new scenarios (cap-in-params both ways, disconnect-mid-call). The hosted-CI limitation is disclosed in stability.md, not papered over.

---

## Definition of Done

- [ ] **W1:** `forwardModeForSendResults` forces `yourself` when `target == .imported`; reflected-to direct handlers observably run inline; soak (normal+chaos+deadline) passes leak-checked with the new forward mode.
- [ ] **W2:** an echoed `Unimplemented(Disembargo)` produces a connection-level error (asserted by a test), not a stuck/hung embargo.
- [ ] **W3:** `hasKnownDisembargoTarget` accepts only the correct id space; a regression test proves the previously over-accepted id is now rejected.
- [ ] **F1:** `releaseImport`, the four `sendCall*` entry points, `Connection.init`, and `Connection.enableWake` expose named (non-inferred, non-`anyerror`) error sets from `errors.zig`; the five user-callback typedefs intentionally KEEP `anyerror`; verified by api-snapshot diff.
- [ ] **F2:** no `test_hooks` method and no `rpc.testing` internal appears in the frozen public api-snapshot; all 38 rpc test files compile/pass against the `rpc.testing` facade; CI leak-check confirms no Internal entry reachable from `src/lib.zig`.
- [ ] **F3:** exactly one public Peer constructor and one primary transport-attach method remain non-deprecated (others are Internal/deprecated aliases, **not deleted**); `Connection.Options` has a documented `default()` contract; the frozen signature uses an opaque socket-handle wrapper, not raw `i32`/`SocketFd`; `ClientSession.connect`/`ServerSession.accept` compile unchanged.
- [ ] **F4:** `docs/api-snapshot.txt` regenerated, reviewed, enforced by a CI diff gate that fails on any unreviewed Stable-module public-surface change.
- [ ] **V1:** `zig build` exposes `bench-rpc` (p50/p99/max + calls/sec); committed baseline exists; `bench-check` demonstrated red on an intentional regression.
- [ ] **V2:** `soak_rpc.zig` reports latency percentiles + a memory-growth curve; runs at ≥100 concurrent peers leak-clean; flat-memory pass criterion asserted programmatically.
- [ ] **E1:** go-client AND rust-client `resolve_disembargo` directions de-SKIPped and green (or the exact remaining direction documented); cpp + python directions remain green (no regression).
- [ ] **E2:** local-Docker matrix passes cap-in-params both directions + disconnect-mid-call.
- [ ] **D1:** `stability.md` marks the promoted RPC modules Stable (with the four Experimental exclusions + one-line reasons + the hosted-CI conformance-gap disclosure); server-behavior defaults contract documented; CHANGELOG updated; **v0.3.0 tagged and validated by a real `zig fetch`**.
- [ ] Whole sprint: `zig build test --summary all`, hardening/honesty gates, soak, bench-check, and the api-snapshot CI gate all green at the tagged commit on Linux+macOS+Windows tiers.

---

## Top 5 risks + mitigations

1. **W1 medium blast radius** (tail-call/ordering assumptions in `forwardResolvedCall`) could regress currently-passing e2e directions. → Land it **first and alone**; gate on full soak (chaos+deadline) + the full local cross-impl matrix (cpp/go/python/rust) before any freeze work stacks on it. Better to discover instability while the surface is still unfrozen. The freeze does not structurally depend on W1 landing perfectly — if it destabilizes, scope it to the un-SKIP-critical path.
2. **Freezing a bug** — if F4 lands before all wire fixes, W2/W3 become permanent breaking changes. → **Hard precedence: F4 is gated on W1/W2/W3 green.** The sequence enforces it; the api-snapshot gate makes it mechanical, not a judgment call.
3. **The one #4 fix may not un-SKIP both go and rust** — they fail *differently* (`e2e_runner.zig:104-105`: go can't parse the return type at all; rust hangs). → **E1 empirically verifies each direction in Phase 3 before deleting either skip arm.** If go needs more than the mode flip, it's a scoped follow-up inside Phase 1/3, not a silent re-SKIP or a red matrix blocking the tag.
4. **Scale-up (V2 ≥100 peers) surfaces latent bugs** (fd/thread exhaustion, contention, memory growth) as unbudgeted work right before the freeze. → Treat found bugs as **evidence wins**; time-box fixes; defer non-blocking ones to v0.3.1. V2 runs in the parallel lane from Day 1 so overrun cannot squeeze F1–F4.
5. **13 items is a heavy sprint** → the explicit **cut order** below protects the non-negotiable core (W1 + F1–F4) and sacrifices only reversible evidence-breadth.

---

## Cut order if the sprint tightens (honest triage)

Cut in this order, top-first. **Never cut W1 or F1–F4** — the headline wire fix and the freeze surface are the sprint's reason to exist.

1. **E2 disconnect-mid-call** (keep cap-in-params + the disembargo de-SKIP).
2. **V2's ≥100-peer concurrency stretch** — keep percentiles + memory-growth curve at the current worker count; the subset still backs the claim honestly.
3. **W3** — the smallest wire fix — **IF AND ONLY IF F4 has not yet cut.** Once the api-snapshot gate lands, W3 becomes a breaking change and **MUST stay in**. This conditional is the sharpest triage rule in the plan: reversibility flips the moment the freeze keystone lands.

Non-negotiable core that ships even under maximum pressure: **W1 + F1 + F2 + F3 + F4 + D1** — a real, if narrower, Stable cut, with any deferred evidence disclosed honestly in the release notes rather than papered over.
