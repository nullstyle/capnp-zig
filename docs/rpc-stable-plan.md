<!--
Proposed v0.3.0 sprint plan (post-L3, 2026-07-04). Supersedes the pre-L3 version of this file
(git history has it). Reconciles the RPC-Stable freeze with the landed L3 origination surface +
open defects. Generated via draft(3)/judge(3)/synthesize over the scouted state.
-->

# capnp-zig — Next Sprint Plan (post-L3): "Freeze the core, on proven ground"

**Theme:** Ship the v0.3.0 Stable two-party pin that real consumers wait on — but gate the irreversible freeze keystone behind a killed-and-*proven* #55 UAF and a soaked new surface, so we freeze a sound core, not a 24-hour-old one behind a disclaimer.

---

## Resolved position on the fork: **FREEZE-NOW, hardened**

Two of three judges rank freeze-now first, and their core reasoning holds: serialization + the two-party Level-1 RPC core is the mature, interop-validated, soak-exercised surface **real users consume today**, and it does not depend on the fresh post-v0.2.0 pile. Holding a ready Stable pin hostage to soaking a loopback-only feature (harden-then-freeze) or perfecting a Zig↔Zig-only feature with no interop partner (complete-L3) optimizes the surface the ecosystem is *not* waiting on. Both losing forks admit their own central cost is "slip the Stable tag a full sprint."

**But Judge 3's dissent is not dismissed — it is absorbed as hard gates.** Judge 3 correctly flags that pure freeze-now stacks three irreversible-or-brand-new things in one sprint (W1's medium-blast-radius bytes, the F4 keystone, *and* a never-run allowlist-exclusion mechanism) and lets the #55 fix degrade to a band-aid. That is a real achievability-and-soundness risk. So this plan adopts freeze-now's **target** with harden-then-freeze's **irreversibility discipline**:

1. **The #55 UAF gets a real liveness fix, proven by the exact forbidden ordering — not a fail-safe band-aid.** Judge 3's non-negotiable is right: a disclosure is not soak evidence, and error-on-freed-peer is a band-aid over a UAF, not a liveness fix. BUG55 is the first item and a hard gate on the tag.
2. **The new surface that F4 will exclude gets *targeted* soak this sprint** (the L3/reflected/ServerSession scenarios from harden-then-freeze's H3), pointed only at proving BUG55 and exercising the excluded paths — not a full hardening detour. This buys down the "lightly-soaked pile inches from the frozen core" risk that Judge 3 called fatal, without slipping the tag.
3. **F4 lands last and only after its allowlist exclusion is empirically proven** (throwaway-edit test green on an L3 symbol, red on a promoted symbol). The keystone never rides on an unproven boundary.

**#56 is explicitly deferred.** It is L3 feature-completeness inside an Experimental, loopback-only surface — the freeze neither needs nor improves it, and all three judges agree it is the correct cut. It stays a disclosed known-limitation.

> **Post-freeze update (after v0.3.0):** #56 has since been **resolved** — the introducer now forwards parked pipelined promise-calls cross-peer to the host, and the P4 embargo e-order is proven on the real P-pipeline. This planning doc records the freeze-time decision; the paragraph above and the table row below are preserved as the historical cut, not the current state. See the CHANGELOG and `docs/supported-surface.md` for the current L3 status.

---

## Fate of the two L3 defects

| Defect | Fate this sprint | Why |
|---|---|---|
| **#55 cross-peer OutboundProvide UAF** (`mod.zig:360-395`, borrowed raw `*Peer`) | **FIXED — real liveness fix, proven.** Replace the borrowed pointer with a VatNetwork-owned coupling registry / generation-checked handle so B↔C teardown neutralizes every coupled vine on B↔A *before* any Release can dereference. Proven by a leak-checked chaos test driving the exact forbidden order (drop B↔C, then Release the vine). The `mod.zig:360-369` LIFETIME-CONSTRAINT doc is **deleted** (constraint removed, not reworded). | Project no-UAF ethos is tier-independent; a live UAF cannot sit beside a Stable tag. Judge 3's floor: the fix must be liveness, not error-on-freed-peer. Fail-safe (detect-and-error) is retained **only** as an in-sprint fallback of last resort if the registry overruns — and if invoked, the tag slips rather than shipping the band-aid as "done." |
| **#56 introducer does not forward original parked pipelined calls** (hit rejecting vine, `rpc.capnp:898`) | **DEFERRED, disclosed.** Functional-completeness gap in Experimental L3. Stays in the CHANGELOG/known-limitations note (P4's embargo remains proven via the Accept-result pipeline, not the P-pipeline). | Freeze neither needs nor improves it; unanimous judge cut. Picked up in a future L3 feature sprint. |

---

## In-scope items (numbered, sized, sequenced)

Precedence notation: `→` means "must land before."

### Phase 0 — Evidence lane (Day 1, parallel, `tools/` only)
| # | Item | Size |
|---|---|---|
| **1. V1** | `bench-rpc`: p50/p99/max round-trip latency + calls/sec, wired into `bench-check` regression gating with a committed baseline. Prove the gate red on an intentional regression. | M |
| **2. V2** | Extend `tools/soak_rpc.zig`: emit p50/p99/max latency + periodic RSS/allocator memory-growth curve asserted flat within noise; raise worker ceiling to ≥100 concurrent peers + high-in-flight mode via CLI flags. Soaks the **two-party core**. | M |

These run the whole sprint in the background so a bad baseline surfaces before the tag and never squeezes the freeze.

### Phase 1 — Wire fixes + UAF (must precede ALL freeze work)
| # | Item | Size | Precedence |
|---|---|---|---|
| **3. BUG55** | Kill #55 UAF with a liveness-safe coupling (registry / generation-checked handle). Delete the LIFETIME-CONSTRAINT doc. | M–L | First; gate on the tag |
| **4. SOAK-NEW** | Point new soak scenarios at the excluded surface: L3 Provide+Accept handoff, reflected resolve/disembargo, ServerSession accept/teardown under churn — **including the exact #55 drop-B↔C-then-Release ordering**, leak-checked, to PROVE BUG55 rather than merely refactor it. (H3's mechanic, scoped to proving the excluded paths.) | M | BUG55 → SOAK-NEW |
| **5. W1** (`#4`) | Force `forwardMode = yourself` when `target == .imported` in `forwardModeForSendResults` (`peer_forward_orchestration.zig:37-42`, `mod.zig:2172-2229`). Emits `yourself`/`resultsSentElsewhere` instead of `takeFromOtherQuestion`; unblocks go-capnp + capnp-rpc clients, retires 2 known-limitations. **Land FIRST and ALONE** in the wire lane under full soak (chaos+deadline) + local cross-impl matrix. | M | Before W2/W3 and before F4 |
| **6. W2** (`#1`) | Echoed `Unimplemented(Disembargo)` surfaces a connection-level error instead of a stuck embargo (`bootstrap.zig:27-30`, `mod.zig:3574`). | S | With W3, before F4 |
| **7. W3** (`#2`) | `hasKnownDisembargoTarget` accepts only the correct id space, not export-OR-import (`mod.zig:3757`). | S | With W2, before F4 |

BUG55 runs in parallel with the wire lane (it touches Experimental L3, not the frozen surface). W1 lands alone because of its tail-call/ordering blast radius in `forwardResolvedCall`.

### Phase 2 — The freeze (only after Phase 1 wire fixes are green)
| # | Item | Size | Precedence |
|---|---|---|---|
| **8. F2** | Move `Peer.test_hooks` (24 methods, `mod.zig:4429`) + the ~104 `rpc.testing` internals off the frozen public surface behind an Internal facade. Re-point 38 rpc test files. CI leak-check confirms no Internal entry reachable from `src/lib.zig`. | M | First among F1–F3 (biggest snapshot delta) |
| **9. F1** | Narrow leaked public error sets to named sets in `errors.zig`: `releaseImport` (`mod.zig:2825`), the four `sendCall*` entry points (`1993/2036/2130/2175`), `Connection.init` (`connection.zig:178`), `Connection.enableWake` (`connection.zig:218`). **KEEP `anyerror` on the five user-callback typedefs** (CallBuildFn/QuestionCallback/CallHandler/SaveHandler/RestoreHandler — `mod.zig:61-144`) — correctly open there. Pilot on single-site `releaseImport` to size blast radius before the `sendCall` family. | M | Parallel with F3 |
| **10. F3** | Canonicalize shape: one public `Peer` ctor (demote `initDetached*`), one primary transport-attach (demote `attachTransport*`), `Connection.Options.default()` contract, opaque socket-handle wrapper so raw POSIX `i32`/`SocketFd` (`runtime.zig:10`, `connection.zig:181`) is not baked into the frozen signature. Deprecate-not-delete; `ClientSession.connect` / `ServerSession.accept` compile unchanged. | M | Parallel with F1 |
| **11. F4** | **KEYSTONE — point of no return.** Regenerate `docs/api-snapshot.txt` and gate CI on it, scoped to a **promoted-symbol allowlist** (or Stable/Experimental snapshot split) that structurally EXCLUDES every fresh Experimental symbol. Because the current 1814-line snapshot interleaves the ~27–36 L3 symbol hits with the two-party core under `rpc.peer.Peer.*` and `rpc.transport.tcp.*`, a naive whole-file gate would freeze L3 by accident (forbidden). | M | Last; hard-gated on W1/W2/W3 green + BUG55 green + allowlist-exclusion proven |

### Phase 3 — Conformance evidence (needs W1)
| # | Item | Size | Precedence |
|---|---|---|---|
| **12. E1** | Empirically verify go's and rust's actual failure modes in the local Docker matrix after W1 (they fail *differently*: go-capnp-cannot-parse-`takeFromOtherQuestion` vs capnp-rpc-hangs), THEN de-SKIP the `.go` and `.rust` arms (`e2e_runner.zig:104-105`) per-direction-verified. Do not delete a skip arm on faith. | M | After W1 |
| **13. E2** | Add cap-in-params (both directions) + disconnect-mid-call cross-impl scenarios to the local-Docker matrix. **First cut candidate under pressure.** | M | After E1 |

### Phase 4 — Declare (strictly last)
| # | Item | Size | Precedence |
|---|---|---|---|
| **14. D1** | Docs pass (server-behavior defaults contract) + flip `stability.md` two-party-core + serialization rows to **Stable**; keep L3/ServerSession/`resolvePromiseExportToImport`/VatNetwork/QUIC/persistence/forwarded-internals/events **Experimental** with one-line reasons + an explicit "lightly soaked" disclosure + hosted-CI conformance-gap disclosure. CHANGELOG + tag **v0.3.0**, validated by a real `zig fetch`. | S | After F4 frozen + V1/V2/E1 green + BUG55 green |

---

## Freeze scope (F4-enforced)

**STABLE (frozen by the F4 api-snapshot gate):**
- Serialization / wire already-Stable modules; `wire/protocol.zig`; `wire/framing.zig`; `caps/table.zig`
- The **narrowed** `Peer` public two-party entry points (post-F1/F2/F3)
- `Connection` (narrowed)
- `ClientSession.connect` / `ServerSession.accept` **consumer entry points** — must compile unchanged

**EXPERIMENTAL and OUTSIDE the frozen contract (F4's allowlist must exclude every one, even though several currently appear in `api-snapshot.txt`):**
- The entire L3 origination arc: `sendProvide`, `sendAccept`, `resolvePromiseExportToThirdParty`, `sendThirdPartyAnswer`, `registerPendingThirdPartyAwait`, `setHandoffPickupHandler`, `attachVatNetwork`/`detachVatNetwork`, `ProvideHandle`, `thirdPartyHosted` emission
- `VatNetwork` + `LoopbackVatNetwork`
- `resolvePromiseExportToImport` (reflected-cap resolver — new, loopback + cross-impl-partial only)
- `ServerSession` **as a type** (its `.accept` is a compiling consumer entry point; the struct/API is not frozen)
- Pre-existing exclusions: QUIC, persistence vat-restore, forwarded/3-party internals, events

**Critical F4 mechanic:** gate a promoted-symbol allowlist, not the whole file, so the excluded surface can keep evolving post-tag without a false-red gate or an accidental freeze. This re-scoping (S→M) is the one thing L3-landing genuinely forced and the deferred plan under-sized.

---

## Definition of Done (checklist)

- [ ] **BUG55**: `OutboundProvide` no longer holds a borrowed raw `*Peer`; coupling survives arbitrary per-connection teardown order; LIFETIME-CONSTRAINT doc deleted. Chaos test drives drop-B↔C-then-Release-vine and passes leak/UAF-clean under the sanitizer/GPA. L3 stays Experimental.
- [ ] **SOAK-NEW**: soak scenarios exercise L3 handoff, reflected resolve/disembargo, ServerSession teardown under churn — including the #55 ordering — leak-clean.
- [ ] **W1**: `forwardModeForSendResults` forces `yourself` when `target == .imported`; reflected-to-direct handlers observably run inline; soak (normal+chaos+deadline) passes leak-checked with the new mode.
- [ ] **W2**: an echoed `Unimplemented(Disembargo)` produces a connection-level error (asserted), not a stuck embargo.
- [ ] **W3**: `hasKnownDisembargoTarget` accepts only the correct id space; regression test proves the previously over-accepted id is now rejected.
- [ ] **F1**: the six entry points expose named (non-inferred, non-`anyerror`) error sets; the five callback typedefs intentionally KEEP `anyerror`; verified by api-snapshot diff.
- [ ] **F2**: no `test_hooks` method / `rpc.testing` internal in the frozen snapshot; all 38 rpc test files pass against the facade; CI leak-check confirms no Internal entry reachable from `src/lib.zig`.
- [ ] **F3**: exactly one non-deprecated public `Peer` ctor + one primary transport-attach remain (others Internal/deprecated aliases, not deleted); `Connection.Options.default()` documented; frozen signature uses an opaque socket-handle wrapper, not raw `i32`/`SocketFd`; `ClientSession.connect`/`ServerSession.accept` compile unchanged.
- [ ] **F4**: `api-snapshot.txt` regenerated; CI diff gate fails on any unreviewed Stable-module change; **exclusion PROVEN** — a throwaway param on an L3/`resolvePromiseExportToImport`/`ServerSession`-type/VatNetwork symbol keeps the gate GREEN, while a change to a promoted `Peer` entry point goes RED.
- [ ] **V1**: `bench-rpc` exposes p50/p99/max + calls/sec; committed baseline; `bench-check` demonstrated red on an intentional regression.
- [ ] **V2**: `soak_rpc.zig` reports latency percentiles + memory-growth curve; runs at ≥100 concurrent peers leak-clean; flat-memory pass criterion asserted programmatically.
- [ ] **E1**: go-client AND rust-client `resolve_disembargo` directions de-SKIPped and green, or the exact remaining direction documented with reason; cpp + python remain green.
- [ ] **E2** (if not cut): local-Docker matrix passes cap-in-params both directions + disconnect-mid-call.
- [ ] **D1**: `stability.md` marks promoted two-party-core + serialization rows Stable, keeps the fresh surface Experimental with one-line reasons + lightly-soaked + conformance-gap disclosures; CHANGELOG updated; **v0.3.0 tagged and validated by a real `zig fetch`**.
- [ ] **Whole sprint**: `zig build test --summary all`, hardening/honesty gates, soak, `bench-check`, and the api-snapshot CI gate all green at the tagged commit on Linux + macOS + Windows tiers; BUG55 fixed with a liveness fix (not the fail-safe floor) at that commit.

---

## Top 5 risks + mitigations

1. **F4 re-scoping is the riskiest single item — the deferred plan under-sized it.** The snapshot interleaves L3 with the two-party core; a naive whole-file gate freezes L3 by accident or goes falsely red whenever L3 evolves. → **Mitigation:** build the promoted-symbol allowlist (or two-tier snapshot split) and PROVE the exclusion with the throwaway-edit test in F4's DoD before D1. F4 is the *last* keystone item, never stacked with unproven boundaries.

2. **BUG55 could balloon into an invasive registry rework, and the band-aid is tempting under schedule pressure** (Judge 3's fatal-flaw vector). → **Mitigation:** BUG55 is the *first* item with the whole sprint to absorb it. The liveness fix is the DoD; the detect-and-error fail-safe is a last-resort in-sprint fallback only — **if it is invoked, the tag slips rather than shipping a UAF-band-aid as "Stable-adjacent done."** SOAK-NEW's forbidden-ordering test is the acceptance proof, not a refactor claim.

3. **Freezing a Stable tag beside a lightly-soaked Experimental pile** (freeze-now's central downside; Judge 3 called it fatal). → **Mitigation:** two-pronged — (a) SOAK-NEW gives the excluded surface *targeted* soak this sprint (not just disclosure), buying down the "zero soak history" objection; (b) F4's allowlist structurally holds it out of the frozen contract and `stability.md` discloses it as lightly soaked. Residual: a consumer trusts the "v0.3.0 Stable" headline and leans on Experimental L3 anyway — accepted, because the core's trustworthiness does not depend on the pile.

4. **W1's medium blast radius (tail-call/ordering in `forwardResolvedCall`) could regress passing e2e directions, and may not un-SKIP both go and rust** (they fail differently). → **Mitigation:** land W1 first and ALONE under full soak (chaos+deadline) + local cross-impl matrix before any freeze work stacks; E1 empirically verifies each direction before deleting a skip arm; the freeze does not structurally depend on W1 landing perfectly.

5. **Freezing a bug: if F4 landed before W1–W3, W2/W3 become permanent breaking changes.** → **Mitigation:** hard precedence — F4 is gated on W1/W2/W3 green. The sequence makes it mechanical, not a judgment call. Heavy sprint (14 items) is the meta-risk; the cut order below protects the load-bearing set.

---

## Cut order (if it tightens)

Cut from the top down; **never** cut the protected set.

1. **E2** (disconnect-mid-call cross-impl scenarios) — first cut; broadens the matrix but not load-bearing for the freeze.
2. **V2's ≥100-peer stretch** — keep percentiles + memory curve at the current worker count; drop only the ceiling raise.
3. **W3** — ONLY IF F4 has not yet cut (dropping W3 after F4 freezes would strand an id-space bug behind the gate).
4. **SOAK-NEW's breadth** — keep the #55 forbidden-ordering scenario (it proves BUG55); shed the ServerSession/reflected churn scenarios if truly cornered.

**NEVER CUT:** W1, F1, F2, F3, F4, BUG55, D1. These are the freeze and its non-negotiable hygiene.

---

## If, despite this, the tag slips

The only conditions that force a slip past v0.3.0: BUG55 lands only as the fail-safe band-aid (not a liveness fix), or F4's allowlist exclusion cannot be proven green. **What unblocks the next sprint:** a completed liveness fix for #55 with the forbidden-ordering test passing, plus a proven F4 allowlist. Everything else (W1–W3, F1–F3, V1/V2, E1) is designed to already be green by then — so a slip is a one-item finish, not a re-plan.

---

**Relevant files cited:** `src/rpc/peer/mod.zig` (`:360-395` #55, `:2172-2229`/`peer_forward_orchestration.zig:37-42` W1, `:3574` W2, `:3757` W3, `:2825`+`:1993/2036/2130/2175` F1, `:61-144` callback typedefs, `:4429` test_hooks), `src/rpc/transport/connection.zig` (`:178/:181/:218` F1/F3), `src/rpc/.../runtime.zig:10` (SocketFd F3), `tests/e2e/.../e2e_runner.zig:104-105` (E1), `tools/soak_rpc.zig` (V2/SOAK-NEW), `docs/api-snapshot.txt` (F4), `docs/stability.md` (D1), `docs/rpc-stable-plan.md` (the deferred plan this reconciles).