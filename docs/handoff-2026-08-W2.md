# Handoff — week of 2026-08-W2

State at close: `main` = `cadc24e`, clean, CI green. **v0.10.0 and v0.11.0 both
tagged, validated and released.** One commit sits unreleased (the v0.11.0 hash
record, which is release bookkeeping and needs no tag of its own).

This document is for whoever picks the project up next. It covers what changed,
what is deliberately *not* done, and the three traps that cost the most time —
because two of those recur and the third is a class rather than an instance.

---

## 1. What shipped

**Two releases.** v0.10.0 closed out the "shrink the risk surface" sprint;
v0.11.0 shipped the codegen freeze decision, the generator decomposition, the
first QUIC benchmark, and 19 tests that had never executed.

**Four defects, three of them remotely triggerable by an unauthenticated peer:**

| defect | reach |
|---|---|
| Windows QUIC: oversized datagram killed the connection | one spoofed UDP datagram |
| Fanout server: same, all platforms (landed by a concurrent session) | one spoofed UDP datagram |
| ICMP faults (`PortUnreachable`, `ConnectionResetByPeer`) were endpoint-fatal | an off-path packet |
| TCP wake-door deadlock | not remote; hung four CI legs |

**Decomposition.** `peer/mod.zig` 14,059 → 5,807 across P0–P12. `generator.zig`
4,517 → 3,251 across C1–C2. Both verified with the frozen API snapshot
byte-identical and, for codegen, regenerated artifacts byte-identical.

**An API decision, recorded because it unblocked everything else:** codegen
*internals* are no longer frozen. The frozen `codegen` surface went 54 → 20
declarations, scoped to the plugin contract. Zig's privacy is file-scoped, so
the previous blanket freeze made `generator.zig` and `struct_gen.zig`
unsplittable — extracting anything forced its private helpers `pub`, and each
would have become a permanent contract entry.

---

## 2. The three traps

### 2.1 `std.posix.system` changes return type by platform

```
linux   std.os.linux.read/write/poll  ->  usize   raw syscall; failure is
                                                  -errno as a huge POSITIVE
macOS   std.c.read/write/poll         ->  isize   libc shim; failure is -1
```

So `if (rc > 0)` before consulting `std.posix.errno` reads every error as a
success count. In `wake.zig`'s drain this was an infinite loop, because EAGAIN
is the *ordinary* way a non-blocking drain finishes. Four Linux CI legs had
never completed since the sprint merged; the symptom was a job that printed its
last line and died at the step cap naming no test.

**Always classify via `std.posix.errno(rc)` before treating `rc` as a count.**
`tcp/connection.zig:pollRetryIntr` has the correct idiom and even the correct
comment. The rest of the tree was swept and is clean — do not re-run that
search.

### 2.2 Gates scoped narrower than their names

Five found this week, all quietly passing on things they never checked:

- `test-lib` was wired to nothing — 322 source-module tests ran nowhere
- `std.debug.panic` was unscanned by the hardening gate (only `@panic` matched)
- the TSan lane had never once finished inside its timeout
- `check-test-compile` ran on one target of four, hiding a 32-bit break from July
- `mod_quic.zig` had no `refAllRecursive` walk, so `-Dquic=true` compiled none
  of `src/rpc`'s tests

The `check-test-compile` one is the instructive case. It was pinned to Windows
not by design but because the TSan-instrumented suites were registered into it,
and TSan supports few architectures — so the comment recorded the *symptom* as
the *intent*, and that reading survived for months.

**When something is green, ask what it actually executes.** The ablation recipe
is in `capnp-zig-dead-test-hazards` memory and takes about a minute.

### 2.3 Our QUIC transport is uninstrumented for performance and resources

Three measurement rigs, three times TCP-only:

- `bench-check` — TCP only; could not observe upstream's congestion default flips
- `soak` memory curve — TCP only; cannot observe per-connection footprint
- `bench-quic` — added this week, but single-connection

Correctness coverage is good (four-root evidence gate, 64 tests, zero skips).
It is specifically throughput, latency-under-load and footprint that have no
instrument. **A rig that cannot observe a change reports "no change", and from
outside that is indistinguishable from the change not happening.**

Two upstream sessions were about to record datapoints from our rigs on that
mistaken basis; both expectations were withdrawn once told.

---

## 3. Open, in the order I would take them

### 3.1 quic v0.13.0 bump — ready, low risk, needs a decision only

Tag `v0.13.0` → commit `f9e2ef6`, hash
`quic-0.13.0-DnSYvcT4KwBwFoCH9dujgPfwvOinjILm9rl_AyeLpSpk`. Purely additive over
v0.12.0: no wire-behavior change, no API breaks, defaults unchanged, same
toolchain pin. For us it is a pin change and nothing else.

Upstream measured **−2,166,728 B (−68%) per connection** with a
counting-allocator probe at both tags — larger than their release notes' −1.35
MiB, with the difference accounted for structurally.

**Cite that number as theirs, not ours.** We have no rig that can confirm it
(see 2.3). Write "upstream measured …", not anything implying we verified it.

### 3.2 S-series: `struct_gen.zig` (4,512 lines)

Now the largest file in the tree, and genuinely unblocked — it was gated by the
same freeze rule narrowed in `93b8215`. The C-series proved the recipe end to
end; it is written up in `capnp-zig-codegen-freeze-decision` memory, including
the type-ownership split that is most of the actual work.

### 3.3 A QUIC soak variant — only if the footprint number matters to you

`tools/soak_rpc.zig` is TCP-only. Building a QUIC variant is the one thing that
would close the last gap in 2.3. **If it is ever built, build it BEFORE a bump**
so the before/after is a real comparison rather than a reconstruction. Upstream
explicitly is not asking for this.

### 3.4 Automate the release hash check

For **two releases running**, the version sweep updated the placeholder hash's
version prefix and left the *previous* release's digest in
`docs/build-integration.md`. It looks entirely right — correct package, correct
version, plausible digest — and would fail verification for every consumer who
copied it. Only the post-tag fetch caught it, both times. That is now a pattern,
and it wants a check that the recorded digest matches the published artifact.

---

## 4. Things deliberately NOT done

- **C3+ / further `generator.zig` splitting.** Stopped on measurement: the
  `Generator` struct now ends around line 1956 and the rest of the file is
  inline tests. Implementation is ~1,900 lines; the largest remaining cluster is
  103. A third tranche moves ~3% for a full round of risk.
- **B3–B5 build split.** Deferred with measured evidence — past the module
  boundary a carve relocates coupling into a 70–95 field struct rather than
  removing it.
- **Congestion validation.** Delegated to upstream's QNS and blocking quic-go
  interop gates, which run constrained links. Do not build a network simulator
  twice.
- **`--no-pacing` as proof of anything.** The flag exists and works, but on
  loopback it cannot demonstrate the pacer: ~4% between means against ~9%
  run-to-run spread, because there is no bottleneck for the rate ceiling to bind
  against. Documented as a negative result rather than left to imply capability.

---

## 5. Operational notes

- **Toolchain:** `mise.toml` is the single pin (`0.17.0-dev.1683+5ceec001b`).
  zvm's zig shadows it on PATH — always `PATH="$(mise where zig)/bin:$PATH" zig …`.
- **Dev-tarball shelf life:** `ziglang.org/builds/` garbage-collects. Measured
  this week: the current pin and the previous one resolve, `dev.1252` 404s.
  Retention is finite but deeper than one bump.
- **Verify dependency pins by BUILDING**, never by `zig fetch` alone. Priming
  the global cache with a standalone fetch once produced a bogus
  `N-V-__8AAL…` mismatch a pristine cache did not reproduce.
- **Concurrent sessions.** Another session worked this repo in a worktree this
  week and landed `1643ca8` under me. Its rebase applied with zero textual
  conflicts and then failed to compile — the worse of the two outcomes. Pushes
  from other sessions also cancel your in-flight CI via the concurrency group.
- **CI diagnosis.** `tools/stall_watchdog.sh` is wired into the four jobs that
  run test binaries; on output silence it dumps every live test binary's
  threads, backtraces and embedded test names before the cap kills the step. The
  next hang of that class should self-diagnose.

---

## 6. One habit worth keeping

Three separate defects this week had their **correct implementation already in
the tree, one directory over**, and the wrong copy is what ran:

- `pollRetryIntr` (errno-first) vs the QUIC wake drain (sign-first)
- QUIC's `setNonBlocking` + raw write vs TCP's blocking fd + Io vtable
- upstream's `foreign_loop_embedder.zig` catching ICMP faults vs their library
  treating them as fatal

**When you fix one of a pair, check the sibling immediately — in both
directions.** Two of these were found that way and the third was found the hard
way.

A related note on my own errors, since they are instructive: three self-inflicted
red runs this week (a `u64` atomic on a 32-bit target — the same trap fixed that
morning; a gate widening that was too broad; and a fix that swapped a deadlock
for a panic). Each was caught by a gate before it mattered, which is roughly the
argument for having them. The `u64` one is now a comment in `wake_lock.zig`.
