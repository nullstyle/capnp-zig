# Sprint plan: Tagged 0.17.0, delivered

**Owner decision (2026-10-03): option B, one release.** v0.19.0 ships once, after the quic v0.24.0 move, so consumers migrate once. Items 4 and 14 merge into a single cut at the end (item 14). Consumer handoffs are written for that one cut.

**Theme.** Ship ONE v0.19.0 on tagged Zig 0.17.0 that:
- moves QUIC consumers to a cap-free quic, in lockstep with qmsg;
- closes the consumers' written asks: a pinnable plugin, explicit error sets, and QUIC sessions at TCP parity;
- runs under gates that can actually see a regression.

**Winner:** Plan 1 (downstream value first). It takes these grafts:
- **From Plan 3:** soak gates with teeth before the quic bump; contract truth, including a non-exhaustive DisconnectCause; liveness fixes; the reset key in the hardened preset.
- **From Plan 2:** QUIC session parity; hot paths under a perf gate.

| Plan | Score | Note |
|---|---|---|
| Downstream value first | **7.6** | Every item maps to a named consumer; strongest evidence |
| Risk and quality first | 7.2 | Best gates and ablations; weak on user-facing value and ambition |
| Ambitious features | 6.1 | Its centrepiece (native v2 lanes) is superseded by quic-zig v0.24.0; too large for two weeks |

## State verified today (2026-10-03)

**CI**
- Run 37149923019 on 428197a failed only on `Zig e2e interop`. Every Windows and ReleaseSafe leg passed.
- **613f8bb** (after the surveys) already ports `tools/e2e_l3_cpp.zig` to `Operation.net_write`. Its CI run, 37152177242, was in progress when checked.

**quic-zig v0.24.0**
- **Tagged and pushed today at 11:56.** It removes the 4096 lifetime stream cap: streams now come back as they close.
- It is a BREAKING release. `Connection.max_streams_per_connection` is removed, and `native_outbound_queue.zig:16` reads exactly that constant.
- This supersedes both the native v2 lanes and the proactive rotation.

**Package consumers**
- No package-consumer root references `rpc.transport.tcp`. The roots name only `canonical`, `Peer` and `quic.Connection`, so a v0.18.0-style read break would go unseen.

**Confirmed defects and gaps**
- `CHANGELOG.md` repeats its `### Changed` and `### Fixed` headings.
- Reflection-by-default, the UTF-8 getters and the new enum variant are filed outside Breaking.
- `DisconnectCause` is exhaustive.
- The hardened preset has no `stateless_reset_key`.
- WarmRedialClient's `redials` counter never resets.
- Deadline-cancel errors are only debug-logged.
- WorkerPool has no idle or first-frame deadline.
- The soak has no NODELAY, no bound on `transport_errors`, and a memory gate that counts only the Zig heap.
- `src` has 32 `copyForwards` calls.
- `bench-check-quic` is not wired into any workflow or recipe.
- 9 generated files are not fmt-clean.
- No build step sets `has_side_effects`.

## Ordered items

### Week 1: green, honest, tagged

1. **P0 S: Close the compile-gate gap behind the e2e_l3_cpp break.** The fix itself is in 613f8bb.
   - Confirm a fully green CI run by reading the logs.
   - Add a `check-tools` step for the L3/C++/VatC/go-probe/fuzz_evidence binaries. `check` depends on it on non-Windows hosts.
   - Ablate: make the vtable arm unconditional and confirm `check` goes red.
   - Collapse the four write shims into one.
2. **P0 M: Release-contract truth sweep.**
   - Reconcile CHANGELOG classes and add Breaking/Migration entries.
   - Make `DisconnectCause` non-exhaustive.
   - Fix stale docs: README:3; stability.md:27, 29, 61 and 222-227; the build.zig.zon:9 comment; the README claim that every socket op goes through `std.Io`.
   - Add a "Consumer build pitfalls" section to troubleshooting.md.
   - Replace the Evented CI step with an expected-fail canary.
   - Add fork handoffs for `Stream.read` ReadResult and for `processReplacePath`. Restamp netAcceptWindows and mark the stale handoffs.
3. **P0 S: Package consumers force analysis of every downstream-used API**, including TCP `Transport.read`, `readTimeout` and `write`.
   - Ablation: put back v0.18.0's `stream.read` and package-preflight goes red.
4. **Merged into item 14 (decision B).** No separate early cut. The handoffs below move to item 14.
   ~~Cut v0.19.0, small delta, quic pin unchanged.~~
   - Preconditions: items 1-3 landed, a green Nightly, and the full ceremony with a real `zig fetch` and the hash recorded.
   - Write per-consumer handoffs (qmsg, slcp-zig, bucketlist, mruby-quic, prollytree, capnp-qmsg-demo, capnp-deno). Include slcp's overlay.zig:280 net_read break and a verdict on its 07 F4.
5. **P1 M: Soak gates with teeth.**
   - Set TCP_NODELAY on the soak client.
   - Bound `transport_errors`.
   - Add an RSS-aware memory gate. It must go **red on quic v0.19.0's AEAD leak** before item 6 relies on it.

### Week 2: QUIC consumers and consumer asks

6. **P1 M: Bump quic to v0.24.0** (tag `1d34b32`, hash `quic-0.24.0-DnSYvUNHNABoGpnSvxo5VEF7RVsZTxddSW3UGp1vCrPw`; fallback v0.23.0). Source: the quic-zig session's downstream note (`~/.claude/projects/-Users-nullstyle-prj-zig-quic-zig/handoff/DOWNSTREAM-NOTE-v0.24.0.md`) and EMBEDDING.md "Stream limits are a window".
   - **Delete the lifetime-cap code.** The build breaks on purpose at `native_outbound_queue.zig:16`. Remove `stream_lifetime_cap`, `error.StreamLifetimeExhausted` (`native_outbound_queue.zig:230-231`, `connection_termination.zig:83`), `DisconnectCause.stream_limit_exhausted`, their tests, and the `docs/quic-transport.md` limit bullet. CHANGELOG: Breaking (Experimental) removal with a Migration line.
   - **`StreamLimitExceeded` is always temporary:** keep the frame queued and retry after the next pump. A test proves it at a stream index far past 4096.
   - **Size the stream windows.** `initial_max_streams_bidi/uni` (ours: 16 / 4, `options.zig:162-163`) is now a true "open at once" window. An id returns about 2 RTT after a stream fully closes. Native mode's lifetime control stream holds one uni slot, so only about 3 large frames can be in flight per direction. Measure large-frame throughput at uni 4 / 16 / 64 and raise the defaults (other stacks' test servers use 100 bidi; maximum 4096).
   - Verified not affected: we read none of the renamed `Connection` fields, and we never call `streamStopSending` / `streamReset`. Audit how unexpected peer-opened streams are handled; refusing one now needs stop AND reset.
   - Require more than 10,000 large frames over one connection with zero disconnects and E-order asserted.
   - Pin from a pristine cache. Keep the Windows and Linux ReleaseSafe legs green and the RSS gate green.
   - Rebuild capnp-qmsg-demo with a single quic module, in lockstep with qmsg (qmsg must fix its leaked-id and `no_reply` half-open streams first, per the note).

7. **P1 M: Liveness fixes, red test first.**
   - Route deadline-cancel errors through `report_nonfatal_error` and the observer.
   - Add secure-default first-frame and idle deadlines to WorkerPool. Run the exploit-first test before changing anything.
8. **P1 M: QUIC self-healing in production.**
   - The hardened preset requires `stateless_reset_key` and gains an explicit `.restore_only` 0-RTT opt-in.
   - Crash-restart e2e: the client certifies `.stateless_reset`.
   - WarmRedialClient's budget counts consecutive failures (Decision 3).
   - Refresh quic-durable-caps-plan.md.
9. **P1 M: QUIC sessions at TCP parity.**
   - Add an `on_session_accepted` callback.
   - Add an Experimental QUIC `ClientSession`/serve helper.
   - Add `zig build example-rpc-quic`, run in CI.
   - Add a "Concurrency model" doc section once the owner confirms.
10. **P1 S: Plugin/runtime skew guard.** Generated code fails with one readable `@compileError` against an older runtime.
    - It rides v0.19.0 if it is green by the cut.
11. **P1 M: Pinnable plugin.** Document the `dep.artifact("capnpc-zig")` recipe and prove it in package-preflight from a tarball consumer (slcp 07 F5).
12. **P1 M: Explicit error sets on builder primitives and generated `initX`/`setX` methods** (slcp 07 F6).
    - Add an @typeInfo test.
    - Add a snapshot rule that rejects `anyerror` on Stable builder lines.
13. **P2 M: Hot paths and codegen hygiene.** This item slips first.
    - Hard-gate allocation counts and wire `bench-check-quic` into Nightly.
    - Then make the measured `copyForwards` swap to `@memcpy`/`@memmove`, with cursor framers.
    - Make generated output fmt-clean.
    - Set `has_side_effects` on the freeze gates.
14. **P0 M: Cut v0.19.0 (the single release).** Gate it on at least 2 green Nightlies with the RSS gate enforcing.
    - Publish a capnp/quic/qmsg/Zig compatibility table.
    - Update the handoffs.
    - Scratch-build capnp-qmsg-demo and confirm it links a single quic.

## Owner decisions

1. **quic target for item 6.** Settled: **v0.24.0**. It shipped with full release gates (1811 tests, 2M fuzz runs, a 21-cell interop matrix), and our change is mostly deletion. Fallback: v0.23.0 if the day-1 spike fails.
2. **v0.19.0 scope.** Decided by the owner: **B**, hold v0.19.0 until the quic bump is green, so consumers migrate once.
3. **What `max_redials` counts.**
   - A: consecutive failures, reset after a healthy generation.
   - B: a lifetime budget plus a separate rotation counter.
   - **Recommendation: A.** With the cap gone, a rotation counter would guard a cause that no longer exists.

## Refuted or already done

- **The e2e_l3_cpp port** landed in 613f8bb.
- **"Windows legs still running on 428197a"** is wrong: all of them passed.
- **Native v2 lanes and proactive rotation** are superseded by quic-zig v0.24.0.
- **"quic v0.23.0 keeps `max_streams_per_connection`"** is true, but v0.24.0 removes it, so the bump needs an adapter change.
- **"The package consumer only names Peer"** is partly wrong: `common.zig` exercises serialization and reflection. The TCP gap is real.
- **Items memory lists as open but already landed:** close.zig:42, 4c563e4, c093648, 858bc14, and others.
- **`types.zig` `unreachable`** is not a gap.

## Deferred

| Item | Reason |
|---|---|
| Generated-shape freeze gate | Next sprint, first |
| Module-root restructure | Size L; moves Stable decl paths |
| Unix sockets and FD passing | Headline of the next feature sprint |
| Ticket-key forwarding | Needs the new quic pin; forward-secrecy questions |
| QuicVatNetwork rungs | Policy still open |
| Fanout re-measure | Needs the new pin and the RSS gate first |
| Native and idle-held soak modes | — |
| Compat-arm and 0.18 deprecation sweep | Waits for path-dependency consumers to move |
| macOS and QUIC TSan; aarch64-windows | Opportunistic |
| Fuzz campaign | — |
| Windows `-j1`, nested-zig and naming work | — |
| mise mirror opt-out | Opportunistic |
| SafeAllocator | After RSS baselines exist |
| Evented feature detection and fork lane | Fork rebase first |
| TCP port off `poll(2)` | — |
| Windows NODELAY via AFD | — |
| Bounded-copy batching | — |
| JSON codec | — |
| Stable promotions | — |
| struct_gen split | — |
| WorkerPool L3 vats and L4 | — |
| Downstream canary recipe | — |
| v0.18.1 patch | — |

## Risks

- The first 0.17.0 Nightly may go red.
- quic v0.24.0 is less than a day old and breaking. The v0.23.0 fallback bounds this.
- The RSS gate may be too noisy. If its ablation does not go red, the gate must be redesigned before item 6 relies on it.
- Several Breaking behavior changes need precise Migration notes.
- Generated-shape churn needs golden review.
- Capacity is tight. Items 13, then 12 and 11, slip first. v0.20.0 is gated on evidence, not on a date.
- Downstream repos get scratch builds and handoffs only, never pushes.
- Some premises are survey-sourced and were not re-run here: the dev-build 404s, the Nagle cause of the ~42 ms p99, WorkerPool exploitability, and the slcp reports. Each affected item's first step confirms or kills its premise.
- Concurrent sessions share main; it moved during planning. Coordinate before pushing.
