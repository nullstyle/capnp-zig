# Sprint plan 2026-10-15: Green for real reasons

Fix both Nightly failures at their cause, close two capnp-swift correctness asks, and make the tag gate read Nightly results on the release commit.

- **Goal:** Ship v0.27.0 with both current Nightly failures fixed at their cause, the two QUIC and native asks from capnp-swift closed, and a tag gate that will not tag a commit without green Nightly runs or acknowledged flakes.
- **Length:** 7 working days. Days 1-5 are item work: about 10-11 agent-days, done by 2 or 3 agents in separate worktrees. Days 6-7 are the v0.27.0 ceremony. If work slips, cut in this order: S-1, P2-3, P2-1, P2-2.
- **Release target:** v0.27.0, a minor release.
  - `docs/api-snapshot.txt` (the Stable API) stays byte-identical.
  - The Experimental changes are: WarmRedialClient stop accounting, the QUIC stream-0 session end, and native tail-call Returns.
  - The quic-zig pin stays at v0.38.0 unless the owner picks D1 option A. The cluster-B pairing with http3-zig v0.5.7 holds.
  - Tag gate (new, from P1-4): the release commit must have a green CI run. It must also have 3 Nightly runs, started by workflow_dispatch, that are each green or that fail only on unexpired known-flakes rows.
- **Main:** `bbf91e7` = origin/main. The working tree is clean. The v0.26.0 tag is at `75a4eac`.
- **Nightly:** 2 of the last 5 runs failed, all on `bbf91e7`:
  - 38713314813 (10-15): failure
  - 38542668305 (10-14): success
  - 38377271133 (10-13): failure
  - 38231937129 (10-12): success
  - 38128471120 (10-11): success

## Status (2026-10-17)

**Process change.** On 2026-10-16 the owner said: no more PRs. Work lands on main directly. A branch can run the full CI by hand: `gh workflow run CI --ref <branch>`.

**Landed on main**

| item | commits | proof |
|---|---|---|
| P0-1 WarmRedialClient stop race | `66d43fd` | red CI 38918341458 (3 OSes); green CI 38960486931 |
| P0-2 soak window and Windows 64-worker job | `8916bf8`..`5c705a3` | branch Nightly 38918359027: 55,535 ports, 0 setup failures |
| P1-2 stream 0 RESET and FIN end the session | `1fdc029` | 14 new tests red before, green after |
| P1-2 e2e over QUIC lane | `ea5066c`, `e7df409` | CI 38970205763: job "Zig e2e over QUIC" green in 5 min |
| P1-1 Windows `Listener.close` | `8c36f35`, `fd4ae7d` | red CI 38918354154 (abort); green branch CI 38970028135 |
| P1-3a tail-call fixes (see below) | `e8e19f5`..`d0ca8d6` | green branch CI 39008635610, with the Docker cross-implementation lanes |
| Not planned: pycapnp scripts below Python 3.15 | `6dd8780` | macOS CI broke on 2026-10-16 (CPython 3.15 has no pycapnp wheel) |
| Not planned: manual CI trigger | `e9f37e1` | `workflow_dispatch` in `ci.yml` |

**P1-3 was split.**

- The "hold" design (hold a `takeFromOtherQuestion` Return until the named call is answered) is deferred. Five review rounds found new ways that a held question never ends. The last check also found a hang at shutdown and CPU work that a remote peer can force. All 29 commits are on branch `archive/p1-3-tail-call-hold` (`663dbd3`). The review reports list every case.
- P1-3a landed two fixes for defects that main already had:
  - A kept tail-call answer is dropped at the call's Finish and when the remote reuses the id. Before, a later question could get another call's answer.
  - A `yourself` call that fails answers `resultsSentElsewhere` on the wire. Before, it sent an exception Return, and the C++ reference drops the connection for that.
- The capnp-swift ask `tail-call-returns-for-native.md` stays open. A tail call that names a pending call still gets the bare `takeFromOtherQuestion`.

**Still to do:** P1-4, P1-5, P2-1, P2-2, P2-3, S-1, then the v0.27.0 ceremony. Each lands on main directly.

**Notes for the owner to relay to capnp-swift**

- **QUIC stream 0 (H11 check 9).** capnp-zig main (`1fdc029`, for v0.27.0) ends the RPC session when stream 0 ends, in baseline and native mode, for `connect`, `serve` and `EmbeddedSession`. After a clean FIN, capnp-zig dispatches every whole frame that came before it, then closes with code 0 (normal) and cause `peer_close`. Waiting questions end `Disconnected` in under 1 s. A partial frame at the FIN closes with `frame_error` (0x434e5001). A RESET_STREAM closes at once with code 0 and cause `transport_error`. The ALPN stays `capnp-rpc/1`. Send every frame you want delivered before your FIN. Do not send a FIN while you still expect replies. After you pin v0.27.0, run H11 check 9 again. CI now runs the Zig e2e over QUIC (`zig build -Dquic=true e2e-quic`). No Swift cross-implementation test exists yet.
- **Tail calls for native.** The hold is deferred, so the native shim still reports "capnp-swift core: unsupported Return variant" for a tail call that names a pending call. Keep your workaround. Two fixes landed: a failing `yourself` call now answers `resultsSentElsewhere`, and a kept answer can no longer reach a later question with a reused id.

**Words used in this plan**

- **Red before:** a new test fails on the old code, which proves that the test can catch the defect.
- **Ablation:** remove the fix and check that the test fails again.
- **Flake:** a test that fails sometimes for no code reason.
- **Port exhaustion:** the host has no free local TCP ports left. Windows keeps a closed port in TIME_WAIT for 120 s.

## Decisions for the owner

**Decided by the owner on 2026-10-16:**

- **D1: option B.** v0.27.0 stays on quic-zig v0.38.0.
- **D2: option A.** A clean FIN on stream 0 ends the session. P1-2 ships the FIN half.

The text below is the question as the owner saw it.

### D1. Does v0.27.0 move cluster B (capnp-zig and http3-zig) off quic-zig v0.38.0?

- **Option A:** Yes, in this sprint. Ask quic-zig to tag a release that contains `021591e` (2026-10-13, "harden loss recovery, OOM transactionality, multipath routing, and DoS surfaces"), with CHANGELOG and DOWNSTREAM-NOTE entries. Pin capnp-zig v0.27.0 and http3-zig v0.5.8 to that tag. Update every place in the docs that names the pin.
- **Option B:** Not in v0.27.0. Keep v0.38.0. Send quic-zig a handoff that asks for a tag with `021591e` and its notes. Move cluster B in the next release, after that tag exists and the QUIC Nightly has been green for a week after P0-1.
- **Recommendation:** Option B.
- **Why:** `021591e` is in no tag (`git tag --contains 021591e` prints nothing), the quic-zig CHANGELOG says "Do not pin v0.42.1", and a change to loss recovery in the same week would weaken the proof that P0-1 fixed the QUIC Nightly. Pick A only if you treat `021591e` as a security fix for internet-facing QUIC servers, and only after quic-zig tags it. The draft also said that v0.38.0 to v0.42.2 removes no capnp-zig workaround (unverified).

### D2. Does a clean FIN on QUIC stream 0 end the RPC session?

- **Option A:** Yes. The receiver dispatches every whole buffered frame. Then it ends the session with cause `peer_close` and close code `normal`. A partial frame at FIN is a `frame_error` close. This is the QUIC form of TCP EOF. The rule goes under "Wire constants (frozen)" in `docs/quic-transport.md`.
- **Option B:** No. Only a RESET or STOP_SENDING on stream 0 ends the session. P1-2 ships that part with either option. A FIN keeps no meaning.
- **Recommendation:** Option A.
- **Why:** capnp-swift's Network API can close only by FIN, so only option A fixes its H11 check 9. Swift cannot send a RESET (capnp-swift `docs/handoffs/quic-server-close-residual.md`, attempt 4).
- **ALPN note:** `docs/quic-transport.md:327` says "Any change to these needs a new ALPN string." Option A changes no listed constant (ALPN, stream id, framing, zero and oversize length rules, close codes). It gives a meaning to an event that has none today. The risk is a peer that half-closes stream 0 and still waits for replies. Option A would break that peer. No such peer is known (unverified). If you treat option A as a wire change, it needs a new ALPN (`capnp-rpc/2`), and that work is not in this sprint.

## Items

| id | priority | title | effort | depends on |
|---|---|---|---|---|
| P0-1 | P0 | WarmRedialClient: a stop during generation setup counts the generation and reports `.local_close` (Nightly 10-15) | S (1 day) | none |
| P0-2 | P0 | Soak: give the Windows 64-worker lane its own port budget, and judge memory only over the exercised window (Nightly 10-13) | M (1.5 days) | none |
| P1-1 | P1 | Windows: `Listener.close` with a parked accept must not abort the process | M (2 days, timeboxed) | none |
| P1-2 | P1 | QUIC: end the session when stream 0 ends, and add an e2e-over-QUIC CI lane | M (1.5 days) | D2 (FIN half only) |
| P1-3 | P1 | Native C ABI: follow cap-free tail-call Returns (`takeFromOtherQuestion`) | M (1.5-2 days) | none |
| P1-4 | P1 | Tag gate: require 3 Nightly runs on the release commit, with a known-flakes list | S (0.5-1 day) | P0-2 (nightly.yml) |
| P1-5 | P1 | CI headroom: package preflight on Linux only per push, a timeout on every job | S (0.5-1 day) | P0-2, P1-4 |
| P2-1 | P2 | Windows reads: map `STATUS_LOCAL_DISCONNECT`, and file the std read handoff | S (0.5 day) | none |
| P2-2 | P2 | Fix the stale contract-doc claims and ratchet the QUIC test floors | S (0.5 day) | P1-2, D1 |
| P2-3 | P2 | One ledger for downstream asks and one for upstream handoffs | S (0.5 day) | P1-2, P1-3 |
| S-1 | stretch | Test: a back-pressured QUIC client that gets a stateless reset | S (0.5 day) | P1-2 |

## P0-1. WarmRedialClient: a stop during generation setup counts the generation and reports `.local_close`

**Why**

- Nightly 38713314813 (10-15) failed on `bbf91e7`, in the job "Extended gates" (job 116349833595), step `-Dquic=true test-rpc-quic`.
- The failing test is `WarmRedialClient.requestStop never writes into a generation that ended on its own`. It expected 1 and found 0 at `tests/rpc/transport/quic/rpc_quic_peer_test.zig:3931`, called from `:3937`.
- The same job passed on the same SHA on 10-11, 10-12, 10-13 and 10-14. The failure is a race, and it will come back.
- How it happens (verified on `bbf91e7`):
  - `runGeneration` publishes `current_conn` at `src/rpc/transport/quic/warm_redial.zig:431-436`.
  - It then calls `try peer.sendBootstrap` and `try sendRestorePipelined` at `:457-458`. It counts `self.generations += 1` only at `:460`.
  - A `requestStop` in that gap (`:271-282`) calls `requestClose`. `sendFrame` then returns `BrokenPipe` (`connection_dispatch.zig:17`).
  - The catch arm in `run()` sees `stopRequested` and breaks (`:291-295`). The Outcome reports `generations=0` and `last_cause=.unknown` (`:289`).
  - The test waits only for `current_conn != null` (`:3884-3893`). It then asserts `generations==1` and `.local_close` (`:3931-3932`).
- The library reports a stopped generation as "no generation, unknown cause". That is wrong output from an Experimental API. Commit `812b798` fixed the same BrokenPipe-on-close shape in another place.

**Work**

1. Commit A, test only, on its own draft PR. CI runs only on PRs to main and on pushes to main (`ci.yml:3-7`).
   - Add a knob in `src/rpc/transport/quic/test_knobs.zig`, for example `pause_after_publish_ns`. It sleeps between the publish block and `sendBootstrap`.
   - Add a test in `rpc_quic_peer_test.zig`: "requestStop during generation setup reports 1 generation and .local_close". It sets the knob and calls `requestStop` during the pause.
   - Record the red CI run id.
2. Commit B, the fix. Change only the stop path. In `runGeneration`, when `sendBootstrap` or `sendRestorePipelined` fails and `stop_requested` is set, do these steps:
   - Count the generation.
   - Set `last_cause` to `.local_close`.
   - Return "stopped" through the normal teardown order.

   A setup failure without a stop keeps today's accounting.
3. Do not move the count to the publish point, as the draft proposed. `tools/soak_rpc.zig` reads the live `client.generations` at each rebind (`:1548-1556`). It credits 0-RTT growth only when the count is `rebind.generation + 1` (`:642-646`). A count at publish would also count setup failures without a stop. The QUIC heal soak could then mark 0-RTT growth as unattributed.
4. Keep the invariants from `75557be`: `stray_bytes == 0`, and unpublish before `deinit`.
5. Document what `Outcome.generations` counts. Add a CHANGELOG `[Unreleased]` entry for the Experimental behaviour change.

**Done when**

- Red before: the new test fails on the commit-A draft PR (`bbf91e7` plus the knob and the test only), with `generations=0` and `.unknown`. Commit B's message cites that run id.
- Green after: `mise exec -- zig build -Dquic=true test-rpc-quic` passes the new test and the existing stop-race test on all 3 OSes.
- Ablation: with the knob kept and commit B's change reverted, the new test fails again.
- Repeat: a temporary loop step on the PR's ubuntu job runs the existing stop-race test 200 times, and all 200 pass. Remove the step before merge.
- The Nightly QUIC heal soak with `--ticket-key` (`nightly.yml:227`) stays green. It still prints `unattributed 0-RTT generations=0`, as it does today.
- The next Nightly "Extended gates" job is green.

## P0-2. Soak: give the Windows 64-worker lane its own port budget, and judge memory only over the exercised window

**Why**

- Nightly 38377271133 (10-13) failed in the Windows job "RPC soak (chaos + deadlines)" (job 115228694695) with "steady-state live heap grew 108.31% (> 25.0% threshold)" (error `SoakFailed`).
- The cause was not a leak. The `gpa.deinit` leak check (`tools/soak_rpc.zig:2971`) passed. The 64-worker 20 s ReleaseSafe lane hit port exhaustion from +0.6 s: 36927 dials failed after 483 succeeded. So the heap gate judged a run that was mostly idle.
- Evidence (verified on `bbf91e7`):
  - `assessMemory` (`soak_rpc.zig:2119-2158`) drops the first 20% of samples as warmup. It compares the mean of the first quarter of the rest with the mean of the last quarter.
  - Port exhaustion only prints a warning (`:2912-2921`). The heap verdict still fails the run (`:2930-2934`).
  - The sampler runs until a second past the workers' stop (`:1975-1978`). `mem_stop` is set only after "workers joined" (`:2577-2580`). So the teardown burst is inside the judged tail. In the 10-13 log, the 64-worker sparkline ends on its peak cell (about 6.8 MB). The 10-11 run passed by only 31,450 B (230,694 B growth against a 262,144 B floor).
  - The 64-worker lane (`nightly.yml:71-73`) runs after the 120 s and 60 s 8-worker lanes in the same job (`:33-37`). Those lanes leave about 16k ports in TIME_WAIT (the `nightly.yml` comment measured 16,224).
  - Port-exhaustion annotations appear on this lane in 8 of 17 Windows Nightlies since 10-04 (fact-check, from check-run annotations).
  - `ambiguousIsPortExhaustion` (`soak_rpc.zig:347-351`) reported one range-edge failure in the 60 s 8-worker lane as "unexplained".
  - The comment at `nightly.yml:47-59` says that "the close valve fires under parked accepts". That is stale. WorkerPool no longer has a valve. It retries nudges with no bound (`worker_pool.zig:379-380`, `:409-412`).

**Work** (in this order)

1. Port budget first, in `.github/workflows/nightly.yml`:
   - Run the Windows 64-worker lane in its own job on a fresh runner.
   - Widen the dynamic port range with `netsh int ipv4 set dynamicport tcp start=10000 num=55535` (admin rights on hosted runners: unverified).
   - At lane start, print `netsh int ipv4 show dynamicport tcp` and the TIME_WAIT count.
   - Rewrite the stale comment at `nightly.yml:47-59`.
2. Change the judged window in `tools/soak_rpc.zig`:
   - Record the sample index when the workers are told to stop.
   - Judge heap and RSS over `[warmup, min(onset, workers' stop))`, where onset is `min(first_port_exhaustion_ms, ambiguous onset)`.
3. Add a `not_exercised` verdict for two cases: the onset falls before 50% of the run, or too few steady samples remain.
   - Ship it as a `::warning::` annotation, not as a failure.
   - Why not a failure: on `bbf91e7`'s data, a failing `not_exercised` would turn about 6 of 17 Windows nights red (onsets at 0.6, 2.3, 2.9, 7.1, 7.1 and 7.8 s of 20 s). Today 1 of 17 is red.
   - Make it a failure in a later sprint, after the port budget has a clean record.
4. Keep the `gpa.deinit` leak check as a hard gate.
5. In `ambiguousIsPortExhaustion`, treat a single failure that comes after at least a range's worth of successful dials as the exhaustion edge.
6. Add unit tests next to the existing `assessMemory` tests.
   - The soak prints only a 64-cell sparkline and the head and tail means (`:2688`), so an exact replay of the 10-13 run is not possible. Use synthetic series:
     - (a) The 10-13 shape: early onset, flat, and a 6.8 MB teardown spike at the end.
     - (b) The 10-11 shape: no exhaustion, with a teardown spike.
     - (c) A real leak before the onset.
     - (d) An early onset.
   - Optional: add a flag that dumps the samples, so that later failures can be replayed exactly.

**Done when**

- Red before: series (a) fails the heap verdict under `bbf91e7`'s `assessMemory`.
- Green after:
  - (a) gives `not_exercised` as a warning, not a heap FAIL.
  - (b) passes because the teardown is excluded.
  - (c) still fails.
  - (d) gives `not_exercised`.
- A unit test proves the range-edge rule: one failure after 16k successful dials counts as exhaustion, not as "unexplained".
- In the first Nightly after the change, the Windows 64-worker job prints a dynamic range of at least 55,000 ports and has 0 port-exhaustion annotations.
- Its sessions per second are within 2x of the Linux 64-worker lane's rate. Windows runs 20 s and Linux runs 60 s (`nightly.yml:73`), so compare rates, not counts.
- The 3 ceremony Nightlies (P1-4) also show 0 exhaustion annotations on that job. Three clean nights alone are weak proof, because about 9 of 17 nights were already clean. The printed port range is the main proof.

## P1-1. Windows: `Listener.close` with a parked accept must not abort the process

**Why**

- `Listener.close` is Stable and returns `void` (`docs/api-snapshot.txt:1080`). Its doc says that it unblocks any thread blocked in `accept()` (`src/rpc/transport/tcp/runtime.zig:190-191`).
- `ServerSession.accept` is Stable, and `ListenerClosed` is in its error set (`api-snapshot.txt:1094`).
- On Windows, `close` skips shutdown and closes the handle (`runtime.zig:202-225`).
- WorkerPool's own doc says that this close, under a parked AFD accept, completes the wait with `STATUS_CANCELLED`. std 0.17.0's `netAcceptWindows` treats that status as `unreachable`, which aborts the process (`worker_pool.zig:368-382`, first seen in Nightly 33029339794). The std arms are in `lib/std/Io/Threaded.zig` at `:12836`, `:12860` and `:12881`.
- No test closes a TCP listener under a parked accept. The AF_UNIX tests (`rpc_unix_session_test.zig:637`, `rpc_unix_linger_test.zig:657`) are skipped on Windows.
- So the abort is inferred, not reproduced at the Listener level (unverified).
- This item is P1, not P0: no Windows consumer is known, and no gate is red.

**Work**

1. Reproduce first, on a draft PR.
   - Add `tests/rpc/transport/tcp/rpc_listener_close_test.zig` and wire it into `test-rpc-transport`.
   - The test parks threads in `Listener.accept`, `Listener.acceptFd` and `ServerSession.accept`. It calls `Listener.close` from another thread. It expects `error.ListenerClosed` within 2 s.
   - Record the Windows run id.
2. If the repro passes on windows-latest in Debug and in ReleaseSafe on 3 runs, stop. Land the test as a guard, correct the doc comments, and close the item.
3. If the repro fails, timebox the preferred fix to 1 day.
   - Own the Windows accept wait, the same way v0.26.0 owns the receive in `stream_transport.zig`.
   - When `close_requested` is set, map `STATUS_CANCELLED` to `error.ListenerClosed`. Otherwise, post the wait again.
   - Risk: std's `openSocketAfd` is private (`Threaded.zig:12682`), and std's accept is three AFD ioctls plus a socket open (`:12832`, `:12852`, `:12872`). The v0.26.0 receive change alone was a 539-line diff (`27c3d84` and its siblings).
4. Fallback at the end of the timebox:
   - `Listener` counts its parked acceptors and uses WorkerPool's order: nudge, wait for the parked count to reach zero, then close. `close` stays `void`.
   - Under port exhaustion this wait has no bound. `worker_pool.zig:379-380` says that a timeout cannot make the close safe. Document this limit.
5. Add a test knob that makes WorkerPool's nudges fail. Use it for a deterministic test of the exhaustion-shutdown path. This test does not need real port exhaustion, so P0-2's port budget can land first.
6. Correct the doc comments in `runtime.zig` and `worker_pool.zig`. Add a CHANGELOG `Fixed` entry.

**Done when**

- Red before: the fix commit cites the repro's failing Windows run id. If the item closes at step 2, it cites the 3 green run ids.
- Green after: the repro passes on windows-latest in Debug and ReleaseSafe, and on Linux and macOS.
- Forced-nudge test: nudges fail for 500 ms and then succeed. Shutdown finishes, and no close happens under a parked accept.
- With the own-wait fix only: shutdown also finishes within 5 s while nudges fail for the whole test.
- `zig build check-api` is green, and `docs/api-snapshot.txt` is unchanged.

## P1-2. QUIC: end the session when stream 0 ends, and add an e2e-over-QUIC CI lane

**Why**

- Over QUIC, a question that is still waiting when the peer ends stream 0 waits for its full deadline and then ends with `CallTimedOut`. Over TCP, EOF gives `Disconnected` at once.
- capnp-swift `docs/handoffs/quic-server-close-residual.md` (2026-10-08) records check 9 failing this way. It names "Fix path B (capnp-zig)". The 30 s in that report is the question deadline, not the idle timeout.
- Evidence (verified on `bbf91e7`):
  - `baseline_engine.zig:163-177` (`readStream`) returns silently on `n == 0` and on `StreamNotFound`. `native_engine.zig:248-268` does the same.
  - No owned-loop file calls `streamRecvEnd` for stream 0.
  - The embedded seat (`embedded.zig:428-440`) sends `.reset`, `.unknown` and `.stopped` to `requestControlStreamLoss`. That function closes with `normal` and records cause `.transport_error` (`:550-553`). A `.fin` only sets `buf.ended`.
  - The frozen wire section (`docs/quic-transport.md:306-333`) gives FIN no meaning.
  - `CHANGELOG.md:27-36` (0.26.0) says: "No build step or CI job runs this mode yet" for e2e over QUIC.
- The RESET and stop half makes the owned loops match the seat. It does not fix H11 check 9, because Swift cannot send a RESET. Only the FIN half (D2 option A) fixes that check.

**Work**

1. RESET and stop parity. This can start on day 1.
   - In each service pass of `baseline_engine.zig` and `native_engine.zig`, check the end state of stream 0.
   - On a reset, a stop or an unknown end, close the same way the seat does: close code `normal`, cause `.transport_error`.
   - `stream_end_support.zig:1056` asserts `transport_error` for the seat. Keep the two paths the same.
2. FIN, only if D2 is option A by day 3:
   - In the owned loops and in the seat's `.fin` arm, dispatch every whole buffered frame. Then close with `normal` and cause `.peer_close`.
   - A partial frame at FIN is a `frame_error` close.
   - Document the rule and the ALPN reasoning from D2 under "Wire constants (frozen)" in `docs/quic-transport.md`.
3. Add a `zig build e2e-quic` step and a Linux CI job. It runs the Zig e2e server and client with `--transport quic` over all 7 schemas and uses a test certificate. This lane runs the mode that capnp-swift tests against.
4. Write a handoff note for the owner to relay to capnp-swift. Do not edit that repo.

**Done when**

- Red before: new baseline and native tests in `tests/rpc/transport/quic/` RESET stream 0 while a question with a 5 s deadline is waiting. On `bbf91e7` the question ends `CallTimedOut` after 5 s.
- Green after: the question ends with `error.Disconnected` in under 1 s. The tests assert close code `normal` and cause `.transport_error`.
- With D2 option A:
  - The same test with a FIN ends `Disconnected` in under 1 s, with cause `.peer_close`.
  - Ablation: restore the old `buf.ended = true`-only arm and the old return on `n == 0`. The FIN test then times out.
  - A partial frame at FIN gives a `frame_error` close.
- `mise exec -- zig build -Dquic=true test-rpc-quic` and the QUIC evidence lanes are green on all 3 OSes.
- The new `e2e-quic` CI job is green.

## P1-3. Native C ABI: follow cap-free tail-call Returns (`takeFromOtherQuestion`)

**Why**

- capnp-swift lists "C++ promise/tail-call" as a release boundary (`release-readiness-2026-10-13.md:40`). Its ask is open: `~/prj/zig/capnp-swift/docs/handoffs/tail-call-returns-for-native.md`.
- The `else` arm of `fillReturn` in `src/native/conn.zig:770-775` emits the exception "capnp-swift core: unsupported Return variant".
- The Peer delivers a tail call inline only when the referenced answer's results are already stashed and carry no capabilities (`src/rpc/peer/mod.zig:5968-5986`).
- capnp-swift hosts answer asynchronously, so the referenced answer is usually still pending. The native callback then gets the bare `takeFromOtherQuestion` tag.
- The C++ `resolve_disembargo` tail call is `getNumber -> (n :UInt32)`, which carries no capabilities.
- Results that carry capabilities are never stashed. `completeSelfLoopbackReturn` (`src/rpc/peer/return/peer_return_send.zig:184-223`) rolls back their outbound capability effects. The shim has nothing to copy in that case.
- No commit to `src/native` since `5a6c99f` (2026-10-08) handles this.

**Work**

1. Scope: the cap-free case where the referenced answer Q is still pending when the `takeFromOtherQuestion` Return arrives.
2. When that happens, hold the Return. When `completeSelfLoopbackReturn` stashes Q's results, deliver them through the existing `deliverStashedLoopbackResults` path. If Q ends with an exception, deliver that exception. This change probably touches the Peer's tail-call path (`mod.zig:5968-5986`), not only the shim. All Peer users then get the results instead of the bare tag. Add a CHANGELOG entry.
3. Get the order right:
   - Copy Q's results before anything releases them.
   - Each question gets exactly one RETURN (capnp-swift `claims.json` #5).
   - Map `resultsSentElsewhere` to its documented terminal.
4. Leave results that carry capabilities as a documented limit in `docs/native-abi.md`. That case needs a Peer return-path change with capability-ownership risks. It is a later item.
5. Add tests in `src/native/conn_test.zig`. Write a handoff note for the owner to relay. Do not edit capnp-swift.

**Done when**

- Red before: a `conn_test` case where Q is still pending gets the "unsupported Return variant" exception on `bbf91e7`.
- Green after:
  - That case gets exactly one RETURN that carries Q's payload.
  - A case where Q fails gets exactly one exception RETURN.
- Ablation: restore the old fall-through. The pending case fails again.
- The cap-free stashed case already passes on `bbf91e7`. Keep it as a guard.
- A cap-bearing case asserts the documented limit: one exception RETURN, and no leak under the testing allocator.
- `abi_header_test.zig` and `include/capnp_core.h` are unchanged. `zig build test` is green.
- Not a sprint gate: capnp-swift's `resolve_disembargo` run against the C++ `e2e_server`, after it pins v0.27.0.

## P1-4. Tag gate: require 3 Nightly runs on the release commit, with a known-flakes list

**Why**

- `release-tag` (`Justfile:456-462`) and `release.yml:36-51` check only runs named "CI".
- Nightly was red on 2 of the last 5 runs, and nothing linked that to tagging.
- The repo has 0 issues, so a flake has no place to be recorded except agent memory.
- `release-tag` requires the `build.zig.zon` version to equal VERSION at HEAD (`Justfile:459`). So the candidate is always a new release commit, and the daily cron (`nightly.yml:4-6`) has not run on it yet.
- Nightly accepts `workflow_dispatch` (`nightly.yml:7`). A run takes about 19-21 min of wall time (runs 38128471120 to 38713314813).
- `release.yml` runs on the tag push (`release.yml:11-14`). It cannot refuse a tag. It can only flag one after the tag exists.

**Work**

1. Add `.github/known-flakes.txt`. Each row has: the Nightly job name, an optional test or lane name, a run id, an owner, a date and an expiry date.
2. Add a final step to each Nightly job with `if: failure()`. It emits one `::error title=nightly-failure::<name>` annotation for each `error: '<test>' failed` line and each `soak: FAIL` line. The check-runs annotations API returns these annotations. No API returns a job summary.
3. Add `just check-nightly-gate SHA`:
   - It reads the 3 most recent completed Nightly runs whose `head_sha` is SHA.
   - It passes only if there are 3 runs and each run is green or fails only on matching known-flakes rows. A row matches by job name, and also by test name when an annotation exists.
   - `release-tag` calls it after the CI check.
4. Add the same check to `release.yml` as a detective check. It turns the release workflow red when it fails. Do not describe it as a refusal.
5. In `RELEASING.md`, describe the steps: freeze main, start 3 Nightlies with `gh workflow run Nightly --ref main`, and apply the known-flakes rule.

**Done when**

- `just check-nightly-gate bbf91e7` refuses. Its 3 latest Nightlies are 38713314813 (failure), 38542668305 (success) and 38377271133 (failure).
- With 2 unexpired rows for those 2 failing jobs, the check passes. When the rows expire, it refuses again.
- A SHA with no Nightly run is refused.
- The first red Nightly after this lands has an annotation that names the failing test.

## P1-5. CI headroom: package preflight on Linux only per push, a timeout on every job

**Why**

- Test (windows-latest) took 42.0 of its 45 min (`ci.yml:99`, run 38108586653).
- About half of that is the step "Clean-room filtered-package consumers" (`ci.yml:191-195`): 21.1, 20.9 and 20.9 min in runs 38108586653, 38101997379 and 38033933043.
- `release-safe-tests` (`ci.yml:772-776`, 60 min) took about 48 min on Windows in run 38033933043. The scout measured a 49.0 min peak on macOS. That job does not run the preflight, so step 1 does not help it.
- Seven jobs have no timeout: fmt-check, evented-check, docs-smoke, cross-targets, bench-check, wasm-build and release-build. A hung job would run up to GitHub's 6-hour default.

**Work**

1. In `ci.yml`, run the package preflight step on Linux only for each push.
2. In `nightly.yml`, add a 3-OS package-preflight job. Land it after P0-2 and P1-4 change the same file. The P1-4 gate requires green Nightlies on the candidate, so it also covers the 3-OS preflight. No separate tag check is necessary.
3. Add `timeout-minutes` to the 7 jobs, at about 1.5 to 2 times their measured maximum. Wrap the evented-check e2e step and the bench-check step in `tools/stall_watchdog.sh`.
4. Raise `release-safe-tests` to 75 min. Update its comment with the new measurements.
5. Add a Linux CI step that uses `yq` to assert that every job in `ci.yml` and `nightly.yml` has a job-level `timeout-minutes`.

**Done when**

- The Windows Test job finishes in under 25 min on 3 consecutive pushes.
- The `yq` check is green.
- Ablation: delete one job's `timeout-minutes` on a draft PR, and the check goes red. A `grep -c` count is not enough, because step-level timeouts at `ci.yml:160` and `:571` already match.
- The Nightly 3-OS preflight job is green in the 3 ceremony runs.

## P2-1. Windows reads: map `STATUS_LOCAL_DISCONNECT`, and file the std read handoff

**Why**

- `windowsReceiveResult` (`src/rpc/transport/tcp/stream_transport.zig:2370-2379`) maps `CONNECTION_RESET` and `REMOTE_DISCONNECT`, but not `LOCAL_DISCONNECT`. That status falls to `unexpectedStatus`, which prints a stack trace.
- The Windows soak log of Nightly 38377271133 has 2,989 lines with `0xc000013b`. Of the stack frames, 2,932 point at `stream_transport.zig:2377`. They come from the Debug 120 s lane, where `transport_errors=2619` equals `chaos_closes`.
- At least 10 traces come from std's `netWriteWindows` (`Threaded.zig:13528`), and this item does not change that path.
- `docs/upstream/handoff-zig-fork-netacceptwindows.md` (`:96-150`) already asks for the read arm.
- No handoff covers the stray `CANCELLED` in std `netReadWindows` (`Threaded.zig:13010`), which v0.26.0 works around.

**Work**

1. Add `.LOCAL_DISCONNECT => error.ConnectionResetByPeer` to `windowsReceiveResult`.
2. Add a case to the test at `:2478`. std has no `LOCAL_DISCONNECT` arm, so retitle that test or add a comment about the divergence.
3. Check that the soak tolerance still balances: `transport_errors` must equal `chaos_closes` in each lane.
4. Extend `handoff-zig-fork-netacceptwindows.md`, or add `handoff-zig-fork-netreadwindows-stray-cancel.md`. Cover the read arm and the write arm, and cite runs 38039927959 and 38083790370 ("flake-tcp-rt3/rt4 loop").
5. Do not move code in `stream_transport.zig`.

**Done when**

- Red before: the new case fails on `bbf91e7`, which returns `error.Unexpected`.
- Green after: the case passes on windows-latest.
- The next Windows Nightly soak log has 0 traces with `windowsReceiveResult` in the frame. Traces from `netWriteWindows` can remain, and the handoff covers them.
- The handoff has a status line dated 2026-10-15 or later.

## P2-2. Fix the stale contract-doc claims and ratchet the QUIC test floors

**Why**

- Every pre-tag audit finds many wrong claims. Each audit found 19-35 (task context; see `2c0457d`, `75a4eac`). Docs that the audit does not read are also wrong:
  - `docs/stability.md:48` gives the floors "26 + 1 + 17 + 8 = 52". `tools/quic_test_evidence.zig:11-14` enforces 43/1/36/37. Its `if (total < 52)` check (`:125`) can never fail.
  - `docs/stability.md:72-74` calls quic-zig v0.24.0 "the pinned tag". `build.zig.zon` pins v0.38.0.
  - `docs/stability.md:145-147` says that the Windows job places `capnp.exe` on `PATH`. CI now runs `mise run bootstrap:capnp` (`.github/actions/setup-capnp/action.yml:14`).
  - `docs/supported-surface.md:25` says "within 0.3.x".
  - `docs/quic-transport.md:37-40`: the wording about the Windows lane (scout-reported; read it again before you edit).

**Work**

1. Fix each stale claim. Read each one again before you edit it.
2. Raise the floors in `tools/quic_test_evidence.zig` to the counts after P1-2 lands. Make the total check equal the sum of the floors, or delete it.
3. Land this after P1-2 (which adds QUIC tests) and after D1 (which sets the pin text).

**Done when**

- `grep` finds no "the pinned tag" next to v0.24.0 and no "0.3.x" in those docs.
- The floors in `stability.md` equal the values in `quic_test_evidence.zig`.
- Ablation: remove one test from `rpc_quic_peer_test.zig` on a scratch commit, and `quic_test_evidence` fails.
- `zig build docs-smoke` is green.
- The v0.27.0 pre-tag audit commit message records its finding count.

## P2-3. One ledger for downstream asks and one for upstream handoffs

**Why**

- The status of downstream asks lives only in the downstream repos, and it goes stale.
- `docs/capnp-deno-upgrade-handoff.md` was last changed in `31a2ec3` (2026-07-06). It names quic_zig 0.7.0.
- `docs/upstream` has 12 handoff files and no index.
- `tests/native/fuzz_abi.zig:18` cites `handoff-zig-fork-debug-allocator-slabs.md`, which is not in `docs/upstream`. It exists only in capnp-swift's `docs/handoffs/`.
- `handoff-zig-fork-ios-nullfile.md` still says DRAFT, but upstream 0.17.x `f842de1845` fixed it (unverified).
- This sprint closes two capnp-swift asks. The owner needs one place to relay them from.

**Work**

1. Create `docs/downstreams.md`. Add one row per consumer and ask: the pin, the ask, the handoff path, and the status (delivered in vX, open, deferred or declined).
   - capnp-swift: H3, H9, H11, H12 (deferred; workaround is `Context.init` with full nodes), tail-call, and the native ABI items.
   - capnp-deno: feature bits 10-12.
   - slcp-zig.

   This file replaces `docs/capnp-deno-upgrade-handoff.md`.
2. Create `docs/upstream/README.md`. Add one row per handoff: the status, where it is fixed (ref and commit), and which capnp-zig workaround to delete when the fix ships.
3. Copy the debug-allocator-slabs handoff into `docs/upstream`.
4. Add both ledgers to the `RELEASING.md` checklist.
5. Put the relay text for capnp-swift in the ledger for the owner to send. Do not edit downstream repos.

**Done when**

- Both ledgers exist, and every row is dated 2026-10-15 or later.
- The stale doc is deleted, and `grep` finds no reference to it.
- The `fuzz_abi.zig` citation resolves inside the repo.
- `RELEASING.md` names both ledgers.
- `zig build docs-smoke` is green.

## S-1 (stretch). Test: a back-pressured QUIC client that gets a stateless reset

**Why:** `CHANGELOG.md:270-274` (0.25.0) says that a back-pressured client that got a stateless reset "hung the same way; no test covers that path". It is the same class of close path as P1-2, and it uses the same harness.

**Work:**

1. Add a test in `tests/rpc/transport/quic/`. A client has frames queued under back-pressure, and the server restarts with the same `stateless_reset_key`.
2. Reuse the existing crash-restart harness (named `crashRestartCertificate` by the critique; unverified).

**Done when:**

- The client ends within the test deadline on all 3 OSes.
- Ablation: disable the v0.25.0 close-at-once path, and the test times out.

## Order of work

**Landing rule.** Each item gets its own worktree and its own draft PR.

- CI runs only on PRs to main and on pushes to main (`ci.yml:3-7`). It cancels in-progress runs per ref (`ci.yml:23-25`), and a cancelled run can save a poisoned `.zig-cache` (`ci.yml:12-22`).
- So merge one PR at a time. Let main's CI finish (about 45 min) before the next merge.
- Do not run agents in the shared main checkout.
- In each new worktree, init the submodules and run `mise run bootstrap:capnp`.

**Day 1**

- Ask the owner D1 and D2.
- Start these items in parallel, because they touch different files:
  - P0-1, commit A: `warm_redial.zig`, `test_knobs.zig`, `rpc_quic_peer_test.zig`.
  - P0-2: `tools/soak_rpc.zig`, `nightly.yml`.
  - The P1-1 repro: a new test file only.
  - The P1-2 RESET and stop half: `baseline_engine.zig`, `native_engine.zig`.

**Days 2-3**

- P0-1 commit B, after the red run id exists.
- Land P0-2.
- P1-1 fix, inside its timebox.
- P1-3: `src/native`, `src/rpc/peer`.
- P1-2: the e2e-quic lane.
- End of day 3 is the decision cut-off. Without a D2 answer, P1-2 ships the RESET and stop half only. Without a D1 answer, the pin stays v0.38.0.

**Day 4**

- One agent does P1-4 and then P1-5. Both edit `nightly.yml` after P0-2. P1-4 also edits the `release-tag` recipe and `release.yml`.
- The P1-2 FIN half, if D2 is option A.
- P2-1.

**Day 5**

- P2-2, then P2-3. These are docs, so they go last and describe the final state.
- S-1, if there is time.

**Days 6-7: the v0.27.0 ceremony**

1. Do the `RELEASING.md` sweep and the version bump.
2. Run the pre-tag audit, and record the finding count.
3. Push and wait for green CI. Freeze main.
4. Start 3 Nightlies on the candidate with `gh workflow run Nightly --ref main` (about 20 min each).
5. Run `just release-tag 0.27.0 "..."`, which now runs `check-nightly-gate`.
6. Run `just verify-release-hash 0.27.0`.
7. Update both ledgers, and give the owner the handoff notes to relay.

## Not in this sprint

- **Moving quic-zig or http3-zig.** This is D1. With the recommended option, the pin stays v0.38.0.
- **The type_resolver lookup-mode defect (H12).** It is real: `lexicalParent` (`src/serialization/type_resolver.zig:337-351`) scans nodes that `initWithLookup` leaves empty. But capnp-swift has a production workaround (`Context.init` with full nodes). Do it with direction #5, which needs the same reverse lookup. Whether codegen is affected is unverified. `brand_fidelity.zig:423` is in a unit test.
- **Native tail-call Returns with capabilities.** This needs a Peer return-path change (`peer_return_send.zig:184-223`). It is a documented limit this sprint.
- **Direction #2 heavy items.** These are: the test census, the merge of about 111 test roots, a docs-only fast lane, and fact-anchor lint in docs-smoke. The census comes first, because the root merge needs it as proof. These are the next sprint.
- **The Windows timer-tick flake class.** Four commits (`c02e088`, `8103f29`, `a35e56c`, `f101a68`) each widened one test's deadline. No shared deadline helper exists. Until the next sprint, record each new case as a known-flakes row (P1-4).
- **The native QUIC deadlock for frames over about 2 MiB** (`native_outbound_queue.zig:178-204`). Native mode is Experimental and opt-in, and the limit is documented in `docs/quic-transport.md`. It belongs to the next QUIC tranche.
- **A local promise capability, or pipelined params on a pending answer (native H9).** This is L effort and needs a direction #7 design.
- **Direction #6:** the 1.0 charter, Stable promotions and narrowing `anyerror`. `docs/api-snapshot.txt` stays byte-identical.
- **Direction #5:** schema tooling (JSON and text codecs, an inspector CLI).
- **The native ABI v1 feature-bit bundle.** It changes the header that capnp-swift snapshots, so it waits until capnp-swift has consumed the tail-call work.
- **Refactor splits of `struct_gen.zig` and `stream_transport.zig`.** Do not move code in files that this sprint changes for behaviour.
- **The Zig fork, or enabling Evented.** The toolchain stays at stock 0.17.0, and `just check-evented` is unchanged.
- **A `just downstream-check` canary.** The downstream repos are local or private.
- **Editing any downstream repo.** All coordination goes through handoff text that the owner relays.

**The rejected theme.** The strongest alternative was a "CI speed and honesty" sprint (direction #2). It would group about 111 test roots into 12-16 domain aggregates, add a docs-only fast lane and add a test census. Its case is real: the Windows jobs run at 80-93% of their timeouts, and the ReleaseSafe legs grew fast. I rejected it for this sprint for four reasons:

1. Nightly is red now, on two different causes.
2. A Stable Windows API may abort the process.
3. Two capnp-swift release boundaries wait on capnp-zig fixes.
4. The root merge is L effort, it can expose order-dependent failures from shared global state, and it needs the census first.

P1-5 buys down the timeout risk cheaply. Direction #2, with the census first, should be the next sprint.

## Hygiene

The owner runs these steps. This sprint does not delete branches or worktrees.

- `/Users/nullstyle/conductor/workspaces/capnp-zig/hong-kong`: git reports "not a git repository". Its branch `nullstyle/hong-kong` (`3a7e7c5`) is merged. Run `git worktree prune`.
- `.claude/worktrees/elated-panini-929a49` (`d95ca38`): the same change is on main as `13221cd`. Remove it.
- `.claude/worktrees/ecstatic-bhaskara-0aa300` (`5fabb36`): merged. Remove it.
- `/Users/nullstyle/prj/zig/capnp-zig-zcode` (`86c9185`, e2e-quic-modes): merged. Remove it after you check with the zcode agent's owner.
- `codex/downstream-wasm-abi` (`da302d5`, worktree `/Users/nullstyle/prj/zig/capnp-zig-wasm-abi-feedback`): not merged. Decide whether to keep or archive it. Read its content before you delete it.
- Local branches: 115 in total, and 99 are merged into main (`git branch --merged main`). Delete the merged ones. Archive-tag `4434215` and `spike/evented-tcp-writer` first, because the draft reported unique content there (unverified).

## Evidence

**CI runs**

- Nightly 38713314813 (10-15, failure; job 116349833595, Extended gates)
- Nightly 38377271133 (10-13, failure; job 115228694695, Windows soak)
- Nightly 38542668305, 38231937129, 38128471120 (green on `bbf91e7`)
- Nightly 38041469279 (green on `2cba5a1`)
- CI 38108586653, 38101997379, 38033933043 (Windows timings)
- Nightly 33029339794 (first WorkerPool accept-teardown abort)
- 38039927959 and 38083790370 ("flake-tcp-rt3/rt4 loop", the stray-cancel proof)

**Logs**

- `n1015.log` (failure at `:1275-1291`)
- `n1013.log` (soak summary near `:1017470-1017566`)

**Commits**

- capnp-zig: `bbf91e7`, `75a4eac`, `2c0457d`, `75557be`, `812b798`, `27c3d84`, `13221cd`, `d95ca38`, `5a6c99f`, `31a2ec3`, `c02e088`, `8103f29`, `a35e56c`, `f101a68`
- quic-zig: `021591e` (in no tag); latest tag v0.42.2

**Files read on `bbf91e7`**

- `src/rpc/transport/quic/warm_redial.zig`
- `tests/rpc/transport/quic/rpc_quic_peer_test.zig`
- `tools/soak_rpc.zig`
- `.github/workflows/nightly.yml`
- `.github/workflows/ci.yml`
- `.github/workflows/release.yml`
- `Justfile`
- `src/rpc/transport/tcp/runtime.zig`
- `src/rpc/integration/worker_pool.zig`
- `docs/api-snapshot.txt`
- `src/rpc/peer/mod.zig`
- `src/rpc/peer/return/peer_return_send.zig`
- `src/native/conn.zig`
- `src/rpc/transport/quic/baseline_engine.zig`
- `src/rpc/transport/quic/embedded.zig`
- `docs/quic-transport.md`
- `docs/stability.md`
- `docs/supported-surface.md`
- `tools/quic_test_evidence.zig`
- `CHANGELOG.md`
- `.github/actions/setup-capnp/action.yml`
- `docs/upstream/`
- quic-zig `CHANGELOG.md`

**Fact-check and critique inputs:** std 0.17.0 `lib/std/Io/Threaded.zig` lines `:12682`, `:12832-12881`, `:13010`, `:13528`, and the capnp-swift handoffs (`quic-server-close-residual.md`, `tail-call-returns-for-native.md`, `H12-type-resolver-lexical-lookup.md`).
