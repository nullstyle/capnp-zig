# Sprint plan 2026-10-21: Cancel-safe for downstreams, cheaper to land

First, fix the answer-path defects that capnp-swift 0.2.0 and capnp-deno can hit. Then prove cancel and id reuse against the reference implementations. Then take the cheap, measured parts of direction #2 ("CI speed and honesty"). The large test-root merge waits for the census.

- **Goal:** Ship v0.29.0 with these results:
  1. ANSWER_FINISHED does not fire while calls are pipelined on the answer. (A pipelined call targets the result of an earlier call that has not returned yet.)
  2. HostPeer exception answers fail the calls that are pipelined on them. A `sendResultsTo=yourself` call gets the correct Return.
  3. The three native answer functions do nothing after the connection closes.
  4. A cross-implementation e2e scenario covers cancel, then id reuse. It also covers cancel with a pipelined call.
  5. A push that changes only Markdown runs a short Docs workflow, not the 30-job CI matrix.
  6. A gated test census exists. CI pins its Ubuntu image and checks job headroom. Release preflight lists the CI runs that went green only on a re-run.
  7. The H9-pending design note is written and reviewed. Its code can start next sprint.
- **Length:** 8 working days, from 2026-10-22.
  - Days 1-6 are item work: about 13 agent-days, done by 3 agents in separate worktrees.
  - Days 7-8 are the v0.29.0 ceremony.
  - If D1 is option A, the v0.28.1 patch ceremony uses about 1-1.5 days of main time on days 2-3. Branch work continues during it.
  - If work slips, cut in this order: P1-6 commit B, P2-4, P2-5, P2-3 (move it after the tag), then P1-3 scenario 1.
- **Release target:** v0.29.0, a minor release, about 2026-10-30.
  - Before it, v0.28.1, a patch with `bdc656f`, P0-1 and P1-2, if D1 is option A.
  - `docs/api-snapshot.txt` (the Stable API) stays byte-identical. The changed surfaces are Experimental: `Peer.sendReturnCanceled` (a new refusal), `Peer.sendPrebuiltReturnFrame`, and the native answer functions. The `capnp_core.h` changes are comments only.
  - The quic-zig pin stays at v0.38.0 unless D2 is option A.
- **Main:** `bdc656f` = origin/main. The working tree is clean.
  - The v0.28.0 tag is at `73416c7`. The hash commit is `7843485`.
  - `bdc656f` ("rpc: answer a parked call that the replay drain cannot decode again") is the first `[Unreleased]` change.
  - The last 4 main CI runs are green: 39541793776 (`bdc656f`), 39530075877, 39522707768, 39511977861.
- **Nightly:** green since 2026-10-18:
  - 39577274345 (10-21, schedule, `bdc656f`)
  - 39522716854, 39522719636, 39522722588 (10-21, dispatched on `73416c7`, the v0.28.0 tag gate)
  - 39489296792 (10-20, dispatch), 39400927246 (10-20), 39231302956 (10-19), 39115213373 (10-18)

## Decisions for the owner

**Decided by the owner on 2026-10-21:**

- **D1: option A.** Cut a v0.28.1 patch with `bdc656f`, P0-1 and P1-2.
- **D2: option B.** v0.29.0 stays on quic-zig v0.38.0. Send the tag request now (P2-2), and move cluster B in its own release after the tag exists.

The text below is the question as the owner saw it.

### D1. How does the P0-1 fix reach capnp-swift 0.2.0?

capnp-swift 0.2.0 shipped on 2026-10-21 on v0.28.0. Its default `cancelHandlersOnFinish = true` (`Sources/CapnpRPC/RPCConnection.swift:57`) turns on ANSWER_FINISHED (`:169-170`). Because of the P0-1 defect, a call that is pipelined on a finished answer can then fail.

- **Option A:** Cut v0.28.1 on day 2, with `bdc656f`, P0-1 and P1-2. Use the full ceremony and short release text. capnp-swift then makes one 0.2.1 with one pin bump and one header-snapshot update. It keeps cancel on by default.
- **Option B:** Make no patch. The fixes ship in v0.29.0. Until then, capnp-swift ships a 0.2.1 that sets `cancelHandlersOnFinish = false`. With false, capnp-swift does not set `answer_finished` (`:169-170`). The core then sends no ANSWER_FINISHED, and handlers run to the end. This is the behavior before H13, and it is correct. capnp-swift turns the setting on again after its v0.29.0 pin bump.
- **Recommendation:** Option A.
- **Why:** The fix is small, red-first and in one area, so the patch text and the audit surface are small. capnp-swift keeps its released default and makes one release, not two. P0-1 and P1-2 both change `capnp_core.h` comments, and that trips capnp-swift's header snapshot gate at the pin bump (`docs/downstreams.md:58` says this is expected). Option A puts both changes into one update. The cost is about 1-1.5 days of main time for a second ceremony. The cut order above pays for it. Pick B if you want only one ceremony in this sprint. B is safe.

### D2. Does cluster B move off quic-zig v0.38.0 in this sprint?

On 2026-10-16 you chose to keep v0.38.0 until quic-zig tags `021591e` (sprint plan 2026-10-15, D1 option B). It is still in no tag: `git tag --contains 021591e` in `~/prj/zig/quic-zig` prints nothing. The latest tag is v0.42.2. The local tip `b0d7d78` contains `021591e`.

- **Option A:** Send the tag request on day 1 (P2-2). If quic-zig tags by day 3, move capnp-zig in v0.29.0. Re-run the `-Dquic` api-snapshot gate, the QUIC soak and e2e-quic. Pair the move with http3-zig v0.5.8.
- **Option B:** Send the tag request on day 1, but keep v0.29.0 on v0.38.0. After the tag exists, move cluster B in its own release (v0.29.1 or v0.30.0).
- **Recommendation:** Option B.
- **Why:** No tag exists yet. This week already changes the Peer and HostPeer answer paths. A four-minor QUIC jump in the same release makes a regression hard to place. `SECURITY.md:44-45` excludes DoS against the QUIC transport while it is Experimental. The downstream scout reads `021591e` as a fix for a pre-auth transport-parameter scan (unverified). No DoS was measured.

## Items

| id | priority | title | effort | depends on |
|---|---|---|---|---|
| P0-1 | P0 | ANSWER_FINISHED: do not report it while calls are pipelined on the answer | S (0.75 day) | none |
| P1-1 | P1 | HostPeer exception answers: fail the pipelined calls, record the failed answer, honor `sendResultsTo=yourself` | M (1-1.5 days) | none (lands after P0-1) |
| P1-2 | P1 | Native answers are a no-op once the connection is closed | XS (0.25 day) | none (lands right after P0-1) |
| P1-3 | P1 | Cross-implementation e2e: cancel then id reuse, and cancel with a pipelined call | L (2.5 days) | P0-1 (red evidence from a branch at `bdc656f`) |
| P1-4 | P1 | Docs lane: Markdown-only pushes skip the 30-job CI matrix | S (0.5 day) | lands after the v0.28.1 tag if D1 = A |
| P1-5 | P1 | Test census: unique root names, a wiring check, a root ratchet, per-root cost | M (2-2.5 days) | none |
| P1-6 | P1 | CI inputs: pin `ubuntu-24.04`, raise the QUIC Windows cap, add a job-headroom gate; then pin uv, just and pycapnp | S (0.75 day) | commit B: P1-5 |
| P2-1 | P2 | H9-pending design note (design and review only, no code) | M (2-3 days elapsed, in parallel) | none |
| P2-2 | P2 | Refresh both ledgers, write the quic-zig tag request, fix the evented-canary advice | S (0.5 day) | none |
| P2-3 | P2 | Soak lanes: quiet the debug logs and make a silent exit diagnosable | S (0.5 day) | must land by day 3 |
| P2-4 | P2 | Fact anchors for the mechanical audit class, and claim counts at the cut | S (0.75 day) | none |
| P2-5 | P2 | List the CI runs that went green only on a re-run, in release-preflight | S (0.5 day) | none |

## P0-1. ANSWER_FINISHED: do not report it while calls are pipelined on the answer

**Why**

- The spec lets a callee cancel a call only when nothing else cares about the result, "e.g. there are no other outstanding calls pipelined on the result" (`src/rpc/capnp/rpc.capnp:579-582`).
- The notify gate at `src/rpc/peer/mod.zig:4875-4880` checks `host_owes_return`, `!canceled_automatic_target` and `hostOwesFinishedReturn(qid)`.
- `hostOwesFinishedReturn` (`mod.zig:4916-4923`) checks `hasQueuedPendingQuestionId(answer_id)`. That asks if the answer itself is queued. It does not check `pending_promises[qid]`, which holds the calls queued on the answer.
- When the host then answers canceled, `sendReturnCanceled` calls `failQueuedPromisedCalls(answer, "call canceled")` (`src/rpc/peer/return/peer_return_send.zig:742-747`). Every call pipelined on that answer fails.
- A C++ client makes this wire sequence for `foo().getSub().bar()` when it drops foo's promise: Call(foo), Call(bar on foo), Finish(foo).
  - QuestionRef sends Finish on its last unref (`vendor/ext/capnproto/c++/src/capnp/rpc.c++:2362-2398`). PipelineClient holds a QuestionRef (`:1233-1240`). The Finish has `requireEarlyCancellationWorkaround=false` (`:2387`).
  - The C++ server lets bar finish. LocalClient::call gives the pipeline its own branch (`capability.c++:661-679`). handleFinish only drops the completion task (`rpc.c++:4131-4160`).
- capnp-swift 0.2.0 (its tag contains `f1f669b`) is hit by default:
  - `cancelHandlersOnFinish = true` (`RPCConnection.swift:57`) sets `answer_finished = 1` (`:169-170`).
  - `answerFinished` (`:847-857`) cancels the Task and calls `capnp_return_canceled` with no other check.
  - So bar fails when the Finish reaches the core before the Swift handler answers foo. Nobody has run this against a Swift server (unverified in practice).
- capnp-deno is hit too, at its pin `fe6634d` (WASM feature bit 12):
  - Its RpcServerBridge (`640d8a4`) calls `sendReturnCanceled` for every answer-finished event (`src/rpc/server/bridge.ts:1237-1262`).
  - That goes through `capnp_peer_send_return_canceled` into `Peer.sendReturnCanceled` (`src/wasm/capnp_host_abi.zig:2047-2063`).
  - The bridge sets `entry.finished = true` before the call and swallows any error. It then drops the handler's late answer.
- The Rust client was not checked (unverified).

**Work**

1. Red first. Write three tests:
   - In `src/native/conn_test.zig`, with `answer_finished = 1`: Call 30 to an export. Call 40 on `promisedAnswer(30, [getPointerField 0])`. Finish 30 with `requireEarlyCancellation = false`. Expect only `{INBOUND_CALL}`. Then `returnResults(30, export)` delivers INBOUND_CALL 40 exactly once.
   - A Peer twin in `tests/rpc/peer/rpc_answer_lifecycle_test.zig` with `setAnswerFinishedHandler`: the hook is not called, `sendReturnCanceled(30)` refuses, and nothing goes on the wire.
   - A WASM twin in `tests/wasm_host_abi_test.zig`: the same sequence gives no answer-finished event, and `capnp_peer_send_return_canceled` refuses.
2. Fix the gate. Add `and !self.pending_promises.contains(qid)` to the notify condition (`mod.zig:4875`).
3. Make `Peer.sendReturnCanceled` refuse while `pending_promises` holds calls on the answer. Use a new error, for example `error.AnswerHasPipelinedCalls`.
   - The native `capnp_return_canceled` already returns `CAPNP_E_INVAL` in that case, because no effect was queued (`capnp_core.h:383-392`).
   - This refusal is safe for capnp-deno only because the hook no longer fires in that case. The WASM test pins this.
4. Optional: add a fuzz invariant in `tests/native/fuzz_abi.zig` (near `:249-255`): no ANSWER_FINISHED while a call is queued on that answer. The Nightly native fuzz then guards the fix.
5. Ablate the new condition. All three tests must go red.
6. Docs. The contract has hand-written copies. Change all of them in one commit:
   - `src/native/include/capnp_core.h:383-392` (comments only).
   - `src/native/conn.zig:735-745`.
   - `docs/native-abi.md:126-129` and its "When no effect comes" list.
   - In `src/rpc/peer/mod.zig`: the `setAnswerFinishedHandler` doc (`:1650-1686`), the `AnswerFinishedFn` doc (`:237-238`), the `sendReturnCanceled` doc (`:3917-3926`) and the gate comment (`:4865-4874`).
   - The WASM doc (`src/wasm/capnp_host_abi.zig:2041-2046`).
   - Add one more case. If the pipelined calls finish later and `pending_promises[qid]` empties, the host is not told. That wastes work, but it is correct.
   - A CHANGELOG `[Unreleased]` "Fixed" entry that names the tests.
7. Write relay rows in `docs/downstreams.md`:
   - capnp-swift: the pin bump and the header-snapshot update. Until the bump, the mitigation is `cancelHandlersOnFinish = false`.
   - capnp-deno: the fixed version. Until the pin moves, the mitigation is to not answer canceled on answer-finished events.

**Done when**

- Red before: all three tests fail on a branch at `bdc656f`. The native effects are `{INBOUND_CALL, ANSWER_FINISHED}`. Record the branch CI run id.
- Green after the fix. With the condition ablated, all three go red again.
- These pass: `mise exec -- zig build test-serialization` (it runs `src/native/conn_test.zig` through the core root, `src/lib_core.zig:36` and `build/build_impl.zig:1585`), `test-rpc-peer`, `test-native-abi` and `hardening`. `tests/wasm_host_abi_test.zig` passes in branch CI.
- Branch CI (`gh workflow run CI --ref <branch>`) is green before the push to main.
- All the doc copies in step 6 say the same thing. The `capnp_core.h` diff is comments only. `docs/api-snapshot.txt` has no diff.

## P1-1. HostPeer exception answers: fail the pipelined calls, record the failed answer, honor `sendResultsTo=yourself`

**Why**

- `HostPeer.respondHostCallException` (`src/rpc/integration/host_peer.zig:279-289`) calls `sendSanitizedReturnException`. That calls `peer.sendPrebuiltReturnFrame` (`host_peer.zig:672`).
- `sendPrebuiltReturnFrame` (`peer_return_send.zig:267-317`) has three gaps:
  - It clears the `yourself` routing before the send (`clearSendResultsRouting`, `:283`).
  - It records only `.results` answers (`:287`).
  - It never calls `failQueuedPromisedCalls`, and it never records a failed answer.
  - `sendReturnExceptionTyped` and `sendReturnExceptionNoDrain` (`:336-455`) do all three correctly.
- Result 1: a call that is pipelined on a HostPeer exception answer gets no Return until the connection drops. A call pipelined after the exception finds no failed-answer record. The correctness scout's scratch test showed this: parent 1 Return, child 0 Returns, `pending_promises.contains(parent) = true`, `failed_answers.contains(parent) = false` (scout run, not re-run).
- Result 2: for a `sendResultsTo=yourself` call (a tail call), an exception answer puts an exception Return on the wire. A results frame through `respondHostCallReturnFrame` (`host_peer.zig:370`) puts a results Return on the wire. The C++ reference rejects both for a tail call (`rpc.c++:4043-4044`, `4055-4056`) and drops the connection.
- `respondHostCallResults` is not affected. It goes through `sendReturnResults` (`host_peer.zig:334`), which handles `yourself` (`peer_return_send.zig:60-63`).
- capnp-deno already has this defect at its pin. `fe6634d` has the same `sendPrebuiltReturnFrame` shape (`git show fe6634d:src/rpc/peer/return/peer_return_send.zig`). capnp-deno answers throwing handlers with `respondHostCallException` (`src/rpc/server/bridge.ts:1600-1771`). Only a pin move to a fixed commit delivers the fix.

**Work**

1. Red-first tests in `tests/rpc/integration/rpc_host_peer_test.zig`:
   - (a) A call pipelined on a host call that `respondHostCallException` fails gets exactly one exception Return.
   - (b) A call pipelined after the exception gets one exception Return, because the failed answer is now recorded.
   - (c) The same as (a), through `respondHostCallReturnFrame` with an exception frame.
   - (d) A `yourself` host call answered with an exception sends a Return with tag `resultsSentElsewhere`, not an exception.
   - (e) A `yourself` host call answered through `respondHostCallReturnFrame` with a results frame gets the same outcome as `respondHostCallResults`: a `resultsSentElsewhere` Return. Cap-free results go to the stash for the `takeFromOtherQuestion`, as `completeSelfLoopbackReturn` does (`peer_return_send.zig:191-231`).
2. Fix `sendPrebuiltReturnFrame`:
   - Check `send_results_to_yourself` before `clearSendResultsRouting`.
   - For `.exception`: record the failed answer with the same gates as `sendReturnExceptionNoDrain`, then call `failQueuedPromisedCalls`.
   - For `.canceled` and `.resultsSentElsewhere`: drain only.
   - Another way: route `respondHostCallException` through `sendReturnExceptionTyped` with the sanitized reason. Pick the smaller diff and give the reason in the commit message.
3. Keep the redaction and param-cap tests green (`rpc_host_peer_test.zig:835`, `:1307`).
4. If a `catch {}` line moves, move its hardening allowlist entry in the same commit.
5. Docs: the HostPeer known limit in `docs/supported-surface.md:906-915`, a CHANGELOG entry, and a capnp-deno relay row in `docs/downstreams.md`.
6. Not in this item: one shared "Finish proves delivery" helper for all six Return senders. See "Not in this sprint".

**Done when**

- Tests (a)-(e) fail on a branch at `bdc656f` (child Returns = 0, wrong Return tags). Record the red run id. They pass after the fix.
- The existing HostPeer exception, redaction and param-cap tests pass.
- `test-rpc-integration`, `test-rpc-peer`, `hardening` and `check-api` pass. The Stable snapshot has no diff. (`Peer.sendPrebuiltReturnFrame` is Experimental, `docs/api-snapshot-experimental.txt:2571`.)
- The e2e matrix counts on branch CI are unchanged.
- `docs/supported-surface.md` no longer states the fixed limit.

## P1-2. Native answers are a no-op once the connection is closed

**Why**

- `returnResults`, `returnException` and `returnCanceled` (`src/native/conn.zig:709-750`) have no `isClosed()` check. `isClosed()` is `failed`, `remote_aborted` or `transport_closed` (`:901-903`).
- `bindingSend` refuses only on `transport_closed` (`conn.zig:1268-1269`). So after a remote Abort or a protocol failure, an answer still queues a Return OUT_FRAME after the Abort.
- The Peer's `handleAbort` (`mod.zig:4664-4678`) does not clear `pending_promises`. So a call parked on that answer replays, and it reaches the host as a new INBOUND_CALL after CLOSE_REQUESTED. The scout's scratch test showed `returnResults(30)` returned OK, queued 1 OUT_FRAME and delivered INBOUND_CALL 40 (scout run, not re-run).
- Five other functions already return early once closed: `cancel` (`:586`), `finish` (`:633`), `release` (`:645`), `resolvePromise` (`:688`) and `rejectPromise` (`:704`). Three others return `error.Closed`: `call` (`:382`), `setDeadline` (`:603`) and `promiseExport` (`checkOpen`, `:905-906`).
- The impact is small. capnp-swift drops a late answer after `transportIsClosed` is set (`handleTransportClosed`, `RPCConnection.swift:1017-1019`). The exposure is the window between CLOSE_REQUESTED (`:736-738`, `transport.cancel()`) and `handleTransportClosed`. In that window capnp-swift can start handler Tasks for calls on a dead connection.

**Work**

1. Red-first tests in `src/native/conn_test.zig`:
   - A remote Abort, then an answer.
   - A protocol failure (push a bad frame), then an answer.
2. At the top of the three functions, if closed:
   - Return `CAPNP_E_BAD_ID` for an unknown id.
   - Otherwise remove the pending answer and return `CAPNP_OK`.
   - OK, not CLOSED, matches the five no-op functions. capnp-swift's `answerFinished` treats OK and BAD_ID as consumed (`RPCConnection.swift:850-852`). Pin this choice in the tests.
3. Document "A no-op once the connection is closed" in `capnp_core.h` (comments only) and in `docs/native-abi.md`.
4. Add a CHANGELOG entry.

**Done when**

- Both tests fail on `bdc656f` (1 OUT_FRAME and 1 INBOUND_CALL after CLOSE_REQUESTED). They pass after the fix (0 and 0). With the check ablated, they go red again.
- Held questions are still swept as DISCONNECTED at `transportClosed`. The existing test stays green.
- `test-serialization` (it runs `conn_test.zig` and `abi_header_test.zig`) and `test-native-abi` pass. The header diff is comments only.
- It lands on main right after P0-1. If D1 is option A, it ships in v0.28.1.

## P1-3. Cross-implementation e2e: cancel then id reuse, and cancel with a pipelined call

**Why**

- `tests/e2e/schemas` has no RPC cancellation scenario. The `cancel` methods in `inventory.capnp:53` and `matchmaking.capnp:77` are application methods.
- capnp-swift asked for a cancel scenario (its `docs/handoffs/H13-answer-finished-native.md:164-166`). It records "no e2e scenario yet" (`:252`). capnp-zig's docs do not record this ask.
- The C++ client reuses the lowest free question id at once (`docs/native-abi.md:95-98`). It accepts `Return{canceled}` only for a question that it already finished (`rpc.c++:4061-4062`).
- Today, only in-process tests and fuzz prove the H13 behavior and the owed-id rules. Scenario 2 is the cross-implementation guard for P0-1.

**Work**

1. Add `tests/e2e/schemas/cancellation.capnp`: `slow()` answers only when it is cancelled, `fast()` returns a value, and `getSub()` returns a `Sub` with `bar()`. Copy the schema into `tests/e2e/go/schemas/`.
2. Zig e2e server (`tests/e2e/zig/main_server.zig`):
   - Install `setAnswerFinishedHandler` and answer with `sendReturnCanceled`, the way capnp-swift does.
   - Hold the answers to `slow()` and `getSub()`, the way `pipelined_params_deferred` holds `mint()` (`main_server.zig:773-781`, `:994`). Without the hold, getSub returns before the Finish arrives, and scenario 2 can never go red.
   - Give getSub's held answer a release trigger, for example the arrival of bar, or a tick. After P0-1, the held answer then returns and bar replays.
3. Scenario 1: start `slow()`, drop it (Finish), then call `fast()` N times. Each `fast()` gets its own value. The reused id never gets slow()'s late answer.
4. Scenario 2: call `getSub().bar()`, and drop getSub's promise after bar is sent. bar returns results.
5. Clients: C++ first, then Go, Rust and Python. Add a Zig client too, so the Zig-to-Zig lanes (e2e-self, and e2e-quic at `ci.yml:828`) can run the scenario. If a client never sends an early Finish, add a row to `tools/e2e_known_limits.zig` with its reason.
6. Scope the first landing to the `reference-client_to_zig-server` direction. Add a `scenario_exclusions` entry in `tests/e2e/test_matrix.json` for `zig-client_to_reference-server`, all four backends, with the reason "reference servers come next sprint".
7. Rebuild the Docker images (stale-image hazard). Add the scenario to `test_matrix.json`. Give capnp-swift the schema, so it can run it against its Swift server.

**Done when**

- Red before: on a branch cut from `bdc656f` (before P0-1), with the held-answer server, scenario 2 FAILs for the C++ client. Record the run id.
- Green after P0-1: scenario 2 PASSes in the "Zig e2e interop" job. Scenario 1 passes for C++.
- Each known-limit row and each exclusion has a reason. The XFAIL/XPASS runner stays strict.
- Branch CI is green. The commit message records the new pass, skip and XFAIL counts. Today they are TCP 56 / 12 / 4 and Unix 14 / 57 / 1 (CI 39541793776).
- Effort note: the last scenario of this size, `bdc5f12` (pass_back and pipelined_params), changed 16 files (+942 lines) across 4 reference languages, plus `94c55d7` for the Zig side. This item builds only the reference clients.

## P1-4. Docs lane: Markdown-only pushes skip the 30-job CI matrix

**Why**

- The owner's rule: push to main only after the previous main CI ended. CI cancels in-progress runs per ref (`ci.yml:26-28`). So main CI time is landing latency.
- The push trigger has no paths filter (`ci.yml:3-10`). Every push runs 30 jobs (run 39530075877). The last 4 main runs took 48.2, 44.7, 43.3 and 51.0 minutes (39541793776, 39530075877, 39522707768, 39511977861).
- A GitHub paths filter tests the whole push diff. By push range, about 15 of 88 main-push CI runs since 2026-10-03 were Markdown-only, about 580 wall-minutes (fact-check count). That is about 32 minutes a day.
- The larger gain: P2-1, P2-2 and many audit fixes are Markdown. A push that starts no CI run cannot cancel a running one. So Markdown pushes stop using the serial main queue.
- "Docs smoke and snippets" took 4 minutes in CI 39541793776.

**Work**

1. CI push trigger: `paths: ['**', '!**/*.md', 'README.md', 'docs/stability.md', 'docs/rpc-unix-sockets.md']`. These are the 3 Markdown files that build code reads: `build/build_impl.zig:421`, `tools/quic_test_evidence.zig:22`, and `tools/package_preflight.zig:48` (README.md is also in the `build.zig.zon` `.paths`).
2. Add `.github/workflows/docs.yml` for pushes that change `*.md`. It runs docs-smoke, test-docs-snippets and the ci_gates checks that read docs. Give it its own concurrency group (`docs-${{ github.ref }}`). It does not run fmt-check or check-api-experimental, because a Markdown-only change cannot change Zig formatting or the API snapshot.
3. Add a `ci_gates.ts` check, with fixtures and an ablation. Every `.md` path that CI-only Zig code reads (outside the docs lane's own steps) must be in CI's include list.
4. RELEASING.md: when the release commit is Markdown-only, run `gh workflow run CI --ref main`. The release-tag guard counts any CI run on HEAD and refuses `NO_RUN` (`Justfile:486`).
5. Do not use job-level `if:` skips inside CI. A skipped job reports success, and release-tag would accept a partial run.
6. Propose to the owner: a Markdown-only push may go to main while main CI runs, because it starts no CI run and cannot cancel one. Do not change the rule without the owner's yes.

**Done when**

- Before: a Markdown-only push runs 30 jobs in 43-51 minutes.
- After: the first Markdown-only push to main starts no CI run, and its Docs run ends in 8 minutes or less. The next code push still runs the full matrix.
- The new ci_gates check fails when a build-read `.md` file leaves the include list (ablation fixture), and passes otherwise.
- `just release-tag` still refuses a commit that has no CI run.
- If D1 is option A, this lands after the v0.28.1 tag, so the patch ceremony uses the old trigger.

## P1-5. Test census: unique root names, a wiring check, a root ratchet, per-root cost

**Why**

- The CI critical path is the ReleaseSafe ubuntu job. In job 119030978818, the step "Run the full suite under ReleaseSafe" took 39.6 minutes: 270/270 steps, 2679 of 2712 tests passed, 33 skipped. Debug "Run tests" took 3m17s (job 119030978932).
- In that tree, about 115 compile steps take about 3400-3500 s. About 117 test runs take 119 s. The exact compile sum depends on the parser: 3370 s (ci-cost scout) or 3521 s (fact-check).
- Roots with 10 or fewer tests use about half the compile time (52-54%) for 10-15% of the tests.
- No inventory exists. `addLibTest` (`build/helpers.zig:46-71`) sets no `.name`. 21 paths are registered 2-3 times. 36 direct `b.addTest` calls exist outside `helpers.zig`. So nobody can give cost per root, and a root merge cannot prove that it kept every test.
- All `src/` tests are collected (code-health scout binary-name census; `7a5327a` for the QUIC root). So the wiring check needs to cover only `tests/` and generated roots. The comment at `build/build_impl.zig:2048-2053` is stale.

**Work**

1. Give every `add*Test` a unique `.name`: the file stem, plus the family or the optimize mode where one path is registered more than once.
2. In `helpers.zig`, record for each root: the path, the family, the optimize mode, `-Dquic`, the top-level steps that reach it, and its number of named test declarations.
3. `zig build test-census` writes `docs/test-census.txt`. `zig build check-test-census` diffs it the way check-api does. It runs in the docs-smoke job, with no test compiles.
4. Wiring check: every `tests/**/*_test.zig` is a root, or a root imports it.
5. Add a `max_roots` ratchet header to `docs/test-census.txt`.
6. `ci_gates.ts census` parses the `--summary all` trees of "Test (ubuntu)" and "ReleaseSafe (ubuntu)" into per-root rows (tests, skips, compile s, run s).
   - Document how it parses, so that before and after numbers come from one parser.
   - Pin it with recorded fixtures.
   - Upload the rows as a CI artifact.
   - Per-lane floors: tests at or above the floor, skips at or below the ceiling.
7. Fix the stale comment at `build/build_impl.zig:2048-2053`.
8. Commit a ranking of roots by compile seconds per test. It is the merge order for next sprint's root merge.

**Done when**

- `docs/test-census.txt` is committed.
- `check-test-census` fails when a root is added without an update, and when a scratch orphan `*_test.zig` exists. Ablate both.
- Self-consistency on one green branch CI run: the per-root compile and run seconds sum to within 2% of that run's own Build Summary totals. The test and skip counts match that run's summary line.
- The census step adds less than 1 minute to the docs-smoke job.

## P1-6. CI inputs: pin `ubuntu-24.04`, raise the QUIC Windows cap, add a job-headroom gate; then pin uv, just and pycapnp

**Why**

- GitHub's notice says that ubuntu-latest moves to Ubuntu 26 "beginning October 19, 2026" (annotation on job 117485972364). This repo has not seen the move yet: `bdc656f`'s Test job 119030978932 ran image ubuntu-24.04 20261012.334.1.
- `ubuntu-latest` occurs 18 times in `ci.yml`, 5 times in `nightly.yml` and once in `release.yml`. `tests/rpc/transport/unix/unix_kernel_semantics_test.zig:9-11` is designed to go red on a kernel change.
- QUIC targeted transport on windows-latest has `timeout-minutes: 30`. Its note says "11m (windows)" (`ci.yml:337-339`).
  - Over the last 40 main runs: median 21.7 minutes, maximum 23.8 (37865210913).
  - A branch run reached 24.8 (39537367357).
  - One run was killed at 30m06s (run 37248467342, job 111571015980).
- Floating inputs:
  - `mise.toml:5-6` has `just = 'latest'` and `uv = 'latest'`.
  - The PEP 723 headers in the three `tests/interop/*.py` files name pycapnp with no version. `tests/e2e/python/requirements.txt` pins `pycapnp==2.2.2`.
  - CI 38911061944 went red because uv picked CPython 3.15, and pycapnp 2.2.4 has no wheel for it. The `requires-python = ">=3.10,<3.15"` cap (`tests/interop/*.py`, lines 3-7 of each header) fixed that.
  - The pycapnp interop tests skip when pycapnp cannot import (`shouldSkipPycapnpFailure`, `tests/serialization/interop_roundtrip_test.zig:161-171`). So lost coverage can show green.

**Work**

Commit A (lands after the v0.28.1 tag, or on day 2 if D1 is option B):

1. Replace every `ubuntu-latest` with `ubuntu-24.04` in the three workflows.
2. Follow the rename, because job names embed the matrix value:
   - `.actrc` (`-P ubuntu-24.04=...`, `--matrix os:ubuntu-24.04`).
   - The act recipes at `Justfile:518-535`.
   - `README.md:643`.
   - The `.github/known-flakes.txt` rows that name a job with its matrix value.
   - The `tools/ci_gates_test.ts` fixtures (`:516`, `:764`, `:769`, `:1052`).
3. Raise the quic-transport `timeout-minutes` to 40. Put today's numbers and run ids in the comment.
4. Add `ci_gates.ts job-headroom`:
   - Read the last 10 or more green main runs.
   - Print each job's peak duration and its run id.
   - Fail when a peak is over 75% of the job's cap.
   - Map old and new job names across the rename, so that the history does not start over.
   - Add fixtures and an ablation. Run the check in release-preflight.
5. Record the image pin next to the Zig pin in the toolchain notes. Say that the kernel inside 24.04 still moves, so `unix_kernel_semantics_test` can still go red.

Commit B (after P1-5 shows which lanes skip today; first to cut):

6. Pin uv and just to exact versions in `mise.toml`.
7. Pin pycapnp in the three PEP 723 headers to the version in `requirements.txt`, or bump both together.
8. With `CI=true`, make the pycapnp import-failure skip and the no-mise skip fail.

**Done when**

- Commit A: `grep -rn ubuntu-latest .github .actrc Justfile README.md` prints nothing. Branch CI is green. One Nightly dispatched on the branch is green.
- job-headroom: a fixture with a 30-minute cap and a 24.8-minute peak exits non-zero. The same fixture with a 40-minute cap exits 0. On live main data, it reports QUIC Windows at about 60% of 40.
- Commit B: a local ablation with a broken pycapnp import under `CI=true` fails the interop test. It does not skip.

## P2-1. H9-pending design note (design and review only, no code)

**Why**

- This is capnp-swift's top open ask. It lists H9 first under "Still deferred upstream" (its `docs/sprint-m9-guardrails-2026-10-18.md:36`).
- It is the only Level-1 conformance gap in the per-push cross-implementation matrix. All 5 XFAIL rows are `h9-pending-answer`: `tools/e2e_known_limits.zig:47-51`, server `zig`, clients cpp/go/python/rust on TCP and cpp on Unix.
- In CI 39541793776, "Zig e2e interop" reports TCP 72 cases (56 pass, 12 skip, 4 XFAIL) and Unix 14 pass, 57 skip, 1 XFAIL.
- The gap is in the Peer, so the Zig e2e server fails too. `src/native/cap_remap.zig:329` refuses with `PromisedCapUnsupported`. The Stable `ResolvedCap` union has 4 variants (`docs/api-snapshot.txt:842-846`). `queuePromisedCall` is at `src/rpc/peer/mod.zig:5980`.
- The last Peer-level hold design was archived (`archive/p1-3-tail-call-hold`) after several review rounds (the count of 5 is unverified). So this design needs a termination argument and a review before any code. It is too risky to implement in a release week.

**Work**

Write `docs/notes/h9-pending-answer-design.md`. Timebox it to 3 days of design and review. It covers:

- The Peer-level mechanism. Option 1: queue a call whose params name one of our own pending answers until that answer returns (what C++ does). Option 2: a local-answer client that parks through `queuePromisedCall` and replay. Neither may change the Stable `ResolvedCap`.
- A termination argument, modeled on `docs/notes/tail-call-hold-design.md`.
- The edge cases: a Finish of the named answer before replay, cycles (see `58612dc`), E-order against the remote's own pipelined calls, OOM in the parked state, and shutdown.
- An input from P0-1: the "nothing else cares about the result" rule should also count a params cap descriptor that names `receiverAnswer` of the finished answer.
- The native shape: a `CAPNP_CAP_OWED_ANSWER` kind behind a feature bit and an opt-in `capnp_conn_opts` field, with no layout change.

Give it to 2 adversarial reviewers. List the red tests: the 5 XFAIL rows become XPASS.

**Done when**

- The design note is committed (through the docs lane, after P1-4).
- Both reviews are recorded. Each finding is resolved in the note or listed as open.
- The note names the files, the red tests and an effort for next sprint's implementation.
- No product code changes.

## P2-2. Refresh both ledgers, write the quic-zig tag request, fix the evented-canary advice

**Why**

- `docs/downstreams.md:34` says capnp-swift pins "v0.27.0 for the core", checked 2026-10-20. capnp-swift 0.2.0's `core/build.zig.zon:16` is v0.28.0, and 0.2.0 shipped on 2026-10-21.
- `docs/upstream/README.md:30` says "Not written yet". Rows `:41`, `:43` and `:47` are stale against upstream 0.17.x (`c629ee4e08`, `f842de1845`, `6171a2be74`). Rows `:46` and `:48` are stale against the fork (`03263c1c44`).
- Two std workarounds have no ledger row: the Windows TCP_NODELAY no-op (`src/rpc/transport/tcp/runtime.zig:342-349`) and the raw setsockopt (`:350-360`).
- The Nightly canary notice (`nightly.yml:452`) tells the owner to "flip evented_available" if Evented compiles. But at Zig 0.17.0, `lib/std/Io/Dispatch.zig:1713-1714` are `@panic("TODO implement net_receive operation")` and `@panic("TODO implement net_send operation")`. A compile alone does not prove the net paths work.
- D2 needs the quic-zig handoff with either option. It has waited since 2026-10-16.

**Work**

1. Refetch the Zig upstream and fork refs. The local refs are 2 weeks old.
2. `docs/downstreams.md`:
   - Set the capnp-swift row to v0.28.0 (0.1.4, 0.2.0). Mark H13 as verified by capnp-swift 0.2.0.
   - Add a deferred row: "tail-call results with capabilities".
   - Add a row for the e2e cancel scenario that points to P1-3.
   - Start a v0.29.0 relay stub (`bdc656f`, P0-1, P1-1, P1-2).
   - Add short relay texts for capnp-deno (P0-1 and P1-1 affect its pin `fe6634d`; move the pin) and slcp-zig (no ABI change; move to v0.28.0 or v0.29.0).
3. `docs/upstream/README.md`:
   - Fill the "Where fixed" column with commits.
   - Add the setsockopt and TCP_NODELAY rows.
   - List exactly the handoffs still worth sending: netacceptwindows, netreadwindows-stray-cancel (it covers the write side too), the unix-address #3/#4 delta, and the quic-zig tag request.
4. Write `docs/upstream/handoff-quic-zig-tag-hardening.md`. Ask for a tag at `b0d7d78` or later, with CHANGELOG and DOWNSTREAM-NOTE entries. Name the pre-auth transport-parameter scan, but do not claim a measured DoS.
5. Reword the `nightly.yml:452` notice: a successful compile means "run a net_read behavior probe", not "flip evented_available". Propose the matching CLAUDE.md wording to the owner. Do not edit CLAUDE.md without the owner's yes.

**Done when**

- Every changed ledger row cites a commit or a file:line.
- docs-smoke is green.
- The owner has the list of 4 handoffs to send, and the reason why the others are not needed.
- The quic-zig handoff file exists.
- The Nightly notice no longer says to flip `evented_available` on a compile alone.

## P2-3. Soak lanes: quiet the debug logs and make a silent exit diagnosable

**Why**

- `tools/soak_rpc.zig` sets no `std_options` and no log level. The Debug soak lanes print millions of `std.log` debug lines (reliability scout):
  - Nightly 39577274345, Windows soak job 119145820535: 163 MB of log, 1,233,169 `debug(rpc` lines.
  - Ubuntu job 119145820471: 311 MB, 2,387,814 lines.
- Those lines bury every failure. The scout reports that less stderr once made the Windows Debug lane 3-5 times faster (unverified).
- The one unexplained failure since 2026-10-15 was a silent Windows 8-worker exit at +58 s (Nightly 39087827703, job 117558009545). It cannot be diagnosed next time. The step-log artifact (`nightly.yml:148-158`) is a copy of the same tee stream (`nightly.yml:104`).
- The `netsh` port widening only warns when it fails (`nightly.yml:55-64`). The failure has not come back in 11 Nightlies (scout count).

**Work**

1. Add `std_options` with log level `.info` to `tools/soak_rpc.zig`, or a `--log-level` flag with default `warn`. Keep the warn and error lines.
2. In `nightly.yml`, build soak-rpc in its own step. Run the binary directly under `set +e`, echo `PIPESTATUS`, and exit with the harness status.
3. Keep the contract that `ci_gates.ts annotate --log-dir "$RUNNER_TEMP"` reads (`nightly.yml:95`): the step ids `soak_8w` and `soak_8w_releasesafe`, the `$RUNNER_TEMP/<step id>.log` file, and the `soak: FAIL — <reason>` line. Known-flakes rows can name `step:<step id>`.
4. The harness writes a heartbeat every 5 s (elapsed time, sessions, dials_ok) to stderr and to a file that it opens itself. Upload that file on failure.
5. When the `netsh` widening fails, fail the step with a port-budget message.
6. Optional: WER LocalDumps for `soak-rpc.exe` on the Windows lane.

**Done when**

- On a Nightly dispatched on the branch, each soak job log is under 10 MB, with fewer than 10,000 `debug(rpc` lines (before: 163 MB and 311 MB).
- The session counts of both 8-worker lanes are recorded before and after. The Windows port budget (55,535) keeps at least 2x headroom over the new session count.
- Heartbeat lines appear in the log and in the uploaded file.
- An injected `SoakFailed` still turns the step red, and `annotate` still names the failure (ablation).
- It lands by day 3, so at least 2 scheduled Nightlies (about 25 minutes each, for example 39577274345) run on it before the ceremony. If it misses day 3, it moves after the v0.29.0 tag.

## P2-4. Fact anchors for the mechanical audit class, and claim counts at the cut

**Why**

- Every cut's audit found 19-35 wrong claims. By their own fix lists, about 30% of v0.27.0's fixes and about 10% of v0.28.0's were mechanical: copied constants, caps, versions, counts and dates. (Audit commits `d7a5735`: about 6 of 20 fixes; `9cb7bac`: about 2-3 of 24.)
- Neither audit counted the claims it checked. So the error rate per claim is unknown.
- `docs/native-abi.md:100` and `:195` copy the value 4096 by hand from `src/rpc/peer/state.zig:20` (`max_active_inbound_questions`).
- `active_docs` (`tools/docs_examples_smoke.zig:16-37`) leaves out `docs/supported-surface.md`, `docs/downstreams.md`, the upgrade guides and `docs/upstream/*`.
- `verify-release-hash` checks only `docs/build-integration.md` (`Justfile:507`).
- The larger class, behavior claims copied into 5-7 places, needs one source per contract. That is not in this sprint.

**Work**

1. Add three docs-smoke anchor kinds, each with a fixture test:
   - A code constant: `<!-- fact: src/rpc/peer/state.zig#max_active_inbound_questions -->`.
   - A CI cap: `.github/workflows/ci.yml#<job>.timeout-minutes`.
   - A `build.zig.zon` dependency tag.
2. Apply them where audits found drift: the 4096 in `native-abi.md`, the e2e-quic cap text, and the quic-zig version mentions.
3. Add the current upgrade guide, `docs/downstreams.md` and `docs/supported-surface.md` to `active_docs`. Budget time for the failures this shows today.
4. Make `verify-release-hash` check all three docs that record the hash, and grep for the placeholder.
5. At the cut, record a claim count per changed file in the audit brief.

**Done when**

- Each anchor kind fails on a stale value (ablation fixture) and passes on a correct one.
- docs-smoke is green on main. The commit lists the doc fixes that the wider `active_docs` needed.
- The v0.29.0 pre-tag audit reports its mechanical-class finding count and its claims-checked count, next to v0.28.0 (about 2-3 of 24) and v0.27.0 (about 6 of 20).

## P2-5. List the CI runs that went green only on a re-run, in release-preflight

**Why**

- `gh run list` shows only the last attempt of a run. So a red first attempt is invisible.
- Example: run 39063420132 is green on attempt 2. Its attempt 1 failed (`gh api repos/nullstyle/capnp-zig/actions/runs/39063420132/attempts/1` gives `failure`, on `d1cbb5e`).
- The scout counted 6 such runs since 2026-10-01 (unverified).
- `.github/known-flakes.txt` covers only Nightly jobs. `tools/ci_gates.ts` already reads every attempt for nightly-gate (`:12-17`).
- The timer-flake class is deferred until measured flake data exists (see "Not in this sprint"). This item gives that data.

**Work**

1. Add `ci_gates.ts reruns --since <date>`. It lists CI runs whose final attempt is green but an earlier attempt failed. For each, it prints the failed jobs and steps of the earlier attempts.
2. Add fixtures with a 2-attempt run.
3. Run it in release-preflight, report-only. Paste its output into the pre-tag audit brief.

**Done when**

- The fixture tests pass, and an ablated fixture fails.
- On live data, the check lists 39063420132 with its failed job.
- release-preflight prints the list and does not fail on it.

## Order of work

**Landing rule.**

- Each item gets its own worktree and its own draft branch. There are no PRs.
- Run full CI on the branch by hand: `gh workflow run CI --ref <branch>`.
- Push to main only after the previous main CI ended. CI cancels in-progress runs per ref (`ci.yml:26-28`). Merge one item at a time.
- In each new worktree, init the submodules and run `mise run bootstrap:capnp`. Edit through the worktree path, not the main checkout path.
- Do not run agents in the shared main checkout.
- Red-first evidence for P0-1, P1-1, P1-2 and P1-3 comes from branches cut from `bdc656f`.

**Main push order.** Each code push costs about 48 minutes of main CI.

1. P0-1
2. P1-2
3. (v0.28.1 cut, if D1 is option A)
4. P1-6 commit A
5. P1-4
6. P2-3
7. P1-1
8. P2-5
9. P1-3
10. P1-5
11. P2-4
12. P1-6 commit B
13. The v0.29.0 cut

That is about 11 code pushes plus the cuts: about 10-11 hours of serial main CI. Keep the queue busy. Start each branch CI early, so the next push is ready when main CI ends. After P1-4, the Markdown pushes (P2-1, P2-2) use the docs lane.

**Day 1**

- Ask the owner D1 and D2.
- Start these branches in parallel:
  - P0-1 (red, then fix).
  - P1-2.
  - P1-6 commit A, with one Nightly dispatched on the branch.
  - The P1-3 red branch from `bdc656f`: the held-answer server and the C++ client.
  - P2-2. Give the quic-zig handoff to the owner the same day.
  - The P2-1 design.
- Main: P0-1, then P1-2.

**Day 2**

- If D1 is option A: cut v0.28.1 from main (`bdc656f`, P0-1, P1-2). Hold main pushes during its ceremony. Agents keep working on branches: P1-1, P1-4, P2-3, P1-5, P1-3.
- If D1 is option B: land P1-6 commit A, P1-4 and P2-3.

**Day 3**

- After the v0.28.1 tag: land P1-6 commit A, P1-4 and P2-3. P2-3 must land today, or it moves after the v0.29.0 tag.
- P2-1 goes to two adversarial reviewers.
- Decision cut-off: without a D2 answer, v0.29.0 stays on v0.38.0.

**Day 4**

- Land P1-1, then P2-5. P1-3 branch work continues.
- If D2 is option A and quic-zig has tagged: start the quic-zig move on a branch.

**Day 5**

- Land P1-3, then P1-5.

**Day 6**

- Land P2-4, then P1-6 commit B (first to cut).
- Commit the P2-1 design note and the final P2-2 ledger rows through the docs lane.

**Days 7-8: the v0.29.0 ceremony**

1. Do the RELEASING.md sweep and the version bump.
2. Run the pre-tag audit: 3 auditors, 3 voters, a fixer and a reviewer. Record the finding count, the mechanical-class count and the claims-checked count (P2-4). Include the P2-5 re-run list.
3. Run release-preflight (it now includes job-headroom and reruns).
4. Push the audit fixes together with the cut where you can. A Markdown-only fix push gets no CI run, so run `gh workflow run CI --ref main` before you tag.
5. Wait for green CI. Freeze main. Start 3 Nightlies with `gh workflow run Nightly --ref main`.
6. When the owner says tag, run `just release-tag 0.29.0 "..."`. It runs `check-nightly-gate`.
7. Run `just verify-release-hash 0.29.0`.
8. Give the owner the relay texts: capnp-swift (pin bump, header snapshot, cancel on by default is safe again), capnp-deno (P0-1 and P1-1; move the pin) and slcp-zig.

## Not in this sprint

- **Implementing H9-pending.** P2-1 writes and reviews the design only. The code is next sprint's lead item.
- **Any test-root merge, including the serialization pilot.** It needs P1-5's census on main, which comes on day 5. It would then compete with the ceremony for the main queue. It also brings the dead-test and isolation risks that the census exists to manage. It is next sprint's first CI item.
- **One shared "Finish proves delivery" helper for all six Return senders.** Today only `sendReturnResults` (`peer_return_send.zig:140-160`) and `sendReturnExceptionNoDrain` (`:424-438`) have the trailing-Finish rule. `sendReturnTag`, `sendReturnTakeFromOtherQuestion`, `sendReturnAcceptFromThirdParty` (`:749-780`) and `sendPrebuiltReturnFrame` do not. No shipped transport reaches this gap today. The refactor changes the `resolving_answers` and finished-early state that earlier OOM fixes kept getting wrong. Do it next sprint, before the `mod.zig` handler moves.
- **Fail closed when a core-built Return is lost because its one allocation fails (COR-5).** Today this is best effort, per the last fix. It is a policy choice between liveness and availability. It is not owner decision 3, because this plan has room for 2 decisions. The old reason, an overlap with a live drain-OOM session, is stale: that work is `bdc656f`, on main. This choice touches the same Return senders as the helper above. Ask the owner with that item next sprint. The options then: close the transport at each catch site, or keep best effort and document it.
- **The owed-id reuse limit.** When the Peer's finished-early records are full, a reused owed id on a Bootstrap, or on a Call that the core answers itself, is not caught. This is a documented limit.
- **A downstream canary job** (capnp-swift core built with `zig build --fork=$GITHUB_WORKSPACE`, plus a swiftc import of `capnp_core.h`). The old blocker is gone: `gh repo view` shows capnp-swift, capnp-deno and slcp-zig are all public now. It is out of this sprint because the sprint is full. capnp-swift's own header snapshot gate catches this sprint's header changes at its pin bump (`docs/downstreams.md:58`). Schedule it next sprint, with the root merge.
- **A quic-zig move** (cluster B), unless D2 is option A and a tag exists by day 3.
- **A Zig 0.17.1 move, and flipping `evented_available`.** A 0.17.1 tag was not found (unverified). The Dispatch net paths are still TODO panics at 0.17.0.
- **Moving CI to Ubuntu 26.04.** This sprint pins 24.04. A move to 26.04 is a later item, with green runs.
- **An owned Windows AFD send and shutdown path (REL-5).** The abort risk is unverified. Send the existing handoff instead.
- **A shared timing helper or a fake-clock Io for the transport tests (REL-3, CH-11).** The flake class is quiet: one Linux case since 2026-10-10. Revisit it with P2-5's re-run data.
- **Reference servers for the cancellation scenario, and a native-ABI e2e server backend.** Next sprint.
- **Schema tooling and H12, the 1.0 charter, and Stable promotions** (rpc.events, Unix entry points, the native ABI v1 tier).
- **Large file splits:** `src/rpc/peer/mod.zig` handler moves, `conn_test.zig`, `struct_gen.zig`, `generator.zig`, `message.zig`, `stream_transport.zig`. This sprint changes the Return path. Do not move code in files that change for behavior.
- **Restoring `.zig-cache` in CI** (the gain is unverified), and the direction #7 protocol-depth lanes.
- **Any change to a downstream repo.** All coordination goes through relay text that the owner sends.
- **Any deletion of branches, worktrees or caches by an agent.**

**The rejected theme.** The strongest alternative was "Downstream conformance first":

- Implement H9-pending as a local answer promise in the Peer, plus a native `CAPNP_CAP_OWED_ANSWER` kind.
- Add a zig-native e2e server backend that drives the C ABI against the four reference clients.
- Defer all CI work.

It would close capnp-swift's top ask and the 5 XFAIL rows. I rejected it for these reasons:

1. It is an L-effort, hold-style Peer design. The last one went through several review rounds and was archived.
2. It is unlikely to pass review, land, and survive a release ceremony in one week.
3. P0-1 is a live defect in a released downstream default, and it comes first.
4. Landing latency would stay at about 45 minutes per push.

## Hygiene

The owner runs these steps. This sprint does not delete branches, worktrees or caches. Do each step only when no session uses the directory and no build runs.

- `/Users/nullstyle/conductor/workspaces/capnp-zig/hong-kong`: git says "not a git repository". Its branch `nullstyle/hong-kong` (`3a7e7c5`) is merged. Run `git worktree prune`.
- `.claude/worktrees/ecstatic-bhaskara-0aa300` (`5fabb36`): merged. Remove it.
- `.claude/worktrees/elated-panini-929a49` (`d95ca38`, branch `claude/happy-pare-a66c1b`): the branch is not an ancestor of main. The last plan says the same change is on main as `13221cd`. Check that, then remove it.
- `.claude/worktrees/infallible-mayer-ace634` (`bdc656f`, branch `sprint/v029-drain-oom-return`): merged and clean. Its work is `bdc656f`, on main. Its `.zig-cache` is 54 GB. Remove it.
- Branch `claude/infallible-mayer-ace634`: not merged. The native tail-call hold shipped in v0.27.0 by another route (`docs/downstreams.md`, "Tail calls" row). Archive-tag it before you delete it.
- `/Users/nullstyle/prj/zig/capnp-zig-zcode` (`86c9185`, `e2e-quic-modes`): merged. Remove it after you check with its owner.
- `codex/downstream-wasm-abi` (`da302d5`, worktree `/Users/nullstyle/prj/zig/capnp-zig-wasm-abi-feedback`): not merged. Read it, then keep it or archive it.
- Local branches: 119 in all. 101 are merged into main (`git branch --merged main`, which includes main). Delete the merged ones. Keep `archive/p1-3-tail-call-hold`, because P2-1 cites it.
- The main checkout's `.zig-cache` is 208 GB. Clear it when no build runs.

## Evidence

**CI runs**

- Main CI: 39541793776 (`bdc656f`), 39530075877 (`7843485`), 39522707768 (`73416c7`), 39511977861 (`58612dc`). All green, 43.3-51.0 minutes, 30 jobs.
- Nightly: 39577274345, 39522716854, 39522719636, 39522722588, 39489296792, 39400927246, 39231302956, 39115213373 (all green).
- Jobs: 119030978818 (ReleaseSafe ubuntu, 39.6-minute test step), 119030978932 (Debug ubuntu, ubuntu-24.04 image 20261012.334.1), 117485972364 (Ubuntu 26 notice), 119145820535 and 119145820471 (soak log sizes), 117558009545 (silent Windows exit).
- QUIC Windows: 37865210913 (23.8 minutes, main max), 39537367357 (24.8 minutes, branch), 37248467342 / job 111571015980 (killed at 30 minutes).
- Others: 38911061944 (uv picked CPython 3.15), 39063420132 (green on attempt 2; attempt 1 failed), 39087827703 (silent soak exit).

**Commits**

- capnp-zig: `bdc656f`, `7843485`, `73416c7`, `9cb7bac`, `2f842f4`, `d7a5735`, `58612dc`, `386d20f`, `7a5327a`, `bdc5f12`, `94c55d7`, `fe6634d`, `13221cd`
- capnp-swift: tag 0.2.0 (contains `f1f669b`); local main `fdecf9f`
- capnp-deno: `640d8a4`
- quic-zig: `021591e` and `b0d7d78` (in no tag); latest tag v0.42.2
- Zig upstream and fork (from the downstream scout): `c629ee4e08`, `f842de1845`, `6171a2be74`, `03263c1c44`

**Files read on `bdc656f`**

- `src/rpc/peer/mod.zig`, `src/rpc/peer/return/peer_return_send.zig`, `src/rpc/peer/state.zig`
- `src/rpc/integration/host_peer.zig`, `src/wasm/capnp_host_abi.zig`
- `src/native/conn.zig`, `src/native/include/capnp_core.h`, `src/lib_core.zig`
- `src/rpc/capnp/rpc.capnp`, `src/rpc/transport/tcp/runtime.zig`
- `build/native.zig`, `build/build_impl.zig`, `build/helpers.zig`
- `.github/workflows/ci.yml`, `.github/workflows/nightly.yml`, `.actrc`, `Justfile`, `README.md`, `mise.toml`, `SECURITY.md`
- `tests/e2e/test_matrix.json`, `tools/e2e_known_limits.zig`, `tests/interop/*.py`, `tests/native/fuzz_abi.zig`
- `docs/downstreams.md`, `docs/sprint-plan-2026-10-15.md`

**Other repos**

- capnp-swift: `Sources/CapnpRPC/RPCConnection.swift` (`:57`, `:169-170`, `:736-738`, `:847-857`, `:1017-1019`)
- Zig 0.17.0: `lib/std/Io/Dispatch.zig:1713-1714`
- Vendored C++ reference: `rpc.c++` and `capability.c++`, at the lines cited in P0-1 and P1-1 (from the fact-check)

**Not re-run by the planner:** the scouts' scratch tests for P0-1, P1-1 and P1-2, the md-only push count (fact-check count), and the soak log line counts (reliability scout).
