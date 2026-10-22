# Tail-call hold: design and termination proof

Status: design for the capnp-swift ask
`~/prj/zig/capnp-swift/docs/handoffs/tail-call-returns-for-native.md`
(sprint item P1-3, 2026-10-17). It replaces the Peer-level hold on branch
`archive/p1-3-tail-call-hold`.

## The case

The remote answers our question X with `Return(X, takeFromOtherQuestion =
Y)`. Y is a call that the remote sent to us with `sendResultsTo =
yourself`. So X's results are Y's results. When Y already has an answer,
the Peer delivers it to X at once (`loopback_result_stash`, cap-free only).
When Y is still pending, the Peer gives X the bare tag, and the native shim
turns that into one RETURN EXCEPTION, "capnp-swift core: unsupported Return
variant" (`src/native/conn.zig`, `fillReturn`).

The C++ reference does this in `resolve_disembargo`: it forwards a parked
`getNumber` back to our export as `Call(Y, yourself)`, then sends
`Return(X, take = Y)`. A native host answers Y later, so Y is pending.

## Why the Peer-level hold failed

The archived design held X inside the Peer until Y ended. A Peer call can be
in many states: dispatched to a handler, parked on a promise export, queued
on a promised answer, queued behind a streaming call, embargoed, forwarded
back to the remote, relayed to another connection. Some of these states
drop the call without a Return, and some wait for the remote's Finish of Y,
which a compliant remote sends only after our Finish of X (rpc.capnp,
`Call.sendResultsTo.yourself`). Each review round found one more state where
Y never ends while X waits for it. The set of states is open, so no list of
guards closes it.

## The design: hold in the native shim, only on a call the host owes

The hold moves out of the Peer into `native.conn.Conn`. The Peer does not
change.

**Hold rule.** In `onQuestionReturn`, the shim holds X only when all of
these are true:

1. `ret.tag == .takeFromOtherQuestion`, and it names Y.
2. Y is in `pending_answers` with the `yourself` flag: the Peer handed Y to
   the host as an INBOUND_CALL, the host has not answered it, and Y had
   `sendResultsTo = yourself`.
3. No other question is held on Y.
4. X is a `capnp_call` question (retained), not the bootstrap question,
   and not a loopback question (its redirect would name an id in our own
   question space).
5. The hold table entry can be stored (`getOrPut`; out of memory means no
   hold).

Otherwise X gets one RETURN EXCEPTION at once, as today. This includes every
Y that the Peer still holds (parked, queued, embargoed, forwarded): the
shim does not wait on Peer-internal state.

**Hold state.** X's `QuestionCtx` stays linked in `live_questions` and in
`questions`, so the host still sees X as an open question. Its RETURN node,
reserved when X was sent, is not queued. `held_tail_calls` maps Y to X's
context, and the context records Y.

**One way out.** Every end of a hold starts with `takeHold`. It removes
the table entry first (`fetchRemove`; no entry means the hold already
ended, and nothing happens) and unlinks X. Then `endHeld` or
`endHeldWithResults` fills X's reserved node, and `queueHeldReturn` queues
it and frees the context. The close sweep takes the hold the same way. So
X gets at most one RETURN from a hold.

## Termination proof

The Peer delivered X's Return to the shim, so the Peer never calls back for
X again (`on_return` or `deinit_ctx`, never both). From then on only the
shim owns X. A held X waits on exactly one thing: the host's answer to Y.

**Every way Y can end or leave the host's hands** settles the hold:

| Event | Where | X gets |
|---|---|---|
| The host answers Y with results, and the Peer accepts them | `returnResults` | cap-free: a copy of Y's results, with NONE caps made null as the Peer encodes them (`cap_remap.copyCapFreeHostContent`); results that point at a capability: one EXCEPTION (documented limit); copy out of memory: one EXCEPTION with a static reason |
| The host answers Y with an exception, and the Peer accepts it | `returnException` | one EXCEPTION with Y's type and reason (static reason if the copy fails) |
| A new call with Y's id reaches `onExportCall` (the remote reused the id after its Finish of Y; possible when the Peer keeps no record of that Finish) | `onExportCall`, then `failOnOwedIdReuse` | nothing then: the connection fails, and X gets one RETURN DISCONNECTED at transport close (`sweepOpenQuestions`) |

`pending_answers` loses Y only in `returnResults` and `returnException`,
after the Peer accepted the answer, and at `deinit`. A refused answer keeps
Y pending, so the host can answer again. The Peer never answers a call it
handed to the host. The remote's Finish of Y does not end the host's
obligation (the host still answers Y), so it does not end the hold: X still
gets Y's answer, as in the C++ reference, where Finish(Y) after the take
does not touch X.

**Every way X can end from the host's side** settles the hold:

| Event | Where | X gets |
|---|---|---|
| `capnp_cancel(X)` | `cancel` | Finish(X) (`finishHeld`), then one RETURN CANCELED |
| X's deadline passes | `tick` | a `.timeout` event, Finish(X), then one RETURN EXCEPTION (overloaded, `deadline_reason`) |
| The transport closes | `transportClosed` (`sweepOpenQuestions`) | one RETURN DISCONNECTED |
| Graceful shutdown passes `shutdown_drain_timeout_ms` | `tick` | Finish(X), then one RETURN DISCONNECTED (`shutdown_reason`) |
| `capnp_conn_free` | `deinit` | no effect (no effect is produced during free); the node and context are freed |

`finishHeld` ends X the way the Peer's own cancel ends a question: X ends
even when its Finish cannot be sent (no memory). Then the Peer's retained
record is retired, so `capnp_finish(X)` refuses the id as after any cancel,
and transport teardown reconciles the remote's state. So none of these
exits waits for memory.

`capnp_finish(X)` on a held X returns `error.Busy` (CAPNP_E_BUSY, "still in
flight"), so the host cannot end the Peer's record under the hold.

**Graceful shutdown.** The Peer completes its drain when its own questions
are done, and a held X is not one of them. So the shim defers the Peer's
close request (`bindingClose`) while a hold exists and `shutdown` was
called. The last settle, or the drain bound on a tick, queues
CLOSE_REQUESTED. A protocol failure or a remote Abort still closes at once
(`requestClose` directly). This keeps the header's contract: CLOSE_REQUESTED
comes when no open question is left, or when the drain timeout passes.

**No cycle.** A cycle needs Y's answer to wait for something that waits for
X. Y is in the host's hands, so its answer waits only for host code. The
remote's Finish of Y is not needed: the host answers Y without it. Our Finish
of X is not needed by Y either. So the only wait is "the host answers Y",
which is the same wait as for any call the host serves, and cancel, the
deadline and close bound it as they bound any question.

## Bounded work and memory

- **Per frame.** The hold check is three hash lookups. Each settle is one
  `fetchRemove`, one unlink and one queue push. No walk over a chain or a
  queue.
- **Per tick.** The deadline and drain checks walk `live_questions` only
  while a hold exists. That list holds only the host's own questions,
  bounded by `max_retained_questions` (default 1024). The remote cannot
  grow it.
- **Memory.** One table entry per held X. A hold needs one of our open
  questions, and each question holds at most once, so the table is bounded
  by the host's own questions, not by anything the remote sends. A second
  redirect that names a held Y is refused (rule 3). The RETURN node already
  exists. Settling needs no heap except for the results or reason copy, and
  that copy has a static fallback.
- **Hostile remote.** It can make X wait only as long as it could by never
  answering X at all. It cannot make the shim hold a call it does not owe
  (rule 2), and it cannot make one call hold two questions (rule 3).

## Comparison with the C++ reference

`vendor/ext/capnproto/c++/src/capnp/rpc.c++`:

- `handleCall` (about line 3843) stores a `yourself` call's completion
  promise in `answer.task` as `Answer::Redirected`, whatever state the call
  is in.
- The take handler (about line 4073) moves that promise into the question
  (`questionRef.fulfill`), sets `answer.task = Finished` and sends
  `Return(Y, resultsSentElsewhere)` at once (`sendRedirectReturn`, about
  line 3300).
- So in C++ a later Finish(Y) does not touch X, and cancelling X destroys
  the promise, which cancels Y's work. A Return for a question that was
  cancelled before the take (about line 4100) throws Y's promise away.
- Termination in C++ comes from KJ promise ownership: a promise is
  fulfilled, rejected, or destroyed, and destroying one cancels what it
  waits on. The capnp-zig Peer has no such ownership graph, so the archived
  Peer hold had to list every call state. The native shim has a closed list:
  the host owes Y, and the host can only answer it with results or an
  exception.

Differences from C++, all deliberate:

1. C++ follows a tail call in any state. The shim follows one only when the
   host owes it. Others keep today's RETURN EXCEPTION.
2. C++ sends `Return(Y, resultsSentElsewhere)` at the take. We send it when
   the host answers Y, as today. The remote's question Y stays open until
   then.
3. C++ cancels Y's work when X is cancelled. The 0.27.0 C ABI had no
   effect that told the host to stop a call. Since 0.28.0, with the
   `answer_finished` opt-in, the host gets ANSWER_FINISHED for Y when the
   remote finished Y and the hold ends without Y's answer. While a call
   that the remote has not finished is pipelined on Y, it still needs Y's
   results, so ANSWER_FINISHED waits for the Finish of the last such call
   (v0.29.0), and comes only if the remote sends it. The host's answer is
   still sent, and only X ends.
4. C++ delivers results with capabilities. The shim gives one RETURN
   EXCEPTION for them (the Peer cannot re-deliver those results locally
   either).

## Limits that stay

- A redirect that names a call the Peer still holds (parked, queued,
  embargoed, forwarded) gets one RETURN EXCEPTION at once, as on main.
- Results with capabilities get one RETURN EXCEPTION.
- Only the native shim follows pending tail calls. Peer users, HostPeer and
  the WASM host ABI still get the bare `takeFromOtherQuestion` tag for a
  pending call.

## Changes

- No Peer change. No C ABI change: the header text and prototypes stay as
  they are, and every host-visible rule above is one the header already
  states for an open question.
- `src/native/conn.zig`: the hold table, the hold rule, `takeHold` and
  `endHeld`, the shim-side deadline, the shutdown deferral and the reuse
  check.
- `src/native/cap_remap.zig`: `copyCapFreeHostContent`, the host-results
  copy (the remap walker with a null-only mode).
- `src/native/conn_test.zig`: red/green tests for each table row above, plus
  guards for the stash path and the refused-hold cases.
- `docs/native-abi.md` and `CHANGELOG.md`.

No hold timeout and no API change are needed.

## Review (2026-10-17)

One adversarial review of the first implementation found two must-fix
defects. Both are fixed, each with a test that the ablation turns red:

1. Results with only NONE capabilities, or with a caps entry nothing points
   at, got `tail_call_caps_reason`, while the same answer before the
   redirect got RESULTS. The copy now nulls NONE pointers and fails only
   on a pointer to a real capability.
2. With no memory, a cancel failed and a deadline never fired, because both
   waited for Finish(X) to be sent. Both now end X as the Peer's own cancel
   does (`finishHeld`).

The review found no way for a held question to get zero or two RETURNs,
no leak and no use after free, also in its allocation-failure sweeps. It
found no Peer path that answers or drops a call it handed to the host.

Pre-existing issues it found, not changed here (follow-ups):

- A `capnp_return_exception` that fails ends the call's `yourself` mark, so
  the host's retry puts an exception Return on the wire for a tail call
  (`Peer.sendReturnExceptionNoDrain`). A C++ remote whose question is still
  live drops the connection on that.
- After the remote's Finish of a `yourself` call, the host's answer goes out
  as a results Return, not `resultsSentElsewhere` (Finish clears the mark).
- `pending_answers` has no generation: after the remote reuses an id, the
  host's late answer for the old call answers the new one. ANSWER_FINISHED
  (ABI v2) would let the host know. Closed for 0.28.0 without a
  generation: a new call that reuses an id the host still owes closes the
  connection, and ANSWER_FINISHED is an opt-in in ABI v1 ("Answer ids" and
  "Answer finished" in docs/native-abi.md). When the Peer keeps no record
  of the old call's Finish, a Bootstrap that reuses such an id, or a Call
  that the core answers itself (it never reaches a host export), is still
  not caught.
- `capnp_finish` on a question that is still in flight returned
  `CAPNP_E_INVAL`; the header says `CAPNP_E_BUSY`. Fixed for 0.28.0: it
  returns `CAPNP_E_BUSY`.
