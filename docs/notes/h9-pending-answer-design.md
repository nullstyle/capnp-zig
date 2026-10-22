# H9-pending: a local promise for a pipelined param that names a pending answer

Status: design, revised after two adversarial reviews (sprint item P2-1,
plan `docs/sprint-plan-2026-10-21.md`, first written and revised on
2026-10-21 on `d6f19f1`), then checked against main `6acdca02` on
2026-10-22 (see "Checked against main"). No code changes. The
implementation is next sprint's lead item. Line references are to
`6acdca02` unless a line says otherwise. The reviews and their probes ran
on `d6f19f1`. "Review" at the end lists each finding and where this note
answers it.

## Checked against main (2026-10-22)

Main moved from `d6f19f1` to `6acdca02`: the P0-1 roll-up (`a51eabe8`) and
the P1-3 cancel scenarios (`6acdca02`) landed, and many line numbers
moved. Every line reference was re-derived, and each claim about current
behavior was re-read against the code. What changed under this design:

- **P0-1, the part on main.** The hook gate, the `sendReturnCanceled`
  refusal and a delayed hook now exist for calls pipelined on A
  (`hasCallsPipelinedOnAnswer`, `reportFinishedAnswerOnceUnwanted`).
  "The caller's Finish of A" describes this code.
- **P0-1, the part not on main.** Two commits on
  `sprint/v029-p0-1-answer-finished-pipelined` keep a queued call at its own
  Finish when calls are pipelined on it. They are not on main:
  `handleFinish` still cancels such a queued call, unless its Finish sets
  `requireEarlyCancellationWorkaround` (`docs/supported-surface.md`, "A
  Finish of a queued call fails the calls pipelined on it"). This design does
  not need that part. "Calls parked on P" says where it shows.
- **The gap this design closes is now a documented known limit.** Main says
  "The answer-finished hook does not wait for a call whose params name the
  answer" (`docs/supported-surface.md`), pinned by a "known limit" test in
  `tests/rpc/peer/rpc_reflected_direct_handler_test.zig`. For a Peer with
  answer promises on, the fix flips both (see "Files"). The limit stays for
  a native host, `HostPeer` and the WASM host ABI.
- **A Finish never loses its record** (`05cf466a`): the finished-early record
  is reserved when a call is admitted. Probe r2 no longer reproduces, and
  "Ids stay in use" changed.
- **A Return that the Peer settles itself and cannot send ends the
  connection** (`9ffc648e`, `bd494c3c`): see "Calls parked on P".
- **A call parked on a destroyed promise export now gets a Return**
  (`0201e8c4`): see "Lifetime of P".
- **A tail-called answer fails its queued calls** (`ecbee29e`): see "Option
  2a".
- **`forwarded_questions` changed its key** (`7d44e74e`). Rule 7 is
  corrected: `isForwardedAnswer` is a scan, and it was one on `d6f19f1` too.
- **Native:** ANSWER_FINISHED is withheld while a call the remote has not
  finished is pipelined on the answer (see "The native shape").
- **e2e:** P1-3 added the `cancel_id_reuse` and `cancel_pipelined` scenarios,
  so the absolute counts in "Acceptance" changed. It now states deltas.
- **Found in the re-read:** the `active_inbound_questions` value is a `bool`
  with a meaning of its own (rules 6 and 9). The `hook_withheld` flag must be
  set whenever the held-P term stops the hook, not only when it stops it
  alone ("The caller's Finish of A", T-e5). The export-release functions are
  now scope-guard entries ("Answer scope"). The two new in-use terms are
  redundant today ("Ids stay in use"). And the forward re-encoding is now
  confirmed by reading the encoder ("Found while reading").

## Summary

The remote calls `mint()` on us (answer A), then calls `check(token)` (call
B) with `token` = `receiverAnswer{A, [getPointerField 0]}`. Our handler has
not answered A yet. Today the Peer leaves that param `.promised`, so the
generated `resolveToken` fails with `error.PromiseUnresolved` and the native
shim refuses B with `PromisedCapUnsupported`.

The design:

1. When B **arrives**, the Peer gives each such param a promise export P,
   if a local handler owes A. One P serves each path (A, T), as C++ keeps
   one queued client per path. The Peer rewrites B's cap table entry from
   `.promised{A, T}` to `.exported{P}`. B dispatches at once, as today. The
   generated `resolveToken` gives the handler an ordinary `.exported`
   Client for P.
2. Calls on P park on P, in the promise-export queue that capnp-zig already
   has (`pending_export_promises`). They replay in order when P resolves.
3. When A's Return is delivered, the Peer resolves P from it: to the
   capability at path T in A's results, or to an exception. It uses the
   existing promise-export resolution and replay. The remote's own calls
   pipelined on A run first.
4. A's id and B's id stay in use while the Peer keeps a record for them, so
   no other question can take either id first.
5. The caller's Finish of A does not cancel A while anything the Peer can
   see still holds P. When the last such holder goes, the Peer tells the
   host (the answer-finished hook), as it would have at the Finish.
6. Native hosts opt in to a new cap kind, `CAPNP_CAP_OWED_ANSWER`. The host
   gets `{OWED_ANSWER, id = A, ops = T}`, binds it to its own record of A
   when it drains B's effect, and resolves it itself, because the host
   answers A.

No Stable API changes. `ResolvedCap` keeps its 4 variants, and
`InboundCapTable` keeps its fields. Generated code does not change.

**The held state and its ends.** The held state is a *binding*: a promise
export P tied to one answer A by the lease L1. A binding ends exactly once,
at the first of these events. Each one first removes the binding from A's
record, and a binding that is not in a record is never settled.

| A binding ends when | P gets |
|---|---|
| A's Return is delivered (any tag) | the capability at T in A's results, or an exception |
| `Peer.shutdown` starts | an exception: "peer shutting down" |
| The transport closes | an exception: "disconnected" (local only) |
| `Peer.deinit` | nothing (freed, no effects) |

A new Call cannot reuse A's id while the binding lives: the id is in use
(see "Ids stay in use"). The proof is in "Termination proof". The Peer
itself waits on nothing for a binding, and it never stops the handler from
answering A. Only application code waits, by calling P, and each such call
is an ordinary question of ours with its own cancel, deadline and close
exits.

## The case in detail

- `InboundCapTable.init` records a `receiverAnswer` descriptor as
  `.promised{question_id, transform}` (`src/rpc/caps/inbound.zig:230-233`).
  In an inbound table, that id names one of **our** answers: the remote's
  question.
- Just before B dispatches, `resolvePromisedParamCaps`
  (`src/rpc/peer/call/peer_call_inbound.zig:270`) replaces the entry when A
  has returned: `.exported` for an export of ours, `.none` for null. If A
  failed, `prepareParamCapsForDispatch` (`:315`) fails B with a copy of A's
  exception. If A is still pending, the entry stays `.promised`.
- The generated `resolveX` arm for `.promised` calls `peer.resolvePromisedAnswer`
  (`src/capnpc-zig/struct_gen.zig:4566`), which fails with
  `error.PromiseUnresolved` for a pending answer
  (`src/rpc/peer/peer_export_release.zig:653`).
- The native shim refuses the whole call (`src/native/cap_remap.zig:329`).
- `tests/e2e/zig/main_server.zig` serves the `pipelined_params_deferred`
  schema (the comment at `:709-733`): it holds the `mint()` and `mintFail()`
  answers (`HeldAnswer`, `:749-768`) and answers them only at the end of the
  next `check()` (`:1006`), or on a tick after 500 ms
  (`held_answer_idle_ms`, `:773`; the tick pass, `:930-949`). The five
  `h9-pending-answer` rows in `tools/e2e_known_limits.zig:47-51` record the
  two `check()` lines that fail (C++, Go, Python and Rust clients over TCP,
  and C++ over a Unix socket).

## What the C++ reference does

Read in `vendor/ext/capnproto/c++/src/capnp` (submodule at `f8498184`).

- `handleCall` builds the cap table **before** it creates B's own answer:
  `receiveCaps` at `rpc.c++:3859`, `answers.create(answerId)` at `:3876`.
  `answers.create` asserts that the id is not in use (`:3876-3877`).
- `receiveCap` for `RECEIVER_ANSWER` (`rpc.c++:2222-2237`) looks up answer
  A and returns `answer.pipeline->getPipelinedCap(ops)` at once. A missing
  answer, or an answer that has lost its pipeline (its Finish dropped it),
  gives `newBrokenCap("invalid 'receiverAnswer'")`. B's own answer does not
  exist yet, so a param that names B itself is broken.
- So C++ never delays B. B's handler gets a capability that is a promise.
- For a local call, `LocalClient::call` (`capability.c++:606-679`) forks the
  call's promise. The pipeline gets its own branch, wrapped in a
  `QueuedPipeline`. `getPipelinedCap` returns a `QueuedClient` that holds a
  branch of that fork (`capability.c++:514-528`). Calls on it queue until
  the call returns (`capability.c++:393-512`).
- A call sent with `noPromisePipelining` gets a disabled pipeline
  (`capability.c++:636-650`). The pipeline also follows a tail call
  (`onTailCall`, `:672`).
- `handleFinish` (`rpc.c++:4131-4160`) drops `answer.pipeline` and the
  completion task. A `QueuedClient` obtained earlier keeps its own branch,
  so the local call keeps running while anyone holds the param. This is the
  "nothing else cares about the result" rule of `rpc.capnp:577-582` in
  practice.
- `QueuedPipeline` keeps one `QueuedClient` per path (`clientMap`,
  `capability.c++:349-352`, `:518`). Its comment says why: with one queue
  per request, "ordering of calls will be lost between the queues". So the
  remote's own calls pipelined on A.T, and every local holder of that path,
  share one queue.

**A correction to the plan.** The plan's P2-1 text calls option 1 ("queue a
call whose params name one of our own pending answers until that answer
returns") "what C++ does". The code above shows that C++ does option 2: it
dispatches B at once with a promise capability. This design follows C++.

## Options

**Option 1: delay B until A returns. Rejected.**

- E-order: later calls to B's target would overtake B, unless the Peer
  holds the target's whole queue (and the `Disembargo` reflections behind
  it). `docs/supported-surface.md:878-882` already gives this reason.
- Deadlock: if A's handler answers only after B runs, B never runs. The e2e
  server has exactly this shape. Only its 500 ms tick rescues it.
- A param that names B's own answer would wait for itself.
- C++ does not do it, so behavior would differ from the reference.

**Option 2a: a local-answer client that parks in `pending_promises[A]`
(the plan's option 2). Rejected in favor of 2b.**

- The queue is keyed by A's id. When a Return for A does not drain it, a
  later call that reuses the id replays the queue against the wrong answer.
  Review 5 of the archived hold (branch `archive/p1-3-tail-call-hold`, see
  "How this design answers the archived hold's reviews") showed this
  ("PROBE inv A2": a held question
  got another call's results). On `d6f19f1` three Return paths did not
  drain that queue: `sendReturnTakeFromOtherQuestion`,
  `sendReturnAcceptFromThirdParty` and a prebuilt frame that is not
  `results`. On `6acdca02` two still do not:
  `sendReturnAcceptFromThirdParty` (`peer_return_send.zig:803`) and a
  prebuilt frame that is not `results` (`sendPrebuiltReturnFrame`, `:269`;
  P1-1 changes that path and has not landed). `sendReturnTakeFromOtherQuestion`
  (`:782-801`) now fails each queued call (`ecbee29e`). Also, `handleFinish`
  fails the calls still queued on an answer that has returned and is now
  finished (`mod.zig:4979`). Those two changes narrow the hazard. They do not
  remove it, because the first two paths still leave a queue behind.
- E-order would need a drain that loops until the queue is empty, because
  `drainPendingPromises` (`src/rpc/promises/pending_calls.zig:195-245`)
  takes the whole list first.
- It needs a new Client origin, new generated code, and a way to pass the
  promise to the remote anyway.

**Option 2b: a promise export per path. Chosen.**

- P has its own export id, from a counter that does not reuse an id before
  it wraps (`allocLocalCapId`, `src/rpc/caps/lifecycle.zig:397-410`). The
  answer ids it depends on stay in use while it is bound (see "Ids stay in
  use").
- Parking, ordered replay, cancel of a parked loopback call
  (`cancelQueuedPendingQuestion` scans `pending_export_promises`,
  `src/rpc/peer/mod.zig:5202-5218`), resolution cycles (`58612dc`), chains
  of promise exports (H10) and `senderPromise` plus `Resolve` already exist
  and have tests.
- The generated `resolveX` already turns `.exported` into a local Client,
  and `setXClient` writes an unresolved promise export as `senderPromise`
  (`src/rpc/peer/mod.zig:217-232`). So no generated code changes.

## The design

### Records

The new module keeps three kinds of record. Each lookup is one hash
lookup.

- **Answer record**, one per A that has a binding, in
  `answer_promises_by_answer`. It holds the list of A's P records, the
  number of those P's that are *held* (see below), and the `hook_withheld`
  flag (see "The caller's Finish of A"). It exists from the first mint for A
  to the end of A's last binding.
- **P record**, one per path (A, T), found by `(A, ops)` in
  `answer_promises_by_path`. It holds P's export id, A, an owned copy of
  `ops`, whether L1 is still held, the number of leases, a `queued` flag for
  the settle work list, and, after step 1, the outcome. It exists from the
  mint to the removal of P.
- **Lease**, one per (B, P), linked into B's list in
  `answer_promises_by_lessee`. A lease is L2: B holds P until B's Return.

A P record leaves A's list when its binding ends, but it stays linked in
the lists of its leases. It is freed only when P is removed.

**Held.** P is *held* when anything the Peer can see still needs it: a
lease, a wire reference, a promise-held or handoff reference, or a call
parked on P (`pending_export_promises[P]` is not empty). L1 does not count.
Every place that changes one of these counts calls
`noteAnswerPromiseRefChanged(P)`. It recomputes P's held bit in O(1), and
updates the answer record's held count when the bit flips.

### Eligibility

The Peer gives an entry `.promised{A, T}` of call B a P only when all of
these are true. Each check is one hash lookup.

1. B came from the transport. (A loopback call cannot carry
   `receiverAnswer`: `InboundCapTable.initLoopback` refuses it.)
2. A is not B.
3. A is in `active_inbound_questions`: the remote has neither had A's
   Return nor finished A. A compliant remote names A only before its Finish
   of A (`rpc.capnp:577-578`).
4. A is not a loopback answer (`loopback_questions`). Loopback ids live in
   the same answer maps, so a hostile remote could otherwise name one.
5. A is not in `resolving_answers`, `resolved_answers` or `failed_answers`.
   An answered A keeps today's path (resolve just before B dispatches).
6. **A was handed to an application's `on_call` handler.** This is a new
   flag in the value of `active_inbound_questions[A]`, set at the two
   `on_call` sites (`src/rpc/peer/call/peer_call_orchestration.zig:84` and
   `:120`). It excludes an A that is queued, parked or forwarded back to the
   remote. The value is not a bit field today: the map is
   `AutoHashMap(u32, bool)` (`mod.zig:556`), and its `bool` means "this
   call's params granted import refs, so the answer owes `Release` frames".
   That is the input of `returnReleasesParamCaps` (`mod.zig:4429`). So the
   value becomes a small struct with that `bool` and the new flags (this
   rule and rule 9). The writers are `handleCall`
   (`peer_call_inbound.zig:173`) and the automatic third-party routes
   (`peer_third_party_routes.zig:609`, which writes `false`). `handleFinish`
   takes the value out (`mod.zig:4873`).
7. A is not an answer the Peer relays: `cross_peer_proxy_answers` does not
   contain A. Two `on_call` handlers belong to the Peer, not the
   application. The cross-peer proxy export (`peer_cross_peer_proxy.zig:97`)
   forwards the call and records A in `cross_peer_proxy_answers` (`:237`),
   so this rule excludes it. The persistence trampoline
   (`peer_persistence_hooks.zig:205`, `:246-271`) answers Save and Restore
   inside `on_call`, so rule 5 excludes those, and it hands every other
   method to the application's handler, which then owes A. The Peer's own
   `isForwardedAnswer` (`src/rpc/peer/mod.zig:5119`) is not the check here.
   It also covers calls the Peer forwards or parks
   (`forwarded_tail_questions`, the Level-3 vine relay links, and the values
   of `forwarded_questions`), and those never reach an application
   `on_call`, so rule 6 already excludes them. It is also a scan, not a
   hash lookup: it walks `forward_vine_relay_links`, and
   `forwardRelaysAnswer` (`:5136`) walks the values of
   `forwarded_questions` (keyed by the Peer's own question ids since
   `7d44e74e`, so the answer ids are the values; it was a value scan on
   `d6f19f1` too). A remote can grow those lists up to the question limits,
   so calling it for every `.promised` entry would break the O(1) claim in
   "Work per frame".
8. A's results come back as a normal Return: A is not in
   `send_results_to_yourself`, `send_results_to_third_party` or
   `automatic_third_party_routes`. (On `6acdca02` the `yourself` mark is set
   at dispatch, so rule 6 implies it is known by then.)
9. A's Call did not set `noPromisePipelining` (`rpc.capnp:422-425`). The
   Peer decodes the flag (`src/rpc/wire/protocol.zig:535`) but keeps no
   record of it today. It becomes one more flag in the value of
   `active_inbound_questions[A]` (see rule 6). C++ gives such an A a
   disabled pipeline.
10. The Peer is not shutting down (`is_shutting_down`). `handleCall` itself
    has no such check (`peer_call_inbound.zig:107-239`).
11. The mint stays within `Peer.answer_promise_limit` (new, Experimental,
    default 1024). The limit counts live P's and leases together: a new P
    costs two (P and its first lease), a new lease on an existing P costs
    one. A live P is one that has not been removed, whatever keeps it. A
    lease lasts until its B's Return, after B's own cap table is gone, so it
    needs its own bound. The cap table holds at most 10,000 entries in total
    (`max_table_size`, `src/rpc/caps/lifecycle.zig:13`), so the limit leaves
    room for the application's own exports.
12. The Peer has answer promises on (`Peer.answer_promises_enabled`, new,
    Experimental). The native `Conn`, `HostPeer` and the WASM host ABI turn
    it off: their hosts must never see an export that the core minted.

Otherwise the entry stays `.promised`, exactly as today.

Rules 6 and 7 are the lesson of the archived hold. That hold waited on a
call in any state, and each review round found one more state with no end.
Here, A's Return is in the hands of an application handler, as Y was in the
hands of the native host in the redesigned tail-call hold
(`docs/notes/tail-call-hold-design.md`, "Hold rule"). An application
handler can still wait on the remote before it answers A (the e2e
`onInvokeCap` does). That is an application wait, as in C++. The Peer adds
nothing to it.

### Minting, when B arrives

Right after `initCallInboundCaps` builds B's table
(`src/rpc/peer/call/peer_call_orchestration.zig:239-251`, used for both
kinds of target) and before routing (queue, park, forward or dispatch),
for each eligible entry:

1. Find the P record for (A, T). If there is none:
   - allocate P with the internal form of `addPromiseExport`. P is a promise
     export with no handler and no wire reference;
   - store a P record with an owned copy of T. The protocol bounds T to 64
     ops (`max_promised_answer_transform_ops`, `src/rpc/wire/protocol.zig:54`);
   - link it into A's answer record (created on the first mint for A), and
     give P the lease L1;
   - reserve P's slot in the settle work list, so that settling never
     allocates for bookkeeping.
2. If B has no lease on P yet, add one (L2) to B's list.
3. Rewrite B's entry to `.exported{P}`. `resolvePromisedParamCaps` skips it,
   because it handles only `.promised` entries.

Two params that name the same (A, T), in one B or in several, get the same
P. Calls through either param then share one queue, as in C++.

An error later in `handleCall` undoes the mint in an `errdefer`, as it
already undoes `active_inbound_questions`.

A B that waits in a queue keeps its table, so its later dispatch sees
`.exported{P}` (see "At B's dispatch").

### Ids stay in use

`inboundAnswerQuestionIdInUse` (`src/rpc/peer/mod.zig:6103-6118`) also
counts `answer_promises_by_answer` and `answer_promises_by_lessee`. So
while A has an answer record, or B holds a lease:

- a Call, Accept, Join, Provide or Bootstrap with that id returns
  `error.DuplicateQuestionId`. All five use this check
  (`peer_call_inbound.zig:111`, `peer_provide_inbound.zig:91`, `:189`,
  `:507`, `mod.zig:4785`);
- the loopback id allocator (`peer_question_alloc.zig:137-141`) and the
  third-party answer id allocator (`peer_provide_origination.zig:350-361`)
  skip the id.

A compliant remote cannot reuse either id anyway: it must first receive the
Return (`rpc.capnp:404-406`), and each record ends at that Return. C++
asserts the same at `answers.create` (`rpc.c++:3876-3877`), and the native shim
fails the connection for an id the host still owes (`conn.zig:1487-1490`).

On `d6f19f1` this rule mattered because the finished-early tombstones were
bounded. When they were full, a Finish of A left no record, and a hostile
remote could then reuse A's id while the handler still owed A (probe r2 in
"Review"). Main closed that hole (`05cf466a`): `handleCall` reserves the
Finish record when it admits a call (`reserveFinishedEarlyRecord`,
`peer_call_inbound.zig:38` and `:164`), and a finished call that still owes
its Return counts in `max_active_inbound_questions`
(`inboundCallsOwingReturn`, `:28`). So on `6acdca02` A's id is held by
`active_inbound_questions` before the caller's Finish, and by its
finished-early record after it, until A's Return. B's id is held the same way
until B's Return, which is also when its lease ends. The two new terms
therefore add no protection today. They keep the invariant "the ids of a live
binding are in use" inside this module, so it does not depend on the record
policy of another module. Open: drop them and keep an assertion instead (see
"Questions", item 9). Either way, P resolves only from its own A's Return.

### Calls on P

B's handler resolves the param with the generated `resolveX` and gets
`Client{origin = .exported, cap_id = P}`. A call on it takes the loopback
path (`sendCallToExport`), and `handleCall` parks it on P because P is an
unresolved promise export (`planImportedTarget` gives
`.queue_promise_export`). Nothing here is new.

### Settling, in two steps

**Step 1: detach.** It runs where a Return for an inbound answer is known
to be delivered:

- `Peer.sendReturnFrameWithLoopback` and `Peer.sendReturnFrameWithFds`
  (`src/rpc/peer/return/peer_return_send.zig:828-858`), after a successful
  send;
- the two "a Finish proves delivery" branches, in `sendReturnResults`
  (`:148-159`) and in `sendReturnExceptionNoDrain` (`:426-439`).

The Bootstrap Return goes through `sendFrameWithFds` directly
(`mod.zig:4830-4844`). That is on purpose: a Bootstrap answer is never A or
B, because a param cannot name it (rule 6) and it carries no params.

At each site, `noteAnswerReturned(id, frame)` does this:

1. A loopback Return returns at once. A loopback answer is never A (rule 4)
   or B (rule 1), and "Ids stay in use" keeps their ids apart. This is a
   second guard.
2. It takes A's answer record (`fetchRemove`). No record: nothing to do for
   A (one hash lookup per Return).
3. It decodes the Return frame once and computes each P's outcome:
   - `results`: the capability at `ops` (`resolvePromisedAnswerOps`). An
     export E or an import I is pinned at once: a promise-held reference on
     E, or one local reference on I. Step 2 adopts the pin. The pin must
     come before `commitOrRollbackResolvedAnswerAfterSend`, because a caller
     that finished A early with `releaseResultCaps` makes that commit
     release E's references at once. A null cap, a promise on one of our
     questions (`.promised`), or a transform that does not fit gives an
     exception with a fixed reason.
   - `exception`: A's reason (copied, at most 1 KiB) and type.
   - `canceled`: "call canceled".
   - any other tag: "the named call's results were sent elsewhere". For
     `takeFromOtherQuestion`, use the reason that main now gives the queued
     calls of such an answer (`tail_call_no_pipelining`,
     `peer_return_send.zig:32`), so a call parked on P and a call queued on A
     fail with the same text.
   - A decode that runs out of memory gives a static reason.
4. It takes B's lease list for the same id and drops each lease. (The same
   function serves both roles: every Return ends the answer's own bindings
   and the call's own leases.)
5. It appends each P whose `queued` flag is clear to the settle work list,
   using P's reserved slot, and sets the flag. One P can be appended from
   both roles in one flush window (the T-q shape); the flag makes the
   second append a no-op.

Step 1 runs no application code and sends nothing. It does not drop L1.

**Step 2: resolve and reclaim.** It runs at a flush point (see "Answer
scope"). For each P on the list, by id (never by a pointer held across a
callback):

1. Clear P's `queued` flag.
2. If P has an outcome and is not yet resolved, resolve it through the
   existing functions, changed in four ways:
   - no `Resolve` frame for a P that has no wire reference
     (`wire_ref_granted`, `src/rpc/peer/state.zig:131`). Then it also takes
     no wire reference on the target and attaches no fd. Today the Resolve
     path does both before the send (`noteExportRef`,
     `src/rpc/peer/peer_promise_exports.zig:86`; `attachExport`, `:74`);
   - adopt the pin from step 1 instead of taking a new reference;
   - a rejected P fails its parked calls with its own reason and type.
     Today `replayResolvedPromiseExport` uses "promise broken" for every
     rejected promise (`src/rpc/promises/pending_calls.zig:299`). The
     `mintFail()` check needs `mintFail()`'s exception;
   - a parked loopback call that fails gets its Return without the heap
     (see "Calls parked on P").

   A cycle (A's results name P, or a promise export that leads back to P)
   makes `resolvePromiseExportToExport` refuse with
   `error.PromiseResolutionCycle`. P is then rejected with "promise
   resolution cycle".
3. Drop L1. L1 drops only here, after P is resolved or rejected.
4. If P has no lease, no wire reference, no other local reference and no
   parked call, remove it. This also releases its promise-held reference on
   the target, and frees the P record.

A replayed call can send more Returns. Those run step 1 and add to the
list, and the flush loop continues until the list is empty. Each P is
resolved once and removed once, so the loop ends.

A Return path that skips step 1 would leave a binding behind after its
answer returned. Step 0a pins every exit with a test. The T-p sweeps also
check an invariant after each step: no binding names an answer whose Return
went out.

### Answer scope: where step 2 runs

Step 2, and the delayed hook (see "The caller's Finish of A"), run only at
the exit of the outermost *answer scope*. A scope is a depth counter on the
Peer (`answer_scope_depth`), entered and left by a guard.

- Every Peer function that application code, a transport or another Peer
  can call, and that can reach a step-1 site or a held-count change
  (directly, or through a callback it runs), holds the guard. On
  `6acdca02` this includes at least:
  - frame ingress: `handleFrame`, `handleLoopbackFrame`;
  - every public Return function: `sendReturnResults`,
    `sendPrebuiltReturnFrame`, `sendReturnException`,
    `sendReturnExceptionTyped`, `sendReturnExceptionNoDrain`,
    `sendReturnEmptyStruct` (`mod.zig:3997`, the streaming completion path,
    `stream_dispatch.zig:157`), `sendReturnResultsSentElsewhere`,
    `sendReturnCanceled`, `sendReturnProvidedTarget` (`:4051`),
    `sendReturnJoinResultPayload` (`:6031`) and `failQueuedPromisedCalls`.
    (`sendReturnTag`, `sendReturnTakeFromOtherQuestion` and
    `sendReturnAcceptFromThirdParty` are private, `:4033-4043`. Their
    public callers hold the guard.);
  - calls that can dispatch locally: the `sendCall*` family,
    `completeStreamingCall` (`:3136`) and `forwardResolvedCall`;
  - promise resolution: `resolvePromiseExportTo*` (`:2995-3037`, whose
    replay can fail a parked call and so send B's Return) and
    `replayResolvedPromiseExport`;
  - export release: `releaseExport`, `releasePromiseHeldCap` and
    `releaseHandoffHeldExport` (`peer_export_release.zig:497`, `:456`,
    `:289`), and the answer-held release behind them (`:477`). Since
    `0201e8c4` the destruction of a promise export answers each call parked
    on it (`failCallQueuedOnDestroyedExport`, `:49`), and that Return is a
    step-1 site. These entries became step-1 sites after `d6f19f1`;
  - cancel and time: `cancelQuestion`, `cancelQuestionTyped`,
    `checkDeadlines`;
  - lifecycle: `shutdown`, `notifyTransportClosed` and
    `finishTransportClosedNotification` (`:4449-4463`);
  - the cross-peer and Level-3 entries that another Peer calls.
- `deinit` frees every record first. A Return that its drain sends later
  finds no record, so the scope check does not apply while `in_deinit` is
  set.
- Step 0a builds the full list (a grep for the public functions that reach
  the step-1 sites), and adds one test per entry that a Return sent through
  it settles P before the entry returns.
- A missed guard is caught at run time, not by an assertion that a release
  build removes. `noteAnswerReturned` and `noteAnswerPromiseRefChanged`
  check `answer_scope_depth > 0`. Debug and ReleaseSafe builds panic
  ("answer scope missing"), so any test that reaches an unguarded entry
  fails. Other builds record the miss and flush at the next guarded entry or
  `checkDeadlines`. A missed guard then costs liveness, not memory safety.
- The flush keeps the depth raised while it loops, so no flush runs inside
  another. A Return sent from a replayed call adds to the list, and the
  same loop takes it.
- Each Peer has its own depth. When Peer X's entry calls Peer Y's entry, Y
  flushes at its own outermost exit. A callback from Y back into X finds
  X's depth raised and does not flush.

Why the outermost exit, and not the send wrapper: the wrapper runs before
the commit that replays the remote's calls on A
(`commitOrRollbackResolvedAnswerAfterSend`, `peer_return_send.zig:637`).
A flush there would run P's calls first ("E-order") and would run while the
Return function is half done (review question 3).

### At B's dispatch

Just before B dispatches (the place of `resolvePromisedParamCaps`), for
each `.exported{P}` entry that B leases, if P's binding already has an
outcome (step 1 ran):

- an export E: the entry becomes `.exported{E}`;
- null: the entry becomes `.none`;
- A's own exception: B fails with a copy of it, as
  `prepareParamCapsForDispatch` does today for a failed A;
- anything else: the entry stays `.exported{P}`.

This keeps the H9 result of main (v0.23.0) for a queued B whose A was pending at
B's arrival and returned before B dispatched (probe r5 in "Review": main
gives `.exported{E}`). B's lease on P stays until B's Return, so E stays
pinned while B runs, even when the caller finished A with
`releaseResultCaps`.

### Writing P

When the Peer writes an `.exported{P}` capability into a Call, a Return or
a Resolve, and P is a resolved answer promise whose target is our export E,
it writes E instead: `senderHosted(E)`, or `senderPromise(E)` if E is an
unresolved promise export, with E's own wire reference. This is the C++
rule (write the innermost resolved client). Without it, the encoder writes
a resolved P as `senderHosted(P)` (`exportedCapTag`,
`peer_cross_peer_proxy.zig:35-37`). The remote would then hold two imports
for one object, and every call would hop through P. An unresolved P is
written as `senderPromise(P)`, as today. A P resolved to an import keeps
`senderHosted(P)` in v1 (prerequisite 0b).

### Lifetime of P

P lives until it has no lease, no reference of any kind, no parked call,
and no L1.

- L1 drops in step 2, after P is resolved or rejected. So P outlives every
  call parked on it, and a P with parked calls is never removed. (The
  archived hold's review 3 found that `destroyExportEntry` dropped parked
  calls with no Return, `src/rpc/peer/peer_cap_lifecycle.zig:257-265`. Main
  now answers each such call with one exception Return, "promise released
  before it resolved" (`failCallQueuedOnDestroyedExport`,
  `peer_export_release.zig:49`, `0201e8c4`), and a Return that cannot be
  sent ends the connection. That is only the fallback. L1 plus the
  parked-call condition keep P alive, so the calls parked on P reach the
  target when A returns, and do not fail.)
- A lease (L2) ends at its B's Return.
- If B's handler passed P to the remote, the remote's wire references keep
  P, and the remote's last Release destroys it as for any export.
- A Client for P that the application kept after all of these ended gets
  `error.UnknownExport`. This is the existing contract of an `.exported`
  Client ("valid while the export lives", `src/rpc/peer/mod.zig:217-232`).
  An `.exported` Client has no reference count, so the Peer cannot see a
  kept Client. See owner decision O2.

### The caller's Finish of A

This is P0-1's rule, extended to params. Main (`6acdca02`) has the part of
P0-1 that covers calls pipelined on A (roll-up `a51eabe8`), and this design
adds one term to it.

**What main has.**

- `hasCallsPipelinedOnAnswer(A)` (`src/rpc/peer/mod.zig:5108`) is true while
  a call that the remote pipelined on A waits in `pending_promises[A]` and
  the remote has not finished that call. A queued call that the remote
  finished does not count, and neither does a call pipelined on it.
- Three places use it. The hook condition in `handleFinish` (`:4998`) has it
  next to `hostOwesFinishedReturn(A)` (`:5089`, unchanged).
  `Peer.sendReturnCanceled` (`:4025`) refuses with
  `error.AnswerHasPipelinedCalls`, after `AnswerNotFinished` and
  `AnswerNotOwed`. And `reportFinishedAnswerOnceUnwanted` (`:5036`) is the
  delayed hook: when the remote finishes a queued call, `handleFinish` finds
  the answer that the call was queued on (`answerQueuedOn`, `:5047`) before it
  cancels the call. After the Finish it calls the hook for that answer, if
  no waiting call is left and the host still owes the answer.
- A call pipelined on A after A's Finish fails at once
  (`lookupPipelineTargetFailure`, `peer_call_inbound.zig:472`). So no new
  waiting call can appear after the Finish.
- The delayed hook has no flag. It re-derives "the hook was held" from the
  finished-early record and the queue.

**What main does not have.** The two commits on
`sprint/v029-p0-1-answer-finished-pipelined` that keep a queued call at its
own Finish when calls are pipelined on it. `handleFinish` still cancels such
a queued call (`:4925`), unless its Finish sets
`requireEarlyCancellationWorkaround`, and the calls pipelined on it fail
(`docs/supported-surface.md`, "A Finish of a queued call fails the calls
pipelined on it"). This design does not need those commits. It meets that
limit in one place: a call parked on P that the remote finishes is cancelled
with the calls pipelined on it, as any queued call is.

**The gap this closes.** Main lists "The answer-finished hook does not wait
for a call whose params name the answer" as a known limit
(`docs/supported-surface.md`), pinned by a test in
`tests/rpc/peer/rpc_reflected_direct_handler_test.zig:2820`. P0-1 does not
cover params. A param is not a pipelined call, so `hasCallsPipelinedOnAnswer`
does not see it.

- **The gate.** One predicate, "someone still wants A's results", is true
  when `hasCallsPipelinedOnAnswer(A)` is true or A has a held P (the answer
  record's held count is not 0). The three places above use it. In
  particular, `reportFinishedAnswerOnceUnwanted` must test the held-P term
  too. Otherwise the Finish of the last waiting call could run the hook while
  P is held. While a held P names A, the hook is not called.
- **The flag.** The answer record sets `hook_withheld` whenever the held-P
  term stops the hook, also when the pipelined-call term stops it too. The
  pipelined-call term remembers nothing, and it fires on its own when the
  last waiting call is finished. If the flag were set only when the held-P
  term alone stops the hook (the wording of the first version, written
  before P0-1 landed), then a Finish with both terms true would leave no
  flag. The waiting call would finish while P is held and run nothing, P
  would go, and nothing would run the hook.
- **The refusal.** `sendReturnCanceled(A)` also refuses, with
  `error.AnswerHasPipelinedCalls`, while A has a held P. Its doc
  (`mod.zig:4008-4024`) and the WASM doc name "pipelined calls" and need one
  more clause ("or a param"). The native docs need none: the core mints no P.
  With no held P it works, and its Return rejects A's P's with "call
  canceled" through step 1.
- **The delayed hook.** When the held count of an answer record with
  `hook_withheld` drops to 0, the Peer clears the flag and queues a check for
  the flush. At the flush it runs `reportFinishedAnswerOnceUnwanted(A)`. That
  function checks the whole gate again (`hasCallsPipelinedOnAnswer`, the
  held-P term, `hostOwesFinishedReturn(A)` and the finished-early record). If
  the gate holds, it calls the hook once, with the Finish's
  `releaseResultCaps` (kept in the finished-early record). If a waiting call
  is still there, nothing runs now: its own Finish runs the hook later. If a
  P became held again in between (a kept Client parked a call), the check
  sets `hook_withheld` again. The host then cancels or answers A as usual.
  The Peer does not end the bindings itself: A's Return does, through step 1.
- **At most once.** Each trigger checks the whole gate, and the flag is
  cleared when its check is queued, so the held-P trigger fires at most once
  per answer. The other trigger fires only at the Finish of a waiting call,
  and none can appear after A's Finish. A's Return, shutdown and close remove
  the answer record.
- **No hold that never ends.** Without this hook, a host that answers A only
  when told the caller finished it would never answer A once a param named
  A: the binding waits for A's Return, A's Return waits for the hook, and
  the hook waits for the binding (review 1, finding 1; probe r1). Now the
  hook waits only for holders that end without A's Return: B's Return, the
  cancel of a parked call, the remote's Release of P, or the transport
  close.
- **Why this matches C++.** C++ keeps A running while a `QueuedClient` for
  it is held, and cancels A once the caller and the last holder let go. The
  Peer can see B, parked calls and wire references. It cannot see a Client
  that the application kept (O2).
- **Native.** No change in the core. The core mints no P (rule 12), so the
  held-P term never applies to a native host. ANSWER_FINISHED is reported as
  on main now: withheld while a call that the remote has not finished is
  pipelined on A, and queued at the Finish of the last such call
  (`capnp_core.h`, the effect comment; `docs/native-abi.md`, "When no effect
  comes"). `capnp_return_canceled(A)` returns `CAPNP_E_INVAL` while such a
  call waits (`AnswerHasPipelinedCalls` maps there, `abi.zig:352`). The
  host owns every OWED_ANSWER reference, so it applies the C++ rule itself
  (see "The native shape"). The known limit above stays for a native host,
  `HostPeer` and the WASM host ABI: they run with answer promises off.
- **Order.** The part of P0-1 above is on main. This design adds one term to
  it. If the two commits that are not on main land first, nothing here
  changes.

## Termination proof

Every list below is closed: the design adds no other way in or out.

**Bindings.** A binding enters A's answer record only at a mint (rules
1-12). It leaves only through `takeAnswerBindings(A)`, a `fetchRemove` of
A's record. That takes the whole record, so a later take finds nothing. A P
record is in exactly one answer record. So each binding settles at most
once. The existing `error.PromiseAlreadyResolved` refusal is a second
guard.

| Event | Where | P gets | Why it happens at most once |
|---|---|---|---|
| A's Return is delivered | step 1 at the 4 sites above | see step 1 | the take empties A's record, so a second Return for A finds nothing |
| `Peer.shutdown` | shutdown entry | "peer shutting down" | the shutdown takes every record once; `is_shutting_down` guards a second call, and rule 10 stops new mints |
| Transport close | `finishTransportClosedNotification` (`mod.zig:4463`, run from `notifyTransportClosed` or after a deferred close) | "disconnected", no frame | takes every record |
| `Peer.deinit` | deinit | freed, no effect | — |

A Call with A's id cannot arrive while the binding lives: it fails with
`DuplicateQuestionId` ("Ids stay in use"). So the old row "a reused id ends
the binding" is gone. A reuse is now a protocol error, as in C++ and the
native shim.

Why every binding reaches one of these events: rule 6 makes A an answer
that an application handler owes, and rule 7 excludes the answers the Peer
relays. The handler answers A through one of the Return functions, and
each delivered Return passes one of the four step-1 sites (step 0a proves
this list with a test per exit). The Peer never stops the handler from
answering: it delays the answer-finished hook only while a holder of P
exists, and each holder ends without A's Return ("The caller's Finish of
A"). A handler that never answers A keeps the binding until shutdown, close
or deinit, as it keeps A itself.

**Shutdown.** `Peer.shutdown` completes when `questions` is empty
(`src/rpc/peer/peer_lifecycle.zig:344-361`). A call on P is one of our
questions. Without the shutdown row, a handler that never answers A would
stop the shutdown forever: the bare Peer has no drain bound
(`shutdown_drain_timeout_ms = null`, `src/rpc/peer/state.zig:87`). The
archived hold failed its last review on this class (a streaming call that
the stopped stream drain never answered). Rejecting every binding at
shutdown makes every parked call on P fail, so the shutdown completes. A
new call on P during shutdown fails at once (`sendCallResolvedAllocating`
returns `error.PeerShuttingDown`). Rule 10 stops a mint after shutdown
starts, so "every binding" means every binding.

**Leases and P.**

| Event | Lease | Then |
|---|---|---|
| binding settles (step 2) | L1 ends | step 2 may remove P |
| B's Return is delivered | that B's L2 ends | step 2 may remove P |
| shutdown | L1 ends (P is rejected); L2 stays until B's Return | step 2 may remove P |
| transport close | all end | the close path removes P |
| deinit | — | everything is freed, no effects |
| a B dropped with no Return (an existing defect class) | L2 stays | until close; P still counts in the limit |

**Calls parked on P.** Each one is either our loopback question or (if P
was sent to the remote) a remote call. The existing promise-export code
already ends each of them exactly once:

| Event | Ends it with |
|---|---|
| P resolves | replay: dispatch, park again on a promise export (H10), forward, or fail |
| P is rejected | an exception with P's reason |
| our question is cancelled or its deadline passes | `finishLoopbackAnswer` -> `cancelQueuedPendingQuestion` -> a `canceled` Return that the cancelled question absorbs |
| the remote finishes its call | `cancelQueuedPendingQuestion` |
| transport close, deinit | the close rejects P, so each parked call fails; deinit uses the existing sweep (`forceCancelAllQuestions` for our questions) |

A call parked on P that the remote finishes is cancelled together with the
calls pipelined on it, as any queued call is on main: the part of P0-1 that
is not on main would keep it (see "The caller's Finish of A"). This design
does not change that.

A loopback call that fails must get its Return without the heap, or one
failed allocation leaves our question with no Return. Step 2 builds that
exception Return in the H8 stack buffer that `deliverLocalException` uses
(`src/rpc/peer/peer_lifecycle.zig:709-741`, 4 KiB). A loopback question
keeps its loopback marker until that Return, also after a transport close
removes P (local capabilities stay callable after close,
`mod.zig:4546-4556`). For a remote call, main now ends the connection when
the Peer settles a call itself and cannot send its one Return
(`peer_return_dispatch.loseReturnForPeer`; `9ffc648e`, `bd494c3c`). The
promise-export replay fails a parked call through `sendParkedCallException`
(`peer_call_inbound.zig:602`), so step 2's failure Return for a remote call
parked on P follows that rule. This replaces the older best-effort rule
(`bdc656f`). A peer with no attached transport (`HostPeer`, the WASM host
ABI) only reports the error (`docs/supported-surface.md`, "Some Returns that
the Peer cannot send do not end the connection").

**The answer-finished hook.** Two terms delay it: a call that the remote
pipelined on A and has not finished (on main), and a held P (this design). The
first ends at that call's Finish or at A's Return. A P is held only by a
lease, a reference or a parked call. A lease ends at its B's Return. A parked
call ends by its own cancel, deadline or close. A wire reference ends at the
remote's Release or the close. None of these needs A's Return. The hook runs
at most once per answer ("The caller's Finish of A", "At most once"):
`hook_withheld` is cleared when the check is queued, and A's Return, shutdown
and close remove the answer record.

**No cycle.**

- B is never delayed. The design holds no message that the Peer would
  otherwise send. The hook is a local callback, not a message.
- The Peer never waits for the remote. Only a delayed hook can wait for a
  remote action (a Release of P), and only if our own handler sent P to the
  remote. C++ does the same: an export of a pipelined cap keeps A's
  pipeline branch.
- Params can name only answers that were active when B arrived, so a
  reference points back in arrival order, and never at B (rule 2). So in a
  chain (B2 names B, B names A), the newest answer has no delayed hook. Its
  caller's cancel (Finish) reaches its host, which cancels its own calls on
  its param. That ends the next holder, and the chain unwinds from the top.
- A's results may name P itself or a promise export that leads to P. The
  existing cycle check refuses that resolution, and P is rejected.
- The only waits are application waits: a handler that waits for a call on
  P before it answers, or a host that answers A only when told and whose
  param is still held. C++ has the same waits. The caller's cancel,
  deadline or close bounds each one.

The archived hold had a protocol cycle: the Peer held X's Return, the
remote sent Finish(Y) only after our Finish(X), and some states of Y ended
only at Finish(Y). Here the Peer holds no message.

## Bounded memory and O(1) work per frame

**Memory.**

- Live P's: within `answer_promise_limit` (default 1024) per Peer, counted
  from the mint to the removal of P, whatever keeps P (L1, a lease, a wire
  reference or a parked call). A remote that keeps B's queued after A
  returned keeps their P's, and those P's still count. At the limit, a new
  path stays `.promised`, which is today's behavior. Each live P holds one
  export entry, a P record (about 64 bytes, plus at most 64 ops of 2
  bytes), and at most one 1 KiB reason copy.
- Answer records: at most one per A with a bound P, so at most the number
  of live P's.
- Leases: one per (B, P), counted in the same limit. Two params of one B
  on the same path share one lease. Without the count, N pending B's that
  each name the same 1024 paths would keep N x 1024 leases after their
  frames are freed.
- The settle work list has one reserved slot per live P.
- Parked calls on P use the existing budgets (`max_pending_export_promises`,
  `max_pending_queued_calls`, `max_pending_queued_call_bytes`).
- One hostile Call that names A 1024 times on one path makes one P.

**Work per frame.**

| Frame or event | New work |
|---|---|
| a Call (B) | O(1) lookups per `.promised` entry (rules 1-12, and the (A, ops) lookup, which hashes at most 64 ops), plus the ops copy for a new P. `InboundCapTable.init` already walks every entry, and the remote pays for the frame. |
| a Call, Accept, Join, Provide or Bootstrap | two more lookups in the in-use check |
| a Return of ours | one lookup, then O(k) for A's P's and B's leases, each once |
| a Finish | one lookup (the gate term) |
| a Release of P, a park, an unpark | one O(1) held recompute |

There is no walk over a queue or a chain per frame. (The archived hold's
last review found a remote-forceable O(chain x queue) walk per redirect.)
The existing per-frame scans (`hasQueuedPendingQuestionId` per Call,
`cancelQueuedPendingQuestion` per Finish) do not change, and calls parked
on P count toward the same bounded queues. Step 2's cycle check walks a
chain of promise exports, but only when a binding settles. A remote can
build such a chain through a deferred echo-like method (each A_k returns
its param P_k). The chain is at most `answer_promise_limit` long, so the
total cost is at most O(limit²) hops, paid once per settle.

## A hostile remote

- **State it can make us hold:** at most `answer_promise_limit` live P's
  and leases together.
  Each needs a pending call to one of our deferred handlers plus a Call
  frame. A binding lives as long as our handler keeps A unanswered, which
  is as long as the remote could keep A alone. A P outlives its binding only
  while something holds it, and it still counts in the limit.
- **A wrong value:** P resolves only from its own A's Return. A's id and
  every lessee B's id stay in use while their records live ("Ids stay in
  use"), so no other question can take them, and P has its own export id.
- **Two resolutions:** no (the take, then `PromiseAlreadyResolved`).
- **Blocking us:** B dispatches at once, and the Peer sends nothing later
  because of a binding. A remote can delay the answer-finished hook for A by
  holding P, but only if our handler sent P to it, as in C++.
- **The cap table:** the limit keeps live P's well below `max_table_size`.
- **Our loopback answers:** rule 4.
- **CPU:** see the table above.

## E-order

E-order (`rpc.capnp:61-65`, which links the E partial-order rules) has a
fork rule: calls that Alice makes on a reference before she passes it to
Bob are delivered before the calls that Bob makes on it.

- **Calls on P.** They park on P in call order. The replay walks the queue
  in that order, and a cancel removes an entry with `orderedRemove`, which
  keeps the order of the others. Calls made after P resolves follow P's
  link to the target, after the replay has dispatched the older ones.
- **The fork: the remote's calls on A.T, then our calls on the param.** The
  remote sends C1 on `promisedAnswer{A, T}`, then passes A.T to us in B,
  and B's handler calls P. C1 must reach E first. The invariant: every call
  queued on `pending_promises[A]` when A's Return goes out is replayed (or
  failed) before any call parked on a P bound to A. Placement gives it. The
  Return function drains `pending_promises[A]` in its commit after the send
  (`commitReservedResolvedAnswer` -> `drainPendingPromises`,
  `peer_call_inbound.zig:410-434`; the exception, sent-elsewhere, canceled
  and tail-call paths fail them, `peer_return_send.zig:353`, `:735`, `:758`
  and `:800`). Step 2 runs later, at the outermost scope exit. Test T-u pins
  this. A change that moves P's settle into step 1, or a Return path whose
  drain runs after the flush (P1-1 changes the prebuilt-frame path), must
  keep T-u green.
- **Two params on one path.** They share one P, so their calls share one
  queue, as with C++ `clientMap`.
- **The remote's calls on A.T sent after B.** They also wait in
  `pending_promises[A]`, so they run before our calls on P even when ours
  were made first. E-order does not order two vats' calls after the fork.
  C++ shares one queue, so it runs them in arrival order. This is a
  deliberate difference.
- **P sent to the remote.** The remote then holds two references (its
  pipeline on A.T, and P). E-order is per reference. A `Resolve` of P to our
  export E needs no embargo: the remote's calls that went through P arrive
  first, and P forwards each one to E at once. Only a result that points
  back at the remote (an import, prerequisite 0b) needs the remote's
  embargo.
- **B.** It is never delayed, so calls to B's target keep their order. This
  is the main reason option 1 was rejected.
- **Pre-existing, not changed:** a call on P made by code that runs inside
  P's own replay is dispatched at once, before replay entries that are not
  yet done. Every promise export has this today, because
  `resolvePromiseExportToExport` sets `resolved` before it replays. C++
  orders it (`QueuedClient` forwards through one fork). This is open
  (review question 5).

## Edge cases the plan names

| Case | Result |
|---|---|
| The remote finishes A before P's parked calls replay | The binding stays (the handler still owes A). The hook waits while P is held. Step 1 pins E before the early-Finish cleanup releases A's result references. Tests T-d, T-e. |
| Cycles (`58612dc`) | References point back in arrival order, and never at B itself. A result that names P, or a chain back to P, is refused by the cycle check, and P is rejected. |
| E-order against the remote's pipelined calls | See "E-order" and T-u. |
| Out of memory while parked | Mint is all or nothing (`errdefer`). Step 1 allocates only for the decode, with a static reason as the fallback. A failed `Resolve` send is reported and dropped, as for other frames the Peer sends itself. P still resolves locally, and the remote's calls on P still reach the target through P. But the remote's import of P then never resolves, which breaks the rule that a `senderPromise` gets one `Resolve` (`rpc.capnp`, `senderPromise`). Out of memory only. Open: is a retry at the next flush worth it? Failing a loopback call uses the H8 stack buffer. |
| Shutdown | Every binding is rejected at shutdown start, and rule 10 stops new ones. |
| Re-entrancy on a synchronous transport | Step 1 sends nothing and calls nothing. Step 2 runs at the outermost scope exit and looks P up by id after every call that can run application code. The e2e shape (B's Return inside P's replay, inside A's Return) ends L2 while P resolves; the `queued` flag keeps P on the list once, and P is removed once, by the step-2 recheck. Test T-q. |

## The native shape

- Feature bit `CAPNP_FEATURE_OWED_ANSWER_CAPS (1ULL << 1)`.
- Opt-in field `uint32_t owed_answer_caps;`, appended to `capnp_conn_opts`
  after `answer_finished`. 1 means on, 0 means off, any other value is
  `CAPNP_E_INVAL`. A host built against an older header passes a smaller
  `struct_size` and gets 0, as for `answer_finished`.
- Cap kind `CAPNP_CAP_OWED_ANSWER 4`: `id` = an answer that the host owed
  when the core queued the effect, `ops`/`nops` = the pointer-field path
  into the results it returns for that answer.
- **Layout.** `capnp_cap` keeps its fields (`kind`, `id`, `ops`, `nops`).
  `capnp_conn_opts` gains one field at its end, by the documented
  `struct_size` rule. The other declarations do not change.
  `CAPNP_CORE_ABI_VERSION` stays 1. capnp-swift's header snapshot changes.
- **Delivery.** In the INBOUND_CALL caps[] of B, a `.promised{A, T}` entry
  becomes `{OWED_ANSWER, A, ops}` when all of these hold: the opt-in is on;
  A is in `pending_answers` (the host owes it); A is not a `yourself` call
  (`PendingAnswer.yourself`); and the Peer still counts A in
  `active_inbound_questions`, so the remote has not finished A (the Peer's
  rule 3). The ops are copied into the effect node and stay valid until the
  commit. Otherwise the call is refused as today.
- **Drain order** (a new rule in `docs/native-abi.md`, next to the
  ANSWER_FINISHED rule at `:190-195`):
  - The host binds an OWED_ANSWER cap to its own record of A when it drains
    B's INBOUND_CALL. It never looks A up by id later.
  - At that moment the id is unambiguous. Effects are one FIFO queue
    (`Conn.pushNode`, `conn.zig:1026`). Any Return the host gave A is an
    OUT_FRAME queued behind B's effect, so the remote has not received it
    and cannot have reused the id. After the host drains that OUT_FRAME,
    the remote may reuse the id at once (`docs/native-abi.md:98-99`).
  - The host may already have answered A between the push and the drain.
    Then it resolves the param from the answer it gave. So after it answers
    an INBOUND_CALL, the host keeps that answer's result capabilities (or
    its failure) until its next drain reaches an empty queue.
  - E-order: `capnp_return_results(A)` queues INBOUND_CALL effects for the
    remote's calls pipelined on A (the Peer replays `pending_promises[A]`
    inside the answer). The host runs its own queued calls on the param only
    after it has drained those effects, for example after its next drain
    reaches an empty queue.
- **Cancel.** The core reports ANSWER_FINISHED as on main now: it waits
  while a call that the remote has not finished is pipelined on A, and queues
  the effect at the Finish of the last such call. `capnp_return_canceled(A)`
  works as on main: `CAPNP_E_INVAL` until an ANSWER_FINISHED was queued for A.
  A param that names A is not a pipelined call, so that wait does not cover
  it. The host holds every OWED_ANSWER reference, so it applies the C++ rule
  itself: it cancels A on ANSWER_FINISHED only when none of its handlers
  still holds a param promise for A. A host that cancels at once fails its
  own param calls, as the core would if it cancelled.
- **The host resolves it.** The host answers A itself, so it knows the
  capability at `ops` in its own results, or A's failure. That is where a
  language runtime keeps local promises (C++ keeps them in `QueuedPipeline`,
  not in the protocol). To pass the promise on before A returns, the host
  makes its own promise export (`capnp_promise_export`) and resolves it
  when A completes.
- **v1 refusals.** An OWED_ANSWER cap as a `capnp_call` target, in the
  caps[] of params or results, or as the `to` of `capnp_resolve_promise`,
  returns `CAPNP_E_INVAL`. On `6acdca02`, `validCap` already rejects any kind
  above PROMISED (`src/native/abi.zig:756-762`). It must keep rejecting
  kind 4 after `effects.CapKind` gains it (guard N-e).
- **Host cost** (for the capnp-swift relay row): a promise type for the
  param; binding at drain time; keeping answered results until the queue
  drains; the E-order rule above; and a reference count for the cancel rule.

## Comparison with the C++ reference

What matches: the param is a promise at arrival; B is never delayed; calls
on it queue until A returns and fail with A's exception; the remote's calls
on A.T sent before B run before the param's calls; one queue per path; the
Finish of A does not cancel A while a visible holder exists; a param that
names B itself is not a promise; reuse of a live answer id is a protocol
error; a resolved param is written as its target.

Deliberate differences:

1. C++ gives a promise for A in any state. We do so only for an A that an
   application handler owes (rules 6 and 7). Other A's keep today's
   `.promised` behavior. A `noPromisePipelining` A gets no promise (rule 9);
   C++ gives a disabled pipeline, so both fail.
2. The remote's calls on A.T sent after B run before our calls on the
   param. C++ runs them in arrival order (see "E-order").
3. C++ follows a result that is itself a pipelined promise, and a tail call
   (`onTailCall`). We reject both in v1: an A answered by
   `takeFromOtherQuestion` rejects P with the reason that main gives the
   queued calls of such an answer ("results taken from another question;
   pipelining is not supported on a tail call"). Main fails those calls
   instead of sending them on to the tail call's answer, as C++ does
   (`docs/supported-surface.md`, "A call pipelined on a tail-call forward
   fails").
4. C++ keeps the param valid while anyone holds it. Ours is valid while the
   Peer can see a holder, or until A returns (O2).
5. C++ cancels A once the last holder and the caller let go. We tell the
   host at that point too, but the Peer cannot see a Client that the
   application kept.
6. A Client for P cannot make pipelined or streaming calls
   (`LocalCapabilityPipelineUnsupported`,
   `LocalCapabilityStreamingUnsupported`, `src/rpc/peer/mod.zig:226-228`).
   A C++ `QueuedClient` can.
7. C++ has no C ABI. The native host resolves OWED_ANSWER itself and
   applies the cancel rule itself.

## How this design answers the archived hold's reviews

The archived hold is the branch `archive/p1-3-tail-call-hold`. Its reviews are
in `docs/notes/p1-3-hold-reviews/` on that branch (`p13-review2.json`,
`p13-review3.json`, `p13-review5.json` and `p13a-result.json`). They are not
on main. The "PROBE inv A2" quoted above is in `p13-review5.json`.

| Finding (`p1-3-hold-reviews/*.json` and the P1-3a result) | Rule here |
|---|---|
| Rounds 2-5: each round found one more state of Y that never ends | Rules 6 and 7: only an A that an application handler owes |
| Round 4: a cycle through the remote's Finish(Y) | The Peer holds no message; B is never delayed; the hook waits only for holders that end without A's Return |
| Rounds 2 and 5: a stale entry keyed by a reused id gave a wrong value | A's and B's ids stay in use while their records live; P has its own id |
| Round 3: `destroyExportEntry` dropped parked calls with no Return | L1 drops only after P resolves; P with parked calls is never removed (main now also answers such calls when an export goes: `0201e8c4`) |
| Round 5 verify: the stream drain stopped at shutdown, so the hold hung | Shutdown rejects every binding; rule 10 stops new ones |
| Round 5 verify: an O(chain x queue) walk per redirect | Hash lookups only; no walk per frame |
| Rounds 3 and 5: a loopback id collided with a remote id | Rule 4, the in-use check, and the loopback skip in step 1 |
| Rounds 2-5: callbacks ran inside a send | Step 1 runs no callbacks; step 2 and the hook run only at the outermost scope exit |
| Round 5: a mark removed without settling the hold | Step 1 runs at every delivery site, plus shutdown, close and deinit; a test per exit, the scope check and the T-p invariant catch a missed site |
| Rounds 3-5: out-of-memory waits | Step 1 needs no heap except one decode, which has a static fallback; failing a loopback call uses the H8 stack buffer |

## Limits that stay

- A param that names an answer that is queued, parked, forwarded, relayed
  by the Peer, routed to a third party, sent with `noPromisePipelining`, or
  a loopback answer stays `.promised`. So does a tail-called answer, when
  the tail call is known before B arrives. An answer that tail-calls after
  B arrived rejects P.
- A param that names an answer that had already returned when B arrived
  keeps today's path. If the caller finishes that answer before B
  dispatches, the param stays `.promised`. Minting for a returned answer
  too would close this, but it is a follow-up.
- A result that is a promise on one of our questions: P is rejected.
- A result that is an import (the caller's own capability): it works only
  if prerequisite 0b holds; otherwise v1 rejects it with a named reason.
- A Client for P is valid while the Peer can see a holder of P, or until A
  returns (O2). It cannot make pipelined or streaming calls.
- The answer-finished hook for A waits while a visible holder of P exists.
- A `receiverAnswer` that names one of our answers in a remote `Resolve`
  or in the results of a remote Return stays `.promised` (and the native
  shim refuses it, `cap_remap.zig:316-329`). Prerequisite 0e fixes only the
  Disembargo that main sends for the `Resolve` case.
- On a synchronous transport, a Return that was delivered, then got a
  Finish, then a trailing send error, runs step 1 only in the two senders
  that have the "Finish proves delivery" branch. Through the other senders
  the binding stays until close, unless step 0a adds the branch there.
- `HostPeer` and the WASM host ABI do not change.
- Native: the host resolves OWED_ANSWER itself and cannot pass it to the
  core.

## Found while reading

**Probed (review 2): a Disembargo with the wrong target.** A remote
`Resolve(Y -> receiverAnswer{A, T})` names one of **our** answers. A C++
peer sends this when it resolves a promise export to a pipelined cap of a
question it asked us (the case in the C++ `clientMap` comment).
`resolveDescriptor` maps it to `.promised{A}` (`caps/inbound.zig:230-233`).
`handleResolveWithOps` then sends `Disembargo(senderLoopback)` with target
`promisedAnswer{A}` (`src/rpc/peer/resolve.zig:73-94`). In a message we
send, that target names **our** question A, not the promise. The target
must be `importedCap(Y)`. Probe "Resolve to receiverAnswer" in "Review"
shows this on `d6f19f1`, and `resolve.zig` is unchanged on `6acdca02`. This is
prerequisite 0e, a separate fix.

**Read, not probed: the forward re-encoding.** `Peer.mapInboundCapForForward`
(`src/rpc/peer/mod.zig:3652-3655`) re-encodes an inbound `.promised` entry
as `receiverAnswer` when the Peer forwards a call back to the remote. It
stores the inbound `PromisedAnswer` unchanged (`noteReceiverAnswer`,
`caps/lifecycle.zig:142`), and the outbound encoder writes it back as it is
(`caps/outbound.zig:343-346`, `writeReceiverAnswer(promised.question_id,
ops)`). In an inbound table that id is one of **our** answers. But
`rpc.capnp` defines `receiverAnswer` as "a capability expected to be
returned in the results of a currently-outstanding call posed by the
sender" (`rpc.capnp:1098-1100`), so a `receiverAnswer` that we send names
one of the **remote's** answers to our questions. So the forwarded call
names a different call. For an eligible entry, this design rewrites the
entry to `.exported{P}` first, so the forward carries `senderPromise(P)`,
which is right. Other entries keep the problem. It is the same id-direction
mistake as the Disembargo above. Re-read on `6acdca02`: the P0-1 roll-up and
its forward-path fixes did not touch this code. The claim now rests on the
code and the spec text, not on a run. Open: a probe ("Questions", item 6);
it is a separate fix.

## Implementation plan (next sprint)

**Step 0: prerequisites, each red first.**

- 0a. List every exit that delivers a Return for an inbound answer. On
  `d6f19f1` and on `6acdca02` they are the two frame-send wrappers and the
  two "Finish proves delivery" branches (re-read: P0-1 added no delivery
  site; the new Returns, such as `failCallQueuedOnDestroyedExport`, go
  through `sendReturnException`). Route any other exit through them. One
  test per exit shows that a binding settles. Build the guarded-entry list for
  "Answer scope" and test each entry. Optionally add the "Finish proves
  delivery" branch to the other senders.
- 0b. A loopback call parked on a promise export that resolves to an
  import is forwarded (translate mode). The archived round-4 repair
  reported that this forward never gets a Finish and leaks on both ends
  (unverified on main). Main's forward-path fixes since then (`959cdc93`,
  `bd608348`, `ed8e0c9f`) change when a forwarded question gets its Finish.
  Whether they cover this loopback case was not checked. Write the red test
  first. Fix it, or make v1 reject an import result.
- 0c. Promise-export internals: no `Resolve`, no wire reference and no fd
  for a P with no wire reference; adopt a pin taken in step 1; a rejected P
  fails its parked calls with its own reason and type; heap-free failure of
  a parked loopback call.
- 0d. The part of P0-1 in "The caller's Finish of A" has landed
  (`a51eabe8`). The two commits that keep a queued call at its Finish are not
  needed.
- 0e. The Disembargo for `Resolve(Y -> receiverAnswer)` targets
  `importedCap(Y)`. Red test: the probe in "Review". Also check where our
  calls on Y go after the embargo (not probed): they must reach A's
  pipeline here, not a `promisedAnswer` target on the wire.

**Files.**

- New `src/rpc/peer/answer_promises.zig`: the deep module that owns the
  invariant (the three record kinds, the held count, the limit, mint, take,
  `noteAnswerReturned`, `noteAnswerPromiseRefChanged`, the scope guard and
  flush, the delayed hook, the dispatch rewrite, reclaim, shutdown, close
  and deinit).
- `src/rpc/peer/call/peer_call_inbound.zig`: mint in `handleCall`, the
  `errdefer`, and the dispatch rewrite next to `resolvePromisedParamCaps`.
- `src/rpc/peer/call/peer_call_orchestration.zig`: the rule-6 flag at the
  two `on_call` sites.
- `src/rpc/peer/return/peer_return_send.zig`: step 1 at the four sites.
- `src/rpc/peer/mod.zig`: the fields, the in-use check, the held-P term at
  the three users of `hasCallsPipelinedOnAnswer` (the hook condition in
  `handleFinish`, `sendReturnCanceled` and `reportFinishedAnswerOnceUnwanted`),
  the scope guards on the public entries, the shutdown, close and deinit
  hooks, the `active_inbound_questions` field (`:556`) and its readers
  (`:4429`, `:4873`), and the docs of `setAnswerFinishedHandler` and
  `sendReturnCanceled` (the hook can now run at a later scope exit, and a
  param can hold it back).
- `src/rpc/peer/third_party/peer_cross_peer_proxy.zig`: `exportedCapTag`
  and its callers, where an `.exported` cap is encoded, write a resolved
  answer promise as its target.
- `src/rpc/peer/state.zig`: the lease count on `ExportEntry`, and the new
  value struct of `active_inbound_questions` (the old `bool`, plus the rule 6
  and rule 9 flags). Its writers are `peer_call_inbound.zig:173` and
  `src/rpc/peer/third_party/peer_third_party_routes.zig:609`.
- `src/rpc/peer/peer_promise_exports.zig`,
  `src/rpc/promises/pending_calls.zig`, `src/rpc/peer/peer_cap_lifecycle.zig`
  and `src/rpc/peer/peer_export_release.zig`: step 0c, the held recompute,
  and the leases in each "no references left" check.
- `src/rpc/peer/resolve.zig`: step 0e.
- `src/native/conn.zig`, `src/native/cap_remap.zig`, `src/native/abi.zig`,
  `src/native/effects.zig`, `src/native/include/capnp_core.h` (new defines,
  one new `capnp_conn_opts` field at its end, and comments).
- `tests/rpc/peer/rpc_reflected_direct_handler_test.zig:2820`: the "known
  limit: the answer-finished hook does not wait for a call whose params name
  the answer" test passes on main and fails once this lands, so the same
  commit changes it. It keeps the limit for a Peer with answer promises off,
  and pins the fix for one with them on.
- `tools/e2e_known_limits.zig` (remove the five rows),
  `docs/supported-surface.md` (the same known-limit bullet: it stays true for
  a native host, `HostPeer` and the WASM host ABI, and not for a Peer with
  answer promises on; and the "pending answer" text that the rows link to),
  `docs/native-abi.md` (the OWED_ANSWER drain
  order and cancel rules), `docs/downstreams.md` (the capnp-swift row: pin
  bump, header snapshot, host cost), `CHANGELOG.md`, both Experimental API
  snapshots.

**Red tests.** Each test passes with the fix and fails again with its part
ablated. All of them fail on main (there the handler cannot call the
param), except these guards, which pass on main and must keep passing:
T-e2, T-j, T-k, T-l, T-m, T-v, T-z, T-aa, N-a, N-c, N-d, N-e and N-k. A new Peer
test file must be wired into `test-rpc-peer`. `src/native/conn_test.zig`
runs only through `zig build test-serialization` and `zig build test` (the
`src/lib_core.zig` root). Prove that each new test runs.

Peer (a new `tests/rpc/peer/rpc_answer_promise_test.zig`):

- T-a: A pending, B names A; a call on the param parks; A answers with
  results; the call reaches A's export exactly once.
- T-b: A fails; the parked call fails with A's reason and type.
- T-c: A ends with `canceled`, `takeFromOtherQuestion` or a prebuilt
  non-results frame; the parked call fails with the fixed reason.
- T-d: the remote finishes A with `releaseResultCaps` before A returns (the
  C++ order); the parked call still reaches E.
- T-e: Finish(A) while B runs; the hook is not called, and
  `sendReturnCanceled(A)` refuses. B returns; the hook runs once, at that
  scope exit; its inline `sendReturnCanceled(A)` works; P is rejected with
  "call canceled".
- T-e2 (guard, probe r1): B returns and is finished, then Finish(A); the
  hook runs at the Finish, as on main; its inline `sendReturnCanceled(A)`
  gives one canceled Return.
- T-e3: B parks a call on P and returns; Finish(A); no hook. The parked
  call's caller cancels it; the hook runs once.
- T-e4: B returns P to the remote; Finish(A); no hook. The remote releases
  P; the hook runs once.
- T-e5: both terms at once: a call C pipelined on A (not finished) and a held
  P. Finish(A); no hook. Then (a) the remote finishes C, and the Peer
  releases P; or (b) the Peer releases P, and the remote finishes C. In both
  orders the hook runs once, at the second event (this pins the "flag" rule in
  "The caller's Finish of A"). The same case with the flag set only when the
  held-P term alone stops the hook never runs the hook (the failure this
  test catches).
- T-f: A's results name P, or a chain back to P; P is rejected with
  "promise resolution cycle"; no recursion.
- T-g: B returns before A; a kept Client parks a call; A answers; the call
  arrives; P is then removed, and a later call gets `error.UnknownExport`.
- T-h: B returns P to the remote; A answers; one `Resolve` goes on the
  wire; the remote's call on P reaches E.
- T-i: P never sent; no `Resolve` on the wire; E's wire `ref_count` is
  unchanged and no fd is attached.
- T-j (guard): with the Peer at `max_active_inbound_questions` owed calls
  (active and finished-early together), while A has a record: a Call,
  Accept, Join, Provide or Bootstrap with A's id returns
  `DuplicateQuestionId`. The same holds for B's id while B holds a lease.
  The loopback allocator skips both ids. The old handler's late Return for
  A then settles A's P. This passes on main since `05cf466a` (probe r2 no
  longer reproduces), and it must keep passing. It cannot show the two new
  in-use terms, which are redundant today ("Ids stay in use").
- T-k: a param names a loopback answer id; nothing is minted.
- T-l: a param names B itself; nothing is minted.
- T-m: at the limit, the next new path stays `.promised`; the cap table
  does not grow.
- T-n: `Peer.shutdown` with a bound P and a parked call, and no drain
  bound; the call fails "peer shutting down", and the shutdown completes.
- T-o: transport close and deinit with a bound P; each parked call gets one
  Return; no leak (testing allocator).
- T-p: one-allocation-failure sweeps over mint, step 1 (results and
  exception), the dispatch rewrite and B's Return; every question gets
  exactly one Return; no leak.
- T-q: B's Return inside P's replay inside A's Return; P is on the settle
  list once and removed once; no use after free.
- T-r: the remote sends 4000 B's that each name A on a new path; minting
  stops at the limit; the time per frame stays flat. B's queued behind
  another answer keep their P's after A returns, and those still count.
- T-s: two calls on P before A answers arrive in order.
- T-t: B queued behind another answer; Finish(A) before B replays; B
  replays with P, and P settles.
- T-u (E-order fork): the remote sends C1 on A.T, then B naming A.T; the
  handler calls P; A returns; C1 reaches E before the handler's call.
- T-v (guard, probe r5): A pending when a queued B arrives, then answered
  before B replays; B's handler gets `.exported{E}`.
- T-w: B's handler writes a resolved P into its results; the Return
  carries `senderHosted(E)`, not P.
- T-x: two params on one path, in one B and in two B's, get one P; calls
  through both keep call order.
- T-y: the scope check: a Return sent through each guarded entry settles P
  before the entry returns; a test-only unguarded entry trips the check.
- T-z (guard): no mint after shutdown starts.
- T-aa (guard): no mint for an A sent with `noPromisePipelining`, or for an
  A relayed by the cross-peer proxy.
- T-0e: `Resolve(Y -> receiverAnswer{A})` sends a Disembargo with target
  `importedCap(Y)` (red on main, see the probe).

Native (`src/native/conn_test.zig`, plus `abi_header_test.zig` and the
native fuzz invariant in `tests/native/fuzz_abi.zig`):

- N-a (guard): opt-in off; the refusal stays as today.
- N-b: opt-in on, A owed by the host; B's caps[] has `{OWED_ANSWER, A,
  ops}`, valid until the commit.
- N-c (guard): A not owed by the host (parked in the core); refused as
  today.
- N-d (guard): A is a `yourself` call; refused.
- N-e (guard): OWED_ANSWER as a `capnp_call` target, in caps[], or as the
  `to` of `capnp_resolve_promise`; `CAPNP_E_INVAL`. On `6acdca02` kind 4
  fails `validCap` already; the test keeps it so after `effects.CapKind`
  gains the value 4.
- N-f: Finish(A) after delivery; ANSWER_FINISHED is reported as on main
  (withheld while a call the remote has not finished is pipelined on A);
  `capnp_return_canceled(A)` works; `capnp_return_results(A)` instead also
  works.
- N-g: the feature bit is set; an older `struct_size` means off.
- N-h: A and B in one push; the host drains A, answers A, then drains B;
  B's caps[] still has `{OWED_ANSWER, A}`, and A's Return OUT_FRAME comes
  after B's INBOUND_CALL in the queue.
- N-i: after the host drains A's Return, the remote reuses A's id; the new
  call is a new INBOUND_CALL, and nothing about B changes.
- N-j: `capnp_return_results(A)` queues the INBOUND_CALL for the remote's
  call C1 on A.T before it returns.
- N-k (guard): the remote finishes A, then B names A; no OWED_ANSWER; refused
  as today.
- Fuzz: every delivered OWED_ANSWER names an answer that was in
  `pending_answers` when the effect was queued, and any Return for that
  answer comes after the effect in the queue.

**Acceptance.** The five `h9-pending-answer` rows turn into XPASS. The same
commit removes them (`tools/e2e_known_limits.zig:47-51`) and fixes the
docs their `link` names. The e2e server and the four reference clients do
not change: `check()` gets a Client for P (origin `.exported`), its `tag()`
call parks on P, the held `mint()` answer resolves P at the scope exit of
`check()`'s frame, and the replayed call reaches our own HostToken, so the
`local` check holds. The Zig e2e interop counts on CI 39541793776 were TCP 56
pass / 12 skip / 4 XFAIL and Unix 14 / 57 / 1. P1-3 (`6acdca02`) added the
`cancel_id_reuse` and `cancel_pipelined` scenarios, so those absolute counts
changed. The effect of this work is a delta: TCP +4 pass and -4 XFAIL, Unix
+1 pass and -1 XFAIL, and no change in skips. Record the real counts from a
main run taken before the work starts, and again after it.

**Effort.** L: about 7 agent-days. Step 0 about 1.5 days (with the scope
guard list and 0e), the Peer module and wiring about 2.5, native about 1,
tests and sweeps about 1, then one adversarial review round and its repair
about 1.

## Owner decisions

- **O1: default.** Recommend answer promises on by default for `Peer`, off
  for the native `Conn`, `HostPeer` and the WASM host ABI. Why: generated
  servers then work without a change, and the hosts never see a core-made
  export. Cost: a hand-written handler that reads an `InboundCapTable`
  entry directly now sees `.exported{P}` where it saw `.promised`.
- **O2: Client lifetime.** Recommend for v1: a Client for P works while the
  Peer can see a holder of P (B still running, a parked call, a wire or
  promise reference), or until A returns. Once the caller finished A and no
  visible holder is left, the host is told and may cancel A; a Client that
  the application kept then sees A's outcome, and later
  `error.UnknownExport`. A reference-counted Client origin (Experimental
  `ClientOrigin`) can come later if an application needs to keep the param.
- **O3: the limit knob.** Recommend an Experimental field,
  `Peer.answer_promise_limit` (default 1024), that counts live P's and
  leases together. A new `PeerLimits` field would change the Stable
  snapshot (`docs/api-snapshot.txt` lists `PeerLimits`).

## Review

Two adversarial reviewers read the first version (`df37b64`) on 2026-10-21.
Both gave "fix-then-ship". This revision answers every must-fix finding.

**Probes.** The repair re-ran the reviewers' probes on `d6f19f1` in a
scratch copy (appended to `tests/rpc/peer/rpc_answer_lifecycle_test.zig`,
then `mise exec -- zig build test-rpc-peer`). They print:

- r1: `hook count=1 canceled Returns for A=1`. Main cancels a
  cancel-driven A after B returned. The first version suppressed that hook
  for good.
- r2: `in_use(10) after Finish(A)=false`, the reused Call 10 is accepted,
  and the old handler's Return for 10 goes out (`results=1`). With the
  tombstones full, main lets a hostile remote reuse an owed id.
- r5 (a variant of the test at `rpc_answer_lifecycle_test.zig:1408`):
  `seen=exported id=0 E=0`. Main gives a queued B the export E when A was
  pending at B's arrival and returned before B dispatched.
- Resolve to receiverAnswer: `Disembargo context=senderLoopback
  target=promisedAnswer imported_cap=null promised_qid=10`. Its
  `expectEqual(importedCap)` fails. All other `test-rpc-peer` tests passed.

**Status on `6acdca02`.** These probes ran on `d6f19f1` and were not re-run
for the update. Only r2 changed: it no longer reproduces, because main
reserves the Finish record at admission (`05cf466a`, "Ids stay in use").
For the others I re-read the code. r1: the hook still runs at the Finish of a
cancel-driven A after B returned, because P0-1 holds the hook back only for
calls pipelined on A, not for a param. r5: `resolvePromisedParamCaps`
(`peer_call_inbound.zig:270`) is unchanged. Resolve to receiverAnswer:
`resolve.zig:73-94` is unchanged.

**Review 1 (must fix).**

| Finding | Resolution |
|---|---|
| 1. The P0-1 term held the hook for good: binding -> A's Return -> hook -> binding (probe r1) | Resolved: "The caller's Finish of A". The gate term counts only *held* P's; the hook is delivered when the last visible holder goes; native reports ANSWER_FINISHED as today. T-e, T-e2-T-e4, N-f replace the old T-e and N-f. |
| 2. Bindings keyed by a reusable wire id; Accept, Join, Provide and Bootstrap skip the reuse check; an old Return could settle a new incarnation (probe r2) | Resolved: "Ids stay in use". The reuse row is gone; reuse is `DuplicateQuestionId`. Step 1 skips loopback Returns. T-j now covers all five message types and both ids. |
| 3. The limit counted bindings, not P's; queued B's could exhaust the cap table | Resolved: the limit counts live P's until removal, and leases (rule 11, "Memory"); one settle slot per P with a `queued` flag. |
| 4. The flush-point list named private functions and missed public entries; L1's drop point was not pinned | Resolved: "Answer scope" (a guard on every public entry, a run-time check, depth raised during the flush). L1 drops only in step 2; removal needs an empty parked queue. |
| Nice to have: shutdown mints, rule 6 text, chain cost, record lifetime, trailing send error, Bootstrap exit, close function, T-j | Resolved: rule 10, rules 6-7, "Work per frame", "Records", "Limits that stay", step 1, the termination table, T-j. |

**Review 2 (must fix).**

| Finding | Resolution |
|---|---|
| 1. The OWED_ANSWER contract was wrong at drain time (an answered A; id reuse after its OUT_FRAME) | Resolved: "Drain order" in "The native shape" (bind at drain; keep answered results until the queue drains; the host E-order rule); N-h, N-i, N-j; host cost in the relay row. |
| 2. False E-order claims; two params on one path got two queues; wrong embargo sentence | Resolved: "E-order" states the fork-rule invariant and pins it with T-u; one P per path (T-x); the embargo sentence is fixed. |
| 3. Hostile id reuse resolved a new binding from the old Return | Resolved with review 1 finding 2. |
| 4. The `Resolve(Y -> receiverAnswer)` Disembargo targets the wrong call (probed) | Recorded: "Found while reading", prerequisite 0e, T-0e. Return-results and Resolve `receiverAnswer` are listed in "Limits that stay". |
| 5. A queued B lost main's `.exported{E}`; a resolved P went back on the wire as `senderHosted(P)` | Resolved: "At B's dispatch" (T-v, probe r5) and "Writing P" (T-w). |
| 6. N-e passes on main | Resolved: N-e is a guard, and it checks `validCap` after `CapKind` gains the value 4. |
| Nice to have: more C++ differences, Resolve refs and fds, `FinishState.withheld`, native rule 3, header field, one P per path, `noPromisePipelining`, `sendCall*` and loopback entries | Resolved: differences 3 and 6, step 2 and T-i, no native withhold (moot), native delivery rule, "Layout", one P per path, rule 9, "Answer scope". |

**Questions for the reviewers, with status.**

1. Is the list of Return delivery sites (step 1) complete on `d6f19f1`?
   Answered: yes for every Return of an A or a B (review 1). The flush
   points were wrong; see "Answer scope". Re-read on `6acdca02`: still
   complete. The new Returns of P0-1 and `0201e8c4` go through
   `sendReturnException`. The export-release functions are new scope-guard
   entries, not new delivery sites.
2. Is any answer handed to `on_call` one whose Return the Peer produces
   from a remote message? Answered: the cross-peer proxy (rule 7 excludes
   it) and the persistence trampoline (rule 5 covers Save and Restore).
3. Can step 2 run while some Peer state is half updated? Answered: with
   the first list, yes. A send path that was not on the list (`sendCall*`,
   loopback dispatch) could answer A and get a flush inside it (review 2).
   The guard on every public entry, and the raised depth during the flush,
   close this.
4. Does rejecting at shutdown break a graceful shutdown that a deferred
   handler would have completed? Open. Review 1 checked that the rejection
   drains our questions.
5. Should the replay order for calls made during a promise export's own
   replay be fixed for all promise exports (see "E-order")? Open.
6. Probe the forward re-encoding in "Found while reading". Open. Read on
   `6acdca02`: the encoder code and the spec text agree with the claim;
   nothing was run.
7. An A sent with `noPromisePipelining`: answered (review 2); rule 9 refuses
   to mint for it.
8. Is a retry of a failed `Resolve` send (out of memory) worth it? Open.
9. Are the two new in-use terms ("Ids stay in use") redundant now that main
   reserves the Finish record at admission? On `6acdca02` the ids of A and B
   are already in use until their Returns. Open. Recommendation: drop the two
   terms and the "two more lookups" row in "Work per frame", and keep a debug
   assertion at the mint and at step 1 that the id is still in use, plus
   T-j. That is fewer places to keep in step with another module's record
   policy.

The next review round runs on the implementation, as the effort line
plans.
