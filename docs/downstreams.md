# Downstream asks

This file is the ledger of what the downstream projects asked capnp-zig
for, and what capnp-zig did with each ask. It replaces the old capnp-deno
upgrade note, which named quic_zig 0.7.0 and was last changed on
2026-07-06.

- The owner relays all coordination as handoff text. Do not edit a
  downstream repository from capnp-zig work.
- Each downstream keeps its handoff files in its own repository. The
  "Handoff" column gives the path in that repository.
- A handoff from a downstream to the Zig fork or to quic-zig goes into
  [the upstream ledger](upstream/README.md), not here.
- Update this file in each release (see [RELEASING.md](../RELEASING.md)).
  "Checked" is the date when someone last read the row against both
  repositories.

Status words:

- **delivered in vX.Y.Z**: a capnp-zig tag ships it.
- **on main for vX.Y.Z**: it is on `main`, and the next tag ships it.
- **open**: capnp-zig accepts the ask and has not done it yet.
- **deferred**: capnp-zig does not do it now. The row says why and what
  the downstream uses until then.
- **declined**: capnp-zig does not do it, or the downstream withdrew it.
- **closed**: nothing more is needed. The row says why.
- **upstream**: the ask is for the Zig fork or quic-zig, and the upstream
  ledger tracks it.

## Consumers

| Consumer | Pins capnp-zig at | Where the pin is | Where its asks are | Checked |
|---|---|---|---|---|
| capnp-swift | v0.28.0 for the core (`core/build.zig.zon:16`) and the vendored e2e peer source (`third_party/capnp-zig/`, cloned at the Nightly `CAPNP_ZIG_TAG`, `.github/workflows/nightly.yml:33`). v0.23.0 for the generator (`tools/capnpc-swift/build.zig.zon:12`). v0.21.0 for the interop Zig peer, on purpose (`interop/zig-peer/build.zig.zon:12`). capnp-swift 0.1.4 (2026-10-20) and 0.2.0 (2026-10-21) ship the v0.28.0 core; 0.1.2 and 0.1.3 shipped v0.27.0. Read at tag 0.2.0 and main `fdecf9f`. | `core/build.zig.zon`, `tools/capnpc-swift/build.zig.zon`, `interop/zig-peer/build.zig.zon`, `third_party/capnp-zig/` | `docs/handoffs/` | 2026-10-21 |
| capnp-deno | `fe6634d` (main after v0.23.0; v0.24.0 is the first tag with it). Its main `eb885df` (2026-10-08) has not moved since. P0-1 and P1-1 of [sprint-plan-2026-10-21.md](sprint-plan-2026-10-21.md) affect this pin; see the relay text below. | the `vendor/capnp-zig` submodule and `tools/runtime-toolchain.json:3` | `docs/capnp_zig_additions.md`, `docs/CHANGELOG.md`, `docs/rpc_l3_wasm_abi_proposal.md` | 2026-10-21 |
| slcp-zig | v0.21.0 (`build.zig.zon:7`) on its local main `346d3ec` (2026-10-07, "build: move capnp-zig to v0.21.0"). That commit and its parent are not pushed: the published main on GitHub is `2524ee4` (2026-09-15), which pins v0.16.0. Neither has moved since. | `build.zig.zon` | `docs/upstream/` | 2026-10-21 |

## capnp-swift

| Ask | What it asks for | Handoff | Status | Notes | Checked |
|---|---|---|---|---|---|
| H1 | Let the core build for iOS: the `-Dfd-passing` option, the iOS gate and an iOS CI lane. | `docs/handoffs/H1-ios-gate-fix.md` | delivered in v0.21.0 | `-Dfd-passing`, `zig build check-ios` and the CI job `ios-check`. capnp-swift builds with `.@"fd-passing" = false`. | 2026-10-17 |
| H2 | Zig fork: `Io.Threaded` does not compile for iOS. | `docs/handoffs/handoff-zig-fork-ios-nullfile.md` | upstream | The same file is `docs/upstream/handoff-zig-fork-ios-nullfile.md`. See the [upstream ledger](upstream/README.md). | 2026-10-17 |
| H3 | Publish and freeze the QUIC baseline wire spec, export its constants, and fix the sentence that gave TCP a u32 length prefix. | `docs/handoffs/H3-quic-wire-spec-and-tcp-doc-fix.md`, `docs/handoffs/H3-quic-baseline-wire-spec.md` | delivered in v0.26.0 | v0.20.0 already exported `alpn` and `baseline_stream_id` (`quic_disabled.zig`). v0.26.0 adds "Wire constants (frozen)" to `docs/quic-transport.md` and fixes the TCP sentence. | 2026-10-17 |
| H4 | Give a persistent export's `deinit_ctx` the app ctx, once. | the H table in `docs/plan-2026-10-06.md` | delivered in v0.21.0 | `5fabb36`. | 2026-10-17 |
| H5 | Tell the host when the caller finishes an unanswered call, and reject a promise with a type. | `docs/handoffs/H5-answer-finished-hook.md` | delivered in v0.23.0 | `Peer.setAnswerFinishedHandler`, `Peer.sendReturnCanceled` and `Peer.resolvePromiseExportToExceptionTyped`. | 2026-10-17 |
| H6 | A public helper that clones a payload and remaps its caps from host tags. | the H table in `docs/plan-2026-10-06.md` | declined | capnp-swift withdrew it at M0: its own pointer walk is about 70 lines on the Stable API. | 2026-10-17 |
| H7 | Move the sans-IO shim into capnp-zig as `src/native/`. | `docs/handoffs/H7-native-move.md` | delivered in v0.23.0 | The `native` module and its C ABI ([native-abi.md](native-abi.md)). | 2026-10-17 |
| H8 | A transport close must end every question, also when memory runs out. | `docs/handoffs/H8-force-cancel-alloc-free.md` | delivered in v0.23.0 | | 2026-10-17 |
| H9 (answered) | Resolve `receiverAnswer` capabilities in call params for the host. | `docs/handoffs/H9-receiver-answer-params.md` | delivered in v0.23.0 | Only for an answer that has returned: the Peer resolves the entry just before the call dispatches, and fails the call with the answer's exception when that answer failed. It never delays a call. | 2026-10-17 |
| H9 (pending) | A local promise capability, and pipelined params that name a pending answer. | `docs/handoffs/H9-receiver-answer-params.md`, `docs/handoffs/H9-pending-answer-params.md` | deferred | Such an entry stays `.promised`. Large effort: it needs a design for local promise capabilities first. capnp-swift documents it as a limit ("native H9"). Evidence (H9-pending, capnp-swift M8): a C++ client fails `pipelined_params` against a native-ABI host every time, because the host answers every call after its dispatch. Shipped in v0.28.0: the e2e schema `pipelined_params_deferred` shows the limit. Its rows are the known limit `h9-pending-answer` in [`tools/e2e_known_limits.zig`](../tools/e2e_known_limits.zig): the C++, Go, Python and Rust clients each fail the same two `check()` lines, recorded as XFAIL. A fix turns them into XPASS, which fails the gate until the rows and docs change. `supported-surface.md` and `native-abi.md` have the native-host sentence. The row stays deferred. | 2026-10-21 |
| H10 | Park a queued call again when the answer's Return carries an unresolved promise export. | `docs/handoffs/H10-parked-call-promise-repark.md` | delivered in v0.23.0 | | 2026-10-17 |
| H11 | The 5-schema QUIC matrix: a `--transport quic` mode on the Zig e2e server and client. | `docs/handoffs/H11-quic-5-schema-matrix.md` | delivered in v0.26.0 | The CI job `e2e-quic` (Zig to Zig, all seven schemas) ships in v0.27.0. capnp-zig has no Swift cross-implementation test. | 2026-10-18 |
| H11 check 9 | End the RPC session when the peer ends QUIC stream 0, so `disconnectNow` ends the waiting questions. | `docs/handoffs/quic-server-close-residual.md` | delivered in v0.27.0 | `1fdc029`. A clean FIN and a RESET of stream 0 both end the session (owner decision D2, option A). Verified by capnp-swift: its QUIC matrix passes 10/10 pairings on 5 schemas, and 14/14 on 7 schemas since its M8, in CI. See the relay text below. | 2026-10-20 |
| H12 | Lookup-backed `type_resolver` contexts lose the nested RPC lexical scopes. | `docs/handoffs/H12-type-resolver-lexical-lookup.md` | deferred | capnp-swift uses `Context.init` with the full request nodes. The fix needs the same reverse lookup as direction #5, so it goes with that work. | 2026-10-17 |
| Tail calls | Follow a cap-free tail-call Return (`takeFromOtherQuestion`) through the native C ABI. | `docs/handoffs/tail-call-returns-for-native.md` | delivered in v0.27.0 | The hold lives in the native shim: a question waits while the host still owes the named call, and the host's answer becomes its one RETURN. Results that point at a capability still end it with RETURN EXCEPTION. Design and termination proof: `docs/notes/tail-call-hold-design.md`. The earlier Peer-level hold is on branch `archive/p1-3-tail-call-hold`. Verified by capnp-swift: C++ server and Swift client `resolve_disembargo` passes 9/9; the v0.23.0 core fails the same run with "unsupported Return variant". See the relay text below. | 2026-10-20 |
| Native feature bits | The native ABI v1 feature bits. `capnp_core_features()` reported 0. | none (sprint plan 2026-10-15, "Not in this sprint") | delivered in v0.28.0 | Bit 0 is H13's `CAPNP_FEATURE_ANSWER_FINISHED`; `capnp_core_features()` reports it. The header spells it `(1ULL << 0)`, so Swift imports it (`fc0533d`). The native bits are separate from the WASM host ABI's bits. | 2026-10-21 |
| H13 | Tell a native host when the caller finishes a call that the host still owes (`ANSWER_FINISHED`), and let the host answer it canceled. | `docs/handoffs/H13-answer-finished-native.md` | delivered in v0.28.0 | The shape sent to capnp-swift on 2026-10-20: feature bit 0 `CAPNP_FEATURE_ANSWER_FINISHED`; opt-in `uint32_t answer_finished` at offset 56 of `capnp_conn_opts` (60 bytes now); effect `CAPNP_EFFECT_ANSWER_FINISHED` (6), withheld while a tail-call hold waits on the call; `capnp_return_canceled(conn, answer_id)`. One correction to the answer text: when the Peer keeps no record of the Finish (records full, or no memory for the record), no effect comes and `capnp_return_canceled` returns `CAPNP_E_INVAL` (no effect was queued), not `CAPNP_E_BAD_ID`. Also on main: the Peer fix for a late exception answer under out-of-memory; `capnp_finish` on an in-flight question returns `CAPNP_E_BUSY`; two header comments are fixed; a new call that reaches a host export with an id the host owes closes the connection, and the host owes at most `max_active_inbound_questions` answers ("Answer ids" in `native-abi.md`); with u32_le framing (capnp-swift's QUIC path), a failed frame now sends one Abort and keeps its cause in `capnp_conn_take_error`, and a remote Abort returns `CAPNP_E_CLOSED` with no Abort back; two Peer crashes that a host reached through the C ABI: a call pipelined on an answer whose results hold a `CAPNP_CAP_PROMISED` entry (`386d20f`), and `capnp_resolve_promise` into a cycle, now `CAPNP_E_INVAL` (`58612dc`). The header changes break capnp-swift's header snapshot gate at the pin bump; that is expected. See the relay text below. Verified by capnp-swift 0.2.0 (2026-10-21): its runtime opts in by default (`cancelHandlersOnFinish = true`, `Sources/CapnpRPC/RPCConnection.swift:57`, `:169-170`), its `RemoteCancellationTests` (5) pass, and 3 ablations fail them (capnp-swift `docs/handoffs/H13-answer-finished-native.md`, status line and "capnp-swift adoption"). That default reaches P0-1 of [sprint-plan-2026-10-21.md](sprint-plan-2026-10-21.md): a call pipelined on a finished answer can fail. On 2026-10-22 the owner held the v0.28.1 patch (D1 changed to option B): the fix ships in v0.29.0, together with a fix in the call-forwarding path. Until capnp-swift pins v0.29.0, `cancelHandlersOnFinish = false` avoids the defect (see "Relay to capnp-swift now" below). | 2026-10-21 |
| ANSWER_FINISHED with pipelined calls | No ask: a capnp-zig defect in H13 that capnp-swift 0.2.0 hits by default. A Finish of a call that other calls are pipelined on (C++ sends Call(foo), Call(bar on foo), Finish(foo) for `foo().getSub().bar()`) queued `CAPNP_EFFECT_ANSWER_FINISHED`, and `capnp_return_canceled` then failed bar. | none (sprint plan 2026-10-21, P0-1) | open | Now no effect comes while a call that the caller has not finished is pipelined on the answer. It comes when the caller finishes the last such call, if the host still owes the answer; until then `capnp_return_canceled` returns `CAPNP_E_INVAL`. One case stays open with a go-capnp caller, cancel on or off: a call it finished, forwarded back to it, keeps its id in use, and the id's reuse closes the connection ("A call finished with go-capnp's Finish flag can close the connection" in [supported-surface.md](supported-surface.md)). capnp-swift's `cancelHandlersOnFinish = true` (`RPCConnection.swift:57`) turns the effect on. Until its pin bump, the mitigation is `cancelHandlersOnFinish = false`: the core then sends no ANSWER_FINISHED, and handlers run to the end. The pin bump to v0.29.0 changes `capnp_core.h` comments only, and that trips capnp-swift's header snapshot gate; update the snapshot. Not run against a Swift server (unverified in practice). See the relay text below. | 2026-10-21 |
| Answers after close | No ask: a capnp-zig gap in the native answer functions. After a protocol failure or a remote Abort, `capnp_return_results`, `capnp_return_exception` and `capnp_return_canceled` still queued a Return after the Abort, and a call pipelined on that answer reached the host as a new INBOUND_CALL after CLOSE_REQUESTED. | none (sprint plan 2026-10-21, P1-2) | open | Now, once the connection is closed (a protocol failure, a remote Abort, or `capnp_conn_transport_closed`), the three functions are no-ops: `CAPNP_OK` for an answer the host owes (nothing is sent, and the host owes it no more), `CAPNP_E_BAD_ID` for any other id. Two return codes change once closed: `capnp_return_canceled` gives `CAPNP_OK` where it gave `CAPNP_E_INVAL`, and after `capnp_conn_transport_closed` all three give `CAPNP_OK` where they gave `CAPNP_E_CLOSED`. A graceful `capnp_conn_shutdown` is no close: until `capnp_conn_transport_closed`, answers still go out and calls still reach the host. The P1-2 review read capnp-swift's `answerFinished` (`RPCConnection.swift:847-857`) as treating OK and BAD_ID as consumed. Not run against a Swift server (unverified in practice). The `capnp_core.h` changes are comments only. See the relay text below. | 2026-10-21 |
| Tail-call results with capabilities | Follow a tail call whose results point at a capability. Today the native hold ends that question with RETURN EXCEPTION. capnp-swift lists it as a known limit, not as an ask. | capnp-swift `docs/handoffs/tail-call-returns-for-native.md` (status line: "a documented limit, not hit by any lane") | deferred | The exception is `unimplemented`, `native.conn.tail_call_caps_reason` (`src/native/conn.zig:203`; `docs/native-abi.md:229-231`). The Peer cannot deliver those results locally either (`docs/notes/tail-call-hold-design.md:177-179`), so a fix needs a design first. No lane hits it. capnp-swift documents the limit in its `docs/stability.md:64` and `docs/release-readiness-2026-10-21.md` (`:52` at tag 0.2.0, `:70` at main `fdecf9f`). | 2026-10-21 |
| Cancel e2e scenario | A cross-implementation e2e scenario for H13: `slow()` cancelled, then `fast()`. With the C++ client it also covers id reuse. | `docs/handoffs/H13-answer-finished-native.md:164-166`; `:252-253` records "no e2e scenario yet" | open | P1-3 of [sprint-plan-2026-10-21.md](sprint-plan-2026-10-21.md), for v0.29.0. Scenario 1: cancel, then id reuse. Scenario 2: cancel with a call pipelined on the answer, the cross-implementation guard for P0-1. The first landing covers the reference clients against the Zig server. capnp-swift gets the schema (`tests/e2e/schemas/cancellation.capnp`) to run against its Swift server. | 2026-10-21 |

### Relay to capnp-swift now: the P0-1 workaround (2026-10-22)

The owner sends this text now. On 2026-10-22 the owner held the v0.28.1
patch, so the fix ships in v0.29.0.

> **Turn off cancel-on-Finish until you pin capnp-zig v0.29.0.** In
> v0.28.0 (and since H5 in v0.23.0) the core reports that the caller
> finished an answer even when the caller still has calls pipelined on
> that answer. capnp-swift 0.2.0 then cancels the handler and answers
> `capnp_return_canceled`, so every call pipelined on that answer fails.
> A C++ client makes this sequence for `foo().getSub().bar()` when it
> drops foo's promise but keeps bar's.
>
> The workaround is one setting: `cancelHandlersOnFinish = false`
> (`Sources/CapnpRPC/RPCConnection.swift:57`). With false, capnp-swift does
> not set `capnp_conn_opts.answer_finished` (`:169-170`), so the core sends
> no `CAPNP_EFFECT_ANSWER_FINISHED`, and each handler runs to the end and
> answers normally. That is the behaviour from before H13, and it is
> correct. One side effect: the same setting also controls the cancel of
> running handlers when the connection closes (`:1039-1041`), so with
> false those handlers also run to the end. Ship it as the default in a
> 0.2.1. Turn it on again after you pin v0.29.0.
>
> Why there is no v0.28.1: the fix for this case exposed an older defect in
> the call-forwarding path. With a go-capnp caller, a call pipelined on a
> finished answer can then be forwarded back and never finished, and the
> caller's later reuse of its id closes the connection. v0.29.0 fixes both
> together.

### Relay to capnp-deno now: the P0-1 workaround (2026-10-22)

The owner sends this text now.

> **Do not cancel on the answer-finished event until you pin capnp-zig
> v0.29.0.** Your pin `fe6634d` (and every capnp-zig release since
> v0.23.0) fires the answer-finished event (feature bit 12) even when the
> caller still has calls pipelined on that answer. Your `RpcServerBridge`
> answers every such event with `sendReturnCanceled`
> (`src/rpc/server/bridge.ts:1237-1262`, attached unconditionally at
> `:1218`), and that fails every call pipelined on the answer.
>
> The workaround: do not attach `#attachAnswerCancellation` (or make it a
> no-op), so the handler runs to the end and answers normally. That is
> the behaviour of a host without feature bit 12, and it is correct; it
> only wastes the work of a cancelled handler. Turn it on again after you
> pin v0.29.0.

### Relay drafts for v0.29.0

These are drafts, written before the fixes landed. P0-1, P1-1 and P1-2 of
[sprint-plan-2026-10-21.md](sprint-plan-2026-10-21.md) are not on main
yet. Before the owner sends a draft, check each sentence against the
commits and the CHANGELOG entries of those items, and add the test names.

### Relay to capnp-swift for v0.29.0 (draft)

The owner sends this text after the v0.29.0 tag exists.

> **ANSWER_FINISHED with pipelined calls (P0-1).** The core no longer
> reports the effect while a call that the caller still wants is pipelined
> on the answer, and `capnp_return_canceled` refuses in that case. You can
> set `cancelHandlersOnFinish = true` again.
>
> **Answers after a close (P1-2).** After the connection closes (a remote
> Abort, a protocol failure or a transport close),
> `capnp_return_results`, `capnp_return_exception` and
> `capnp_return_canceled` queue nothing.
>
> **A parked call under out-of-memory (`bdc656f`).** A call that waits on
> an answer or a promise export now gets an exception Return when the core
> cannot decode it again, instead of no Return.
>
> The `capnp_core.h` changes are comments only. Your header snapshot gate
> reports them at the pin bump; that is expected.
>
> **Cancel e2e scenario.** capnp-zig's e2e matrix has a cancellation
> schema (`tests/e2e/schemas/cancellation.capnp`): cancel, then id reuse,
> and cancel with a pipelined call. The reference clients run it against
> the Zig server. Run it against your Swift server.

### Relay to capnp-swift for v0.28.0

The owner sends this text after the v0.28.0 tag exists.

> **ANSWER_FINISHED (H13).** capnp-zig v0.28.0 ships the shape that we
> sent on 2026-10-20, with one correction. Check
> `capnp_core_features() & CAPNP_FEATURE_ANSWER_FINISHED` (bit 0). The
> header spells it `(1ULL << 0)`, so Swift imports it. Set
> `capnp_conn_opts.answer_finished = 1` (offset 56; the struct is now 60
> bytes). The core then queues one `CAPNP_EFFECT_ANSWER_FINISHED` (6) when
> the remote finishes an INBOUND_CALL that you owe. Find the work by its
> answer id: the effect can come after the EXPORT_DROPPED of its export.
> Answer once, normally with `capnp_return_canceled(conn, answer_id)`. The
> correction: when the Peer keeps no record of the Finish (its records are
> full, or the record gets no memory), no effect comes, and
> `capnp_return_canceled` returns `CAPNP_E_INVAL`, not `CAPNP_E_BAD_ID`.
> The reserved node keeps the effect safe from out-of-memory only once
> that record exists. "Answer finished" in docs/native-abi.md has every
> rule.
>
> **Other native changes.** `capnp_finish` on a question with no RETURN
> yet returns `CAPNP_E_BUSY` (-3), not `CAPNP_E_INVAL` (-1). A new call
> that reaches one of your exports with an id that you owe closes the
> connection, and you owe at most `max_active_inbound_questions` answers
> ("Answer ids" in docs/native-abi.md gives the rule and its limit). With
> u32_le framing, a failed frame sends one Abort and keeps its cause in
> `capnp_conn_take_error`. A remote Abort returns
> `CAPNP_E_CLOSED`, with no Abort back. v0.28.0 fixes two Peer crashes
> that a host could reach: a call pipelined on an answer whose results hold
> a `CAPNP_CAP_PROMISED` entry, and `capnp_resolve_promise` into a cycle
> (now `CAPNP_E_INVAL`). After the remote's Finish, a
> `capnp_return_exception` that cannot be sent now returns its error, and
> you still owe the answer. The header compiles with `-Wall -Werror`. Your
> header snapshot gate reports the new declarations at the pin bump; that
> is expected. docs/upgrading-to-0.28.0.md has a checklist.
>
> **H9-pending.** The e2e schema `pipelined_params_deferred` shows the
> limit. The C++, Go, Python and Rust clients each fail the same two
> `check()` lines against it, and the runner records those rows as XFAIL
> (`h9-pending-answer`). The ask stays deferred.

### Relay to capnp-swift for v0.27.0

The owner sends this text after the v0.27.0 tag exists.

> **QUIC stream 0 (H11 check 9).** capnp-zig v0.27.0 (`1fdc029`) ends the
> RPC session when stream 0 ends, in baseline and native mode, for
> `connect`, `serve` and `EmbeddedSession`. After a clean FIN, capnp-zig
> dispatches every whole frame that came before it. Then it closes with
> code 0 (normal) and cause `peer_close`. Waiting questions end
> `Disconnected` in less than 1 s. A partial frame at the FIN closes with
> `frame_error` (0x434e5001). A RESET_STREAM closes at once, with code 0 and
> cause `transport_error`. The ALPN stays `capnp-rpc/1`.
> Send every frame that you want delivered before your FIN. Do not send a
> FIN while you still expect replies. After you pin v0.27.0, run H11 check
> 9 again. capnp-zig CI now runs the Zig e2e over QUIC
> (`zig build -Dquic=true e2e-quic`). No Swift cross-implementation test
> exists in capnp-zig yet.
>
> **Tail calls for native.** In v0.27.0 the native shim follows a tail
> call to a call that your host still owes. When the remote answers a
> question with `takeFromOtherQuestion` naming an INBOUND_CALL that you have
> not answered, the question waits. Your `capnp_return_results` for that
> call becomes the question's RETURN RESULTS, and your
> `capnp_return_exception` becomes its RETURN EXCEPTION. Results that point
> at a capability give RETURN EXCEPTION (type `unimplemented`, reason
> "capnp-swift core: tail call results carry capabilities"). While it
> waits, a cancel, its deadline, a transport close or the shutdown drain
> bound (`shutdown_drain_timeout_ms`) ends it once. A graceful shutdown
> waits for it, as for any open question. docs/native-abi.md has the
> rules. After you pin v0.27.0, run the C++ `resolve_disembargo` case
> again and drop your workaround if it passes. Two more fixes are in
> v0.27.0: a failing `yourself` call now answers `resultsSentElsewhere`,
> and a kept tail-call answer can no longer reach a later question with a
> reused id.

## capnp-deno

| Ask | What it asks for | Handoff | Status | Notes | Checked |
|---|---|---|---|---|---|
| Feature bit 10 | Level-3 handoff origination over the WASM host ABI. | `docs/rpc_l3_wasm_abi_proposal.md` | delivered in v0.22.0 | capnp-deno uses it (`@nullstyle/capnp/advanced`). | 2026-10-17 |
| Feature bit 11 | Level-3 vat hosting over the WASM host ABI. | `docs/CHANGELOG.md` ("The VatC gap is closed") | delivered in v0.23.0 | capnp-deno uses it. | 2026-10-17 |
| Feature bit 12 | Host answer cancellation over the WASM host ABI: the answer-finished event and `Return{canceled}`. | `docs/CHANGELOG.md` ("Host answer cancellation") | delivered in v0.24.0 | Its pin `fe6634d` has it, and its artifact advertises bits 0-12. | 2026-10-17 |
| Answer-finished with pipelined calls | No ask: a capnp-zig defect in feature bit 12. A Finish of a call that other calls are pipelined on queued the kind-4 event, and `capnp_peer_send_return_canceled` then failed those calls. | none (sprint plan 2026-10-21, P0-1) | open | Now no kind-4 event comes while a call that the caller has not finished is pipelined on the answer. It comes when the caller finishes the last such call, if the host still owes the answer; until then `capnp_peer_send_return_canceled` refuses the answer (`AnswerHasPipelinedCalls`) and sends nothing. capnp-deno's RpcServerBridge (`640d8a4`, `src/rpc/server/bridge.ts:1237-1262`) answers every kind-4 event with `sendReturnCanceled`, so its pin `fe6634d` has the defect. The fix stops the cancel, but bar still fails through `HostPeer`: a call pipelined on a host call that the host answers with `capnp_peer_respond_host_call_return_frame` gets "host call failed" ("HostPeer fails a call that is pipelined on an unanswered host call" in [supported-surface.md](supported-surface.md); not fixed yet). P1-1 of the sprint plan fixes that limit. At `fe6634d`, the mitigation is to not answer canceled on answer-finished events: let the handler's answer go out. | 2026-10-21 |
| Feature bit 10 follow-ups | The WASM pickup-handler export, `sendProvide` with a retained-answer or promised-answer target, and `sendThirdPartyAnswer` with a caller-chosen id. | `docs/rpc_l3_wasm_abi_proposal.md` ("Implementation deltas") | deferred | capnp-deno deferred them itself. It uses the host-driven Accept path, `importedCap` targets and auto-allocated answer ids. The two `sendProvide` target forms need no new feature bit. | 2026-10-17 |
| Upgrade note | The L2, L3 and L4 upgrade guidance for capnp-deno (this repository's old upgrade note, last changed in `31a2ec3`). | none | closed | capnp-deno has moved past it (v0.21.0, v0.22.0, v0.23.0, then `fe6634d`). The tier rules it gave are in [stability.md](stability.md). The note is deleted. | 2026-10-17 |

capnp-deno has no open ask. Its deferred feature-bit-10 follow-ups are in
the table.

### Relay to capnp-deno for v0.29.0 (draft)

The owner sends this text after the v0.29.0 tag exists. See "Relay drafts
for v0.29.0" above: P0-1 and P1-1 are not on main yet.

> Your pin `fe6634d` has two answer-path defects. Move the pin to v0.29.0,
> which fixes both.
>
> **Answer-finished with pipelined calls (P0-1, fixed in v0.29.0).** The
> answer-finished event (feature bit 12) fires even when calls are
> pipelined on the answer. Your `RpcServerBridge` answers every such event
> with `sendReturnCanceled` (`src/rpc/server/bridge.ts:1237-1262`), and
> that fails every call pipelined on the answer. After the fix, the event
> does not fire in that case, and `sendReturnCanceled` refuses while calls
> are pipelined on the answer. Until you move the pin, keep the workaround from "Relay to capnp-deno
> now": do not answer canceled on an answer-finished event.
>
> **Exception answers from `respondHostCallException` (P1-1, fixed in
> v0.29.0).** A call pipelined on a host call that you answer with
> `respondHostCallException` gets no Return until the connection drops. A
> call pipelined after the exception finds no failed-answer record. A
> `sendResultsTo = yourself` call answered with an exception puts an
> exception Return on the wire, and the C++ reference then drops the
> connection. Your throwing handlers use this path
> (`src/rpc/server/bridge.ts:1600-1771`). Only the pin move delivers this
> fix.
>
> From `fe6634d` the move crosses v0.24.0 to v0.29.0. Read their
> Breaking entries in `docs/upgrading-to-0.24.0.md` through
> `docs/upgrading-to-0.29.0.md`.

## slcp-zig

| Ask | What it asks for | Handoff | Status | Notes | Checked |
|---|---|---|---|---|---|
| 01 | Promote the schema-free `canonical` module to the Stable snapshot. | `docs/upstream/01-stabilize-canonical-module.md` | delivered in v0.14.0 | | 2026-10-17 |
| 02 | A direct `MessageBuilder` to canonical-bytes path. | `docs/upstream/02-canonicalize-from-builder.md` | delivered in v0.14.0 | | 2026-10-17 |
| 03 | A validating decode for flat single-segment bytes. | `docs/upstream/03-flat-validating-decode.md` | delivered in v0.14.0 | `Message.initFlat`. | 2026-10-17 |
| 04 | Feedback on `docs/wasm_host_abi.md`: re-entrancy, the import-bearing variant, an ownership table, allocation tracking, capnp-specific labels, the build recipe, one export prefix. | `docs/upstream/04-wasm-host-abi-feedback.md` | delivered in v0.17.0 | Asks 1 to 6 are in [wasm_host_abi.md](wasm_host_abi.md), and `wasm-host` builds ReleaseSmall when the build is Debug. Ask 7 (one export prefix) is declined: the v1 export names stay. | 2026-10-17 |
| 05 | Port the TCP transport to the new `std.Io` net surface, and publish framing fixtures. | `docs/upstream/05-tcp-transport-stdio-port.md` | delivered in v0.16.0 | Also `Transport.readTimeout`, so a deadline read needs no `SO_RCVTIMEO`. | 2026-10-17 |
| 06 | `check-api` must not come from the build cache, and generated code must be `zig fmt` clean. | `docs/upstream/06-check-api-cache-and-fmt-clean-gen.md` | delivered in v0.19.0 | The freeze gates have `has_side_effects`. The plugin writes `zig fmt` clean code, and `just check-generated` checks it. | 2026-10-17 |
| 07 F5 | A pinnable plugin artifact, so the plugin cannot drift from the pinned package. | `docs/upstream/07-consumer-pinning-report.md` | delivered in v0.19.0 | `dep.artifact("capnpc-zig")`. | 2026-10-17 |
| 07 F6 | Explicit error sets on the builder API. | `docs/upstream/07-consumer-pinning-report.md` | delivered in v0.19.0 | `message.BuildError` and `message.CopyError`. | 2026-10-17 |

slcp-zig has no open ask. Its local main pins v0.21.0 and its published
main pins v0.16.0, so it has not taken the releases after v0.21.0.

### Relay to slcp-zig (draft)

The owner sends this text after the v0.29.0 tag exists.

> v0.29.0 fixes RPC answer paths in the Peer, the native C ABI,
> the WASM host ABI and `HostPeer`. slcp-zig uses serialization and
> codegen (`capnpc-zig-core`), `canonical`, `rpc.wire.framing` and
> generated RPC bindings. Its code calls none of the changed functions,
> and no ABI that it uses changes. Your local main (`346d3ec`) pins
> v0.21.0, but it is not pushed: the published main (`2524ee4`) pins
> v0.16.0. Move the pin to v0.29.0. From v0.21.0 the move crosses v0.22.0 to that tag: read the
> Breaking entries in `docs/upgrading-to-0.22.0.md` and the later upgrade
> guides. From v0.16.0 it also crosses v0.17.0 to v0.21.0 (the CHANGELOG,
> and the upgrade guides from v0.19.0). Then regenerate the checked-in
> bindings (`src/gen/`) with the plugin of the new pin.
