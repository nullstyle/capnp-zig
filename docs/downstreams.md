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
| capnp-swift | v0.27.0 for the core and the vendored e2e peer source (`third_party/capnp-zig/`, the Nightly `CAPNP_ZIG_TAG`). v0.23.0 for the generator. v0.21.0 for the interop Zig peer, on purpose. capnp-swift 0.1.2 (2026-10-18) and 0.1.3 (2026-10-20) ship the v0.27.0 core. | `core/build.zig.zon`, `tools/capnpc-swift/build.zig.zon`, `interop/zig-peer/build.zig.zon`, `third_party/capnp-zig/` | `docs/handoffs/` | 2026-10-20 |
| capnp-deno | `fe6634d` (main after v0.23.0; v0.24.0 is the first tag with it) | the `vendor/capnp-zig` submodule and `tools/runtime-toolchain.json` | `docs/capnp_zig_additions.md`, `docs/CHANGELOG.md`, `docs/rpc_l3_wasm_abi_proposal.md` | 2026-10-17 |
| slcp-zig | v0.21.0 | `build.zig.zon` | `docs/upstream/` | 2026-10-17 |

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
| H13 | Tell a native host when the caller finishes a call that the host still owes (`ANSWER_FINISHED`), and let the host answer it canceled. | `docs/handoffs/H13-answer-finished-native.md` | delivered in v0.28.0 | The shape sent to capnp-swift on 2026-10-20: feature bit 0 `CAPNP_FEATURE_ANSWER_FINISHED`; opt-in `uint32_t answer_finished` at offset 56 of `capnp_conn_opts` (60 bytes now); effect `CAPNP_EFFECT_ANSWER_FINISHED` (6), withheld while a tail-call hold waits on the call; `capnp_return_canceled(conn, answer_id)`. One correction to the answer text: when the Peer keeps no record of the Finish (records full, or no memory for the record), no effect comes and `capnp_return_canceled` returns `CAPNP_E_INVAL` (no effect was queued), not `CAPNP_E_BAD_ID`. Also on main: the Peer fix for a late exception answer under out-of-memory; `capnp_finish` on an in-flight question returns `CAPNP_E_BUSY`; two header comments are fixed; a new call that reaches a host export with an id the host owes closes the connection, and the host owes at most `max_active_inbound_questions` answers ("Answer ids" in `native-abi.md`); with u32_le framing (capnp-swift's QUIC path), a failed frame now sends one Abort and keeps its cause in `capnp_conn_take_error`, and a remote Abort returns `CAPNP_E_CLOSED` with no Abort back; two Peer crashes that a host reached through the C ABI: a call pipelined on an answer whose results hold a `CAPNP_CAP_PROMISED` entry (`386d20f`), and `capnp_resolve_promise` into a cycle, now `CAPNP_E_INVAL` (`58612dc`). The header changes break capnp-swift's header snapshot gate at the pin bump; that is expected. See the relay text below. | 2026-10-21 |
| ANSWER_FINISHED with pipelined calls | No ask: a capnp-zig defect in H13 that capnp-swift 0.2.0 hits by default. A Finish of a call that other calls are pipelined on (C++ sends Call(foo), Call(bar on foo), Finish(foo) for `foo().getSub().bar()`) queued `CAPNP_EFFECT_ANSWER_FINISHED`, and `capnp_return_canceled` then failed bar. | none (sprint plan 2026-10-21, P0-1) | on main for v0.28.1 | Now no effect comes while calls are pipelined on the answer, and none comes later when they go; `capnp_return_canceled` returns `CAPNP_E_INVAL` for such an answer. capnp-swift's `cancelHandlersOnFinish = true` (`RPCConnection.swift:57`) turns the effect on. Until its pin bump, the mitigation is `cancelHandlersOnFinish = false`: the core then sends no ANSWER_FINISHED, and handlers run to the end. The pin bump to v0.28.1 changes `capnp_core.h` comments only, and that trips capnp-swift's header snapshot gate; update the snapshot. Not run against a Swift server (unverified in practice). See the relay text below. | 2026-10-21 |
| Answers after close | No ask: a capnp-zig gap in the native answer functions. After a protocol failure or a remote Abort, `capnp_return_results`, `capnp_return_exception` and `capnp_return_canceled` still queued a Return after the Abort, and a call pipelined on that answer reached the host as a new INBOUND_CALL after CLOSE_REQUESTED. | none (sprint plan 2026-10-21, P1-2) | on main for v0.28.1 | Now, once the connection is closed (a protocol failure, a remote Abort, or `capnp_conn_transport_closed`), the three functions are no-ops: `CAPNP_OK` for an answer the host owes (nothing is sent, and the host owes it no more), `CAPNP_E_BAD_ID` for any other id. Two return codes change once closed: `capnp_return_canceled` gives `CAPNP_OK` where it gave `CAPNP_E_INVAL`, and after `capnp_conn_transport_closed` all three give `CAPNP_OK` where they gave `CAPNP_E_CLOSED`. A graceful `capnp_conn_shutdown` is no close: until `capnp_conn_transport_closed`, answers still go out and calls still reach the host. The P1-2 review read capnp-swift's `answerFinished` (`RPCConnection.swift:847-857`) as treating OK and BAD_ID as consumed. Not run against a Swift server (unverified in practice). The `capnp_core.h` changes are comments only. See the relay text below. | 2026-10-21 |

### Relay to capnp-swift for v0.28.1

The owner sends this text after the v0.28.1 tag exists.

> **ANSWER_FINISHED and pipelined calls.** In v0.28.0, a Finish of a call
> that other calls were pipelined on still queued
> `CAPNP_EFFECT_ANSWER_FINISHED`, and `capnp_return_canceled` then failed
> each pipelined call. The C++ reference makes this sequence for
> `foo().getSub().bar()` when the caller drops foo's promise: Call(foo),
> Call(bar on foo), Finish(foo). rpc.capnp lets the callee cancel only when
> nothing else cares about the result, so bar must still run. capnp-swift
> 0.2.0 hits this by default: `cancelHandlersOnFinish = true` sets
> `answer_finished`, and `answerFinished` calls `capnp_return_canceled`. In
> v0.28.1 no effect comes while calls are pipelined on the answer, and none
> comes later when they go: your handler's ordinary answer settles the
> call. `capnp_return_canceled` returns `CAPNP_E_INVAL` for such an
> answer, as for any answer without the effect. A call pipelined on an
> answer after its Finish breaks the protocol. The core now answers such a
> call at once with an exception RETURN, as the C++ reference does, so
> after the effect `capnp_return_canceled` still answers. Bump the pin to
> v0.28.1. Its `capnp_core.h`
> changes are comments only, but they trip your header snapshot gate:
> update the snapshot. Until the bump, set `cancelHandlersOnFinish =
> false`: the core then sends no ANSWER_FINISHED, and handlers run to the
> end. With v0.28.1, cancel on by default is safe again. We did not run
> this against a Swift server.
>
> **Answers after close.** Once a connection is closed,
> `capnp_return_results`, `capnp_return_exception` and
> `capnp_return_canceled` are no-ops. Closed means a protocol failure
> (`capnp_conn_push_bytes` returned `CAPNP_E_PROTOCOL`), a remote Abort
> (it returned `CAPNP_E_CLOSED`), or `capnp_conn_transport_closed`. For an
> answer you owe, each one returns `CAPNP_OK` and sends nothing, and you
> owe that answer no more. For any other id, each one returns
> `CAPNP_E_BAD_ID`. The argument checks still apply: a NULL or empty msg,
> or a malformed caps[] entry, is `CAPNP_E_INVAL`, and you still owe the
> answer. Two return codes change once closed. `capnp_return_canceled`
> returns `CAPNP_OK` where it returned `CAPNP_E_INVAL`. After
> `capnp_conn_transport_closed`, all three return `CAPNP_OK` where they
> returned `CAPNP_E_CLOSED`, so check any code that branches on
> `CAPNP_E_CLOSED` from an answer. After a protocol failure or a remote
> Abort, no call pipelined on the answer reaches you. Before, a late
> answer queued a Return after the Abort and gave you the pipelined call
> as a new INBOUND_CALL. A question held on a tail call to that answer no
> longer gets your late answer. It ends with RETURN DISCONNECTED at
> `capnp_conn_transport_closed`, or earlier on a `capnp_conn_tick`: RETURN
> EXCEPTION at its deadline, RETURN DISCONNECTED at the shutdown drain
> bound. A graceful `capnp_conn_shutdown` is no close. After its
> CLOSE_REQUESTED and until `capnp_conn_transport_closed`, an answer still
> sends its Return, a call pipelined on it still reaches you, and
> `capnp_return_canceled` still needs an ANSWER_FINISHED. To close that
> window, call `capnp_conn_transport_closed` when you act on
> CLOSE_REQUESTED. "After close" in docs/native-abi.md has the rule.

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
| Answer-finished with pipelined calls | No ask: a capnp-zig defect in feature bit 12. A Finish of a call that other calls are pipelined on queued the kind-4 event, and `capnp_peer_send_return_canceled` then failed those calls. | none (sprint plan 2026-10-21, P0-1) | on main for v0.28.1 | Now no kind-4 event comes while calls are pipelined on the answer, and none comes later when they go; `capnp_peer_send_return_canceled` refuses such an answer (`AnswerHasPipelinedCalls`) and sends nothing. capnp-deno's RpcServerBridge (`640d8a4`, `src/rpc/server/bridge.ts:1237-1262`) answers every kind-4 event with `sendReturnCanceled`, so its pin `fe6634d` has the defect. v0.28.1 stops the cancel, but bar still fails through `HostPeer`: a call pipelined on a host call that the host answers with `capnp_peer_respond_host_call_return_frame` gets "host call failed" ("HostPeer fails a call that is pipelined on an unanswered host call" in [supported-surface.md](supported-surface.md); not fixed yet). So the relay below is held until a tag also fixes that limit. At `fe6634d`, the mitigation is to not answer canceled on answer-finished events: let the handler's answer go out. | 2026-10-21 |
| Feature bit 10 follow-ups | The WASM pickup-handler export, `sendProvide` with a retained-answer or promised-answer target, and `sendThirdPartyAnswer` with a caller-chosen id. | `docs/rpc_l3_wasm_abi_proposal.md` ("Implementation deltas") | deferred | capnp-deno deferred them itself. It uses the host-driven Accept path, `importedCap` targets and auto-allocated answer ids. The two `sendProvide` target forms need no new feature bit. | 2026-10-17 |
| Upgrade note | The L2, L3 and L4 upgrade guidance for capnp-deno (this repository's old upgrade note, last changed in `31a2ec3`). | none | closed | capnp-deno has moved past it (v0.21.0, v0.22.0, v0.23.0, then `fe6634d`). The tier rules it gave are in [stability.md](stability.md). The note is deleted. | 2026-10-17 |

capnp-deno has no open ask. Its deferred feature-bit-10 follow-ups are in
the table.

### Relay to capnp-deno for v0.28.1 (held)

Do not send this text yet. Through `HostPeer`, a call pipelined on a host
call that the host answers with `capnp_peer_respond_host_call_return_frame`
still fails ("host call failed"; see the known limit in
[supported-surface.md](supported-surface.md)). So moving the pin to
v0.28.1 alone does not let bar run. Send the text once a tag also fixes
that limit, and put that tag in place of `vX.Y.Z` in it.

> **Answer-finished and pipelined calls (feature bit 12).** Up to
> v0.28.0, a Finish of a host call that other calls were pipelined on
> still queued the kind-4 answer-finished event. RpcServerBridge answers
> every such event with `sendReturnCanceled`, and that failed each
> pipelined call. The C++ reference makes this sequence for
> `foo().getSub().bar()` when the caller drops foo's promise: Call(foo),
> Call(bar on foo), Finish(foo). rpc.capnp lets the callee cancel only when
> nothing else cares about the result, so bar must still run. capnp-zig
> v0.28.1 sends no kind-4 event while calls are pipelined on the answer,
> and none later when they go: the handler's ordinary answer settles the
> call. `capnp_peer_send_return_canceled` refuses such an answer with
> `AnswerHasPipelinedCalls` and sends nothing; after that refusal, answer
> the call as usual. A call pipelined on an answer after its Finish breaks
> the protocol. The peer now answers such a call at once with an exception
> Return, as the C++ reference does, so after a kind-4 event
> `capnp_peer_send_return_canceled` still answers. Your pin `fe6634d` has
> the defect. Move the pin to `vX.Y.Z` or later. Until then, do not answer
> canceled on answer-finished events: let the handler's answer go out.

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

slcp-zig has no open ask. It pins v0.21.0, so it has not taken the
releases after it.
