# Upgrading to capnp-zig v0.28.0

This guide is for projects that depend on capnp-zig. v0.28.0 is a native
C ABI release. Most of it comes from the asks of capnp-swift. It bundles
these changes:

- A native host can learn that the caller finished a call that the host
  still owes (capnp-swift handoff H13). The host opts in with
  `capnp_conn_opts.answer_finished`. Then the core queues
  `CAPNP_EFFECT_ANSWER_FINISHED`, and the host can answer with
  `capnp_return_canceled`. The first native feature bit,
  `CAPNP_FEATURE_ANSWER_FINISHED`, says that the core has it.
- Rules for the answers that a native host owes. A new call that reuses an
  id the host still owes closes the connection. The host owes at most
  `max_active_inbound_questions` answers. `capnp_finish` on a question with
  no RETURN yet returns `CAPNP_E_BUSY`, as `capnp_core.h` says.
- With u32_le framing (the QUIC path of capnp-swift), a frame that fails
  the connection sends one Abort, and a remote Abort is an orderly close
  again.
- Three fixes in the RPC Peer. A call pipelined on an answer whose results
  name a promise on another answer read freed memory. A promise export
  resolved into a cycle overflowed the stack. When a late exception Return
  after the caller's Finish could not be sent, the Peer reported success,
  and no Return went out. A native host reached all three through the C
  ABI. Other Peer users can reach them too.
- Repository work for contributors: the e2e runner records known limits as
  XFAIL, and the new e2e schema `pipelined_params_deferred` shows the
  H9-pending limit.

The Zig toolchain does not change: it stays at tagged 0.17.0. quic-zig and
the http3-zig pair do not change. Serialization and codegen do not change.
No Stable API line changes. The two Experimental API snapshots each gain 27
lines, change 1 line and lose 1 line. The changed line is the type of
`native.conn.Conn.pending_answers`. The lost line is
`native.conn.tail_call_reused_reason`. There are five Breaking entries, all
Experimental. The `0.28.0` section of [CHANGELOG.md](../CHANGELOG.md) is the
authoritative list of changes.

## Who should upgrade

- **Hosts of the native C ABI, such as capnp-swift.** All of the native
  changes above apply to you. The three Peer fixes apply through the C ABI:
  `capnp_return_results` with a `CAPNP_CAP_PROMISED` entry could read
  freed memory, `capnp_resolve_promise` of a promise to itself overflowed
  the stack on the next call to that promise, and `capnp_return_exception`
  after the remote's Finish could return `CAPNP_OK` and drop the answer. Two
  `capnp_core.h` comments are fixed: the 0.27.0 header failed to compile
  in a C host built with `-Wall -Werror`. The header spells
  `CAPNP_FEATURE_ANSWER_FINISHED` as `(1ULL << 0)`, so Swift imports it.
- **Zig code that uses the `native` module.** Read checklist items 6 and 7:
  `Conn.pending_answers` changes type, and a switch on a
  `native.conn.Effect` needs an arm for `.answer_finished`.
- **Every user of the RPC Peer.** The three Peer fixes apply on every
  transport. A Peer can read freed memory when its answer's results name a
  promise on one of its own questions (a `receiverAnswer` capability), and
  the remote pipelines a call on that capability. The Stable
  `Peer.resolvePromiseExportToExport` now refuses a cycle (checklist item
  8). A late exception answer after an early Finish now fails when its
  Return cannot be built or sent, for example when memory runs out.
- Contributors who work in this repository: read the e2e note in "How to
  verify".

This release has no security advisory.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.28.0` | `capnpc_zig-0.28.0-...` |
| quic-zig | `v0.38.0` (tag at `77be067`; no change since v0.25.0) | `quic-0.38.0-DnSYve_hPwBp5d2c8QUoZiNWi-rHT6QFR4pK2DueXEAC` |
| http3-zig (optional) | `v0.5.7` (tag at `e9867fc`; no change since v0.25.0) | `http3_zig-0.5.7-ayZ03AxnEwCS-38QeydGjOHQ1sAQEWT8xWjf82hAOPBw` |

capnp-zig v0.28.0 pins the same quic-zig release as v0.25.0, v0.26.0 and
v0.27.0, with the same dependency options, so http3-zig v0.5.7 still links
next to it with one quic module. That was measured with v0.25.0 at the
v0.5.7 tag, not again for v0.26.0, v0.27.0 or v0.28.0.

## What does not change

- The Stable API (`docs/api-snapshot.txt`) and the shape of generated code
  (`docs/generated-shape.txt`, `docs/generated-shape-experimental.txt`).
- Codegen: the codegen ABI stays 2, so files from the 0.23.0 to 0.27.0
  plugins compile against this runtime.
- The native C ABI version stays 1 (`CAPNP_CORE_ABI_VERSION`). Each
  function, struct field, constant and error code of the 0.27.0 header
  stays. The header only adds the feature bit, the `answer_finished` field
  at the end of `capnp_conn_opts`, effect kind 6 and
  `capnp_return_canceled`. A host that does not opt in never sees effect
  kind 6.
- The TCP, Unix socket and QUIC transports, the QUIC wire constants, and
  the WASM host ABI (`HostPeer`). The Peer fixes apply under each of them.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.28.0
   ```

2. **Native: do not reuse an id that the host still owes (Breaking,
   Experimental behavior).** After an INBOUND_CALL, the host owes one
   answer for its id. A remote that sends a new call with that id before it
   gets the Return breaks the protocol. Before, when the Peer's records of
   finished calls were full, the core gave the host a second INBOUND_CALL
   with the same id. Now the core closes the connection in each case: it
   sends Abort and queues CLOSE_REQUESTED. When the records were full, an
   exception Return (`DuplicateQuestionId`) for the new call comes before
   the Abort. `capnp_conn_push_bytes` returns `CAPNP_E_PROTOCOL`, and
   `capnp_conn_take_error` names `DuplicateQuestionId`. A call that the
   core kept queued reaches this check when the host releases it. Then
   `capnp_resolve_promise` or `capnp_return_results` returns `CAPNP_OK`,
   and the Return, the Abort and CLOSE_REQUESTED follow in the effects. A
   question that waits on a tail call to the old call now ends at transport
   close, with RETURN DISCONNECTED. `native.conn.tail_call_reused_reason`
   is removed.
   A host needs no change: do not use that constant. With full records, the
   core still does not catch a reused id on a Bootstrap, or on a Call that
   the core answers itself. "Answer ids" in [native-abi.md](native-abi.md)
   gives the rule and this limit.

3. **Native: answer every INBOUND_CALL (Breaking, Experimental
   behavior).** The host owes at most `max_active_inbound_questions`
   answers (default 4096). A call beyond that limit gets an exception
   Return (`OwedAnswerLimitExceeded`) and never reaches the host. The
   connection stays up. If your host must owe more answers at once, set
   `max_active_inbound_questions` higher.

4. **Native: `capnp_finish` on an in-flight question returns
   `CAPNP_E_BUSY` (Breaking, Experimental behavior).** It returned
   `CAPNP_E_INVAL` (-1). Now it returns `CAPNP_E_BUSY` (-3), before the Call
   reaches the callee and after. It still sends nothing and queues no
   effect. If your host checks for -1 here, check for -3. `capnp_finish`
   still works after the RETURN.

5. **Native: opt in to ANSWER_FINISHED if you want it.** This is additive.
   A host that does not opt in sees no change from it.
   1. Check `capnp_core_features() & CAPNP_FEATURE_ANSWER_FINISHED`.
   2. Set `capnp_conn_opts.answer_finished = 1`. The struct grows from 56
      to 60 bytes. 0 is off. Any other value makes `capnp_conn_new` return
      `CAPNP_E_INVAL`.
   3. When the remote sends Finish for an INBOUND_CALL that you owe, you
      get one `CAPNP_EFFECT_ANSWER_FINISHED`. Its `id` is the answer id.
      Find the work by that id, not by `export_id` or `host_tag`: the
      effect can come after the EXPORT_DROPPED of its export.
   4. Stop the work and answer once, normally with
      `capnp_return_canceled(conn, answer_id)`. You can also answer with
      `capnp_return_results` or `capnp_return_exception`.
   5. Apply effects in drain order. If you answered the call before you
      drained the effect, the effect is stale: ignore it.
      `capnp_return_canceled` returns `CAPNP_E_BAD_ID` for it.

   When the Peer's records of finished calls are full, no effect comes,
   and `capnp_return_canceled` returns `CAPNP_E_INVAL`: answer as usual.
   While a tail call waits on the call, the core withholds the effect.
   "Answer finished" in [native-abi.md](native-abi.md) gives every rule.

6. **Native Zig API: do not use `Conn.pending_answers` (Breaking,
   Experimental).** `native.conn.Conn.pending_answers` changes from
   `std.AutoHashMap(u32, bool)` to
   `std.AutoHashMap(u32, PendingAnswer)`. It is internal state of the shim,
   and the C ABI does not see it. Code that calls only `contains` or
   `count` compiles unchanged. Do not `remove` an entry: with
   `Options.answer_finished`, that leaks its reserved node. Answer with
   `Conn.returnResults`, `Conn.returnException` or `Conn.returnCanceled`.

7. **Native Zig API: add an `.answer_finished` arm (Breaking,
   Experimental).** `native.effects.Effect` and `native.effects.Kind` (also
   `native.conn.Effect`) gain `answer_finished`. A `switch` with no `else`
   arm does not compile until you add an arm for it. The core produces the
   effect only with `Options.answer_finished`.

8. **Peer: a promise export resolved into a cycle is refused.**
   `Peer.resolvePromiseExportToExport` (Stable) now fails with
   `error.PromiseResolutionCycle` when the export is the promise itself, or
   a promise export that already leads back to it. It sends nothing, and
   the promise stays unresolved, so you can still resolve or reject it.
   Before, a call on that promise overflowed the stack. Through the C ABI,
   `capnp_resolve_promise` returns `CAPNP_E_INVAL`. A long chain of
   resolved promise exports no longer overflows the stack either.

9. **Peer: a late exception answer can fail.** When the caller sends
   Finish before the answer, the Peer keeps a record until a Return goes
   out. If the late exception Return cannot be built or sent, the call now
   fails with that error, and the answer stays owed. Answer again, with an
   exception or with `Peer.sendReturnCanceled`. That answer is the one
   Return. Through the C ABI, `capnp_return_exception` returns the error
   code (for example `CAPNP_E_NOMEM`), and the host still owes the answer.

10. **Native, u32_le framing: one Abort, and the cause in `take_error`.**
    With `CAPNP_FRAMING_U32_LE`, a frame that fails the connection now
    sends one Abort, then CLOSE_REQUESTED, and `capnp_conn_take_error`
    names the cause. A remote Abort now makes `capnp_conn_push_bytes`
    return `CAPNP_E_CLOSED`, not `CAPNP_E_PROTOCOL`. The core sends no Abort
    back, and `take_error` names `RemoteAbort`. Segment-table framing
    already worked this way. If your host handles `CAPNP_E_PROTOCOL` for a
    remote Abort on u32_le, handle `CAPNP_E_CLOSED`.

## How to verify

- Build your project in each configuration that you use (`capnpc-zig`,
  `capnpc-zig-core`, and `.quic = true`), and run your tests.
- Native hosts: compile `capnp_core.h` with the warnings that your build
  uses. The package preflight compiles its C host with
  `-Wall -Wextra -Werror`.
- capnp-swift: the header changes, so expect your header snapshot gate to
  report the new declarations at the pin bump. If you opt in, check the
  feature bit first. The ledger in [downstreams.md](downstreams.md) has
  the H13 and H9-pending rows.
- Contributors who work in this repository: `zig build test` now runs the
  e2e outcome tests (also `zig build test-e2e-outcome`). The
  cross-implementation runner records the rows of
  `tools/e2e_known_limits.zig` as XFAIL. A listed check that passes is
  XPASS, and XPASS fails the gate like FAIL. When you fix a known limit,
  remove its rows and update the docs that the row names.
  [tests/e2e/README.md](../tests/e2e/README.md) gives the rules.

## What is new

Read the `0.28.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with the tests behind each fix.
