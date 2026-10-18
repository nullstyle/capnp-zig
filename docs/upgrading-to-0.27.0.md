# Upgrading to capnp-zig v0.27.0

This guide is for projects that depend on capnp-zig. v0.27.0 is an RPC
tail-call and reliability release. It bundles these changes:

- Fixes for tail calls in the RPC Peer. A tail call is a Return with
  `takeFromOtherQuestion`. It names a call that the remote sent to this
  peer with `sendResultsTo = yourself`. A tail call could get the kept
  answer of an earlier call with the same id. A `yourself` call that failed
  put an exception Return on the wire, and the C++ reference drops the
  connection for that. Now the Peer and the native C ABI answer such a call
  with `resultsSentElsewhere`. `HostPeer` (the WASM host ABI) still sends
  the exception.
- The native C ABI follows a tail call to a call that the host still owes,
  when the results carry no capability. Before, the question ended at once
  with RETURN EXCEPTION.
- On QUIC, the end of stream 0 ends the session, in both modes. A clean FIN
  ends it after the whole frames before it. In the owned loops, a RESET now
  ends it too. This is one of the two Breaking entries.
- Fixes for Windows TCP. The Stable `Listener.close` no longer aborts the
  process while a thread waits in accept. `WorkerPool` shutdown no longer
  dials its own listener. A read that the local network stack ended now
  fails with `error.ConnectionResetByPeer`, not `error.Unexpected`.
- A QUIC `WarmRedialClient` that is stopped during generation setup now
  counts that generation.
- Repository work for contributors: CI runs the Zig e2e over QUIC, and
  `just release-tag` needs 3 Nightly runs on the release commit.

The Zig toolchain does not change: it stays at tagged 0.17.0. quic-zig and
the http3-zig pair do not change. Serialization and codegen do not change.
No Stable API line changes. The two Experimental API snapshots each gain 9
lines and change 1 line: the type of `native.conn.Conn.pending_answers`.
There are two Breaking entries, both Experimental. The `0.27.0` section of
[CHANGELOG.md](../CHANGELOG.md) is the authoritative list of changes.

## Who should upgrade

- **Every project whose Peer talks to the C++ reference.** The C++
  reference sends calls with `sendResultsTo = yourself` to this peer when
  it makes a tail call into it, for example when it forwards a parked call
  back (`resolve_disembargo`). It reuses the lowest free question id. So on
  v0.26.0 a later tail call with a reused id could get the old answer as
  its results. A `yourself` call that failed made the C++ reference drop
  the connection.
- **Hosts of the native C ABI (capnp-swift).** A tail call to a call that
  the host has not answered now waits for the host's answer. The two
  fixes above apply through the C ABI too.
- **Windows users of the TCP transport.** On v0.26.0, `Listener.close`
  aborted the process when another thread waited in `Listener.accept`,
  `Listener.acceptFd` or `ServerSession.accept`: a panic in Debug and
  ReleaseSafe, undefined behavior in the other modes. `WorkerPool`
  shutdown retried its wake dials with no bound, so a host with no free
  ephemeral port held shutdown up.
- **QUIC users.** Read the stream-0 rule below. A question that waits when
  the peer ends stream 0 now ends `Disconnected` at once, not
  `CallTimedOut` at its deadline. `WarmRedialClient` users get the right
  `Outcome` after a stop during setup.
- Linux and macOS users of the TCP transport get no transport change. The
  Peer fixes above apply on every OS.

This release has no security advisory.

## The coordinated set

| Component | Version | Pin |
|---|---|---|
| Zig | `0.17.0` (tagged; no change since v0.19.0) | `mise.toml`: `zig = "0.17.0"`; `build.zig.zon`: `.minimum_zig_version = "0.17.0"` |
| capnp-zig | `v0.27.0` | `capnpc_zig-0.27.0-...` |
| quic-zig | `v0.38.0` (tag at `77be067`; no change since v0.25.0) | `quic-0.38.0-DnSYve_hPwBp5d2c8QUoZiNWi-rHT6QFR4pK2DueXEAC` |
| http3-zig (optional) | `v0.5.7` (tag at `e9867fc`; no change since v0.25.0) | `http3_zig-0.5.7-ayZ03AxnEwCS-38QeydGjOHQ1sAQEWT8xWjf82hAOPBw` |

capnp-zig v0.27.0 pins the same quic-zig release as v0.25.0 and v0.26.0,
with the same dependency options, so http3-zig v0.5.7 still links next to
it with one quic module. That was measured with v0.25.0 at the v0.5.7 tag,
not again for v0.26.0 or v0.27.0.

## What does not change

- The Stable API (`docs/api-snapshot.txt`) and the shape of generated code
  (`docs/generated-shape.txt`, `docs/generated-shape-experimental.txt`).
- Codegen: the codegen ABI stays 2, so files from the 0.23.0 to 0.26.0
  plugins compile against this runtime.
- The functions, types and constants of the native C ABI, and its header,
  `src/native/include/capnp_core.h`. Its tail-call behavior changes: see
  checklist item 5.
- The QUIC ALPN (`capnp-rpc/1`), the stream id, the framing, the
  zero-length and oversize rules, and the close codes.
- The TCP transport on Linux and macOS.

## Checklist for every consumer

1. **Bump the pin.**

   ```sh
   zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.27.0
   ```

2. **QUIC: do not send a FIN on stream 0 while you expect replies
   (Breaking, Experimental behavior).** A clean FIN on stream 0 now ends
   the session, in baseline and native mode. This applies to `connect`,
   `serve`, the owned `Connection` and `Server`, and `EmbeddedSession`. The
   receiver first dispatches every whole frame that came before the FIN.
   Then it closes with close code `normal` and close cause `peer_close`. A
   partial frame at the FIN is a `frame_error` close. A peer that sends its
   FIN and still waits for replies loses the session, and its replies
   never come. Send every frame that you want delivered before the FIN.
   capnp-zig never sends a FIN on stream 0, so two capnp-zig peers need no
   change. "Wire constants (frozen)" in
   [quic-transport.md](quic-transport.md) gives the rule and the reasons.

3. **QUIC: a RESET of stream 0 now ends the session in the owned loops.**
   `Connection` and `Server` close at once with close code `normal` and
   close cause `transport_error`, as `EmbeddedSession` already did.

4. **Native: do not use `Conn.pending_answers` (Breaking, Experimental).**
   `native.conn.Conn.pending_answers` changes from
   `std.AutoHashMap(u32, void)` to `std.AutoHashMap(u32, bool)`. The value
   is true for a call with `sendResultsTo = yourself`. It is internal state
   of the shim. A host that uses the C ABI sees no change. Zig code that
   calls only `contains`, `count` or `remove` on it compiles unchanged.
   Answer an INBOUND_CALL with `capnp_return_results` or
   `capnp_return_exception` (in Zig, `Conn.returnResults` or
   `Conn.returnException`).

5. **Native: know when a tail call waits.** When the remote answers a
   question with `takeFromOtherQuestion` naming an INBOUND_CALL that the
   host has not answered, the question waits. The host's
   `capnp_return_results` for that call becomes the question's RETURN
   RESULTS, and its `capnp_return_exception` becomes RETURN EXCEPTION.
   Results that point at a capability give RETURN EXCEPTION
   (`unimplemented`, "capnp-swift core: tail call results carry
   capabilities"; `native.conn.tail_call_caps_reason`). While the question
   waits, `capnp_cancel`, its deadline, `capnp_conn_transport_closed` and
   the end of the shutdown drain (`shutdown_drain_timeout_ms`) each end it
   once. A graceful shutdown waits for it, and `capnp_finish` returns
   `CAPNP_E_BUSY`. If the remote reuses the call's id before the host
   answers, the question ends with RETURN EXCEPTION (`failed`, "capnp-swift
   core: the tail-called call's id was reused before its answer";
   `native.conn.tail_call_reused_reason`). "Tail calls" in
   [native-abi.md](native-abi.md) has the rules.

6. **Windows: let the first `Listener.close` return before you free the
   listener.** `close` now cancels each pending accept wait. It waits until
   every thread in the listener's accept has returned, and then closes the
   handle. Each of those threads returns `error.ListenerClosed`, or a
   connection that arrived before the cancel. The wait needs no network. A
   second `close` returns at once and does not wait. Linux and macOS keep
   their behavior.

7. **Windows: a read that the local stack ended is
   `error.ConnectionResetByPeer`.** `STATUS_LOCAL_DISCONNECT` and
   `STATUS_CONNECTION_ABORTED` on a TCP read now map to
   `error.ConnectionResetByPeer`, as the remote disconnect statuses do.
   They were `error.Unexpected`. If your code reads the transport itself
   and handles `error.Unexpected` for these, handle
   `error.ConnectionResetByPeer`. `Connection` handles every read error the
   same way. Writes and shutdowns still go through std, which has the same
   gaps, so their stack traces stay.

8. **`WorkerPool` shutdown on macOS still dials its own listener.** On
   Windows the pool now cancels the waits of its workers. On macOS,
   `shutdown(2)` fails on a listening TCP socket, so a dial is still the
   only way to wake a parked accept, and that wait has no bound while
   dials fail.

9. **QUIC `WarmRedialClient`: a stop during setup is a generation.** A
   `requestStop` that came after the generation made its connection
   reachable, and before it enqueued its Bootstrap and Restore frames, used
   to leave that generation out of `generations` and out of `last_cause`.
   A stop during the first setup gave `generations = 0` and
   `last_cause = .unknown`. Now that generation counts, and `last_cause` is
   `.local_close`. A setup failure without a stop is still an error and
   still spends a redial.

## How to verify

- Build your project in each configuration that you use (`capnpc-zig`,
  `capnpc-zig-core`, and `.quic = true`), and run your tests.
- If you run a QUIC peer that is not capnp-zig, check that it sends no FIN
  on stream 0 while it waits for replies.
- capnp-swift: after you pin v0.27.0, run H11 check 9 and the C++
  `resolve_disembargo` case again. The relay text in
  [downstreams.md](downstreams.md) gives the details.
- Contributors who work in this repository: `zig build -Dquic=true
  e2e-quic` (also `just e2e-quic`) runs the Zig e2e over QUIC, as the CI
  job `e2e-quic` does. [RELEASING.md](../RELEASING.md) step 5 gives the
  new tag procedure:
  freeze `main`, start 3 Nightlies on the release commit, and run
  `just check-nightly-gate`.

## What is new

Read the `0.27.0` section of [CHANGELOG.md](../CHANGELOG.md) for the full
list, with the tests behind each fix and the CI runs behind its numbers.
