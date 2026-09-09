# External KVStore consumer evidence

On 2026-09-08, the current ReleaseSafe Zig KVStore example served the existing
`capnp-deno` application over loopback TCP on macOS arm64. This exercises a
separate runtime, generated TypeScript bindings, and persistent service backend.

- Server: `examples/kvstore/zig-out/bin/kvstore-server`. The initial stress,
  cancellation, evolution, and termination probes used native source in
  `bb15e6d`. The graceful process probe additionally used the finite-lifetime
  example change documented below.
- Zig: `0.17.0-dev.1683+5ceec001b`, ReleaseSafe.
- Client: `capnp-deno` revision `7c4f153a475ed9fddefb71ac3d33f73c8ef5c308`,
  `examples/kvstore_stress_2`, Deno 2.6.8. Its runtime, generated bindings, and
  example sources were unchanged. An existing local mise configuration change
  and `.zcode` directory were preserved.
- Both applications used their existing matching KVStore schema. The schema
  evolution probe separately generated a compatible client revision.

## Results

| Check | Observed result |
| --- | --- |
| Existing stress client, 30 seconds, 32 concurrent requests | 109,092 completed batches; 3,922,254 operations; 484.78 MiB of put payload; zero errors |
| End of timed stress run | All workers completed; in-flight requests returned to zero |
| New client request field | Adding `WriteOp.auditTag @3 :Text` preserved existing ordinals and IDs; the old server accepted the write and returned the original value |
| Fresh connection lifecycle | Five bootstrap/read/close cycles succeeded; each closed client reported zero pending Returns |
| Cancellation of sent requests | Eight actual TCP Calls were aborted, eight matching early-cancellation Finish frames were observed, pending Returns returned to zero, and a subsequent request on each connection succeeded |
| Process restart and persistence | After SIGINT termination and restart against the same data directory, the marker value was recovered at version 109094 |
| Normal process shutdown with active calls | With `--run-for-ms 2500 --drain-ms 300`, the process exited with code 0 after 2,858 ms; 64,501 reads completed and 16 calls were pending when TCP closed |
| Pending calls at normal shutdown | All 16 rejected through the external client's configured 1,000 ms call deadline; pending Returns and exported capabilities returned to zero after client cleanup |
| Restart after normal shutdown | The previously acknowledged marker was recovered at version 1 from a fresh probe database; the restarted service also exited normally with code 0 |

The cancellation probe deliberately held incoming Returns at the client
transport adapter until its sent Call was aborted. This makes the pending-call
race repeatable while sending the Call and Finish over the real TCP connection.
It proves cancellation cleanup and continued connection usability; it does not
prove interruption of synchronous server work.

Connection close was awaited and checked. The SIGINT probe checks recovery
after termination. The separate finite-lifetime probe checks normal graceful
process cleanup, without a signal handler. The five fresh connections are
explicit reconnects, not an automatic reconnect policy under service failure.
These are bounded integration checks, not a production soak or a throughput
comparison.

## Finite-lifetime shutdown guarantee

The optional `--run-for-ms N` timer starts after service initialization. At its
deadline, the example calls the existing `WorkerPool.shutdownGraceful`: it stops
accepting, allows existing connections up to `--drain-ms N` (default 1,000 ms)
to finish, then requests closure of stragglers. `run()` joins workers before
normal pool and store cleanup. Omitting `--run-for-ms` preserves indefinite
operation. An early run/start error cancels and joins the timer before the pool
is destroyed.

This bounds the connection drain period after pending accepts return; it cannot
forcibly interrupt an arbitrarily blocked synchronous handler or guarantee a
deadline if the backend permanently fails to wake a pending accept. On Windows,
the listener must remain open until those accepts return: closing it underneath
them aborts the process in the pinned Zig backend. Transient loopback wake
failures are retried, and wake connections stay open until consumed. Calls still
pending at transport close can fail, and an interrupted write may have been
applied without its result reaching the caller. The persistence check therefore
uses an acknowledged write.

The external Deno `RpcWireClient` did not immediately reject pending calls on
EOF: all 16 rejected about 1,003 ms after TCP closed through the explicit
1,000 ms deadlines. The observed guarantee includes those client deadlines;
it does not establish prompt EOF propagation or bounded settlement for a client
without deadlines. Both server runs exited successfully through normal cleanup,
with no allocator leak or panic diagnostics.

Focused regressions cover finite-lifetime argument parsing (including missing
and overflowing values), unchanged indefinite defaults, and normal joining of
an actual idle worker pool. The public executable's previously ignored
`--run-for-ms 30 --drain-ms 20` invocation first failed to exit within two
seconds; after the change it exited with code 0 in 320 ms.

## External decoder gap found

A second compatible client revision also added:

```capnp
# Additional field in Entry:
source @3 :Text = "new-client-default";
```

The Zig server accepted the evolved request. The Deno client then failed while
reading an old-format response that omitted the new pointer field:

```text
KvStore.writeBatch failed: pointer offset out of range: 2
src/encoding/runtime_codec.ts:203 — decodeStructAt
src/encoding/runtime_message.ts:427 — pointerWordIndex
```

The external decoder calls `pointerWordIndex` for every field in its newer
schema; that helper rejects offsets outside the message's original pointer
count. A targeted follow-up belongs in `capnp-deno`'s `decodeStructAt`: treat a
schema field beyond the received pointer section as an absent pointer and apply
its declared default, while retaining bounds checks for present pointers. The
regression should decode an old Entry with two pointer slots using a newer
three-pointer descriptor, checking both empty and explicit Text defaults. It
should also preserve failures for physically truncated present pointers.

No `capnp-deno` runtime changes were made. Full response-field evolution for
this external consumer remains unproven until that separate decoder issue is
fixed; native schema-evolution conformance has its own tests.

## Reproduction and local receipts

Build the server from the capnp-zig root:

```sh
mise exec -- zig build --build-file examples/kvstore/build.zig -Doptimize=ReleaseSafe
mise exec -- examples/kvstore/zig-out/bin/kvstore-server \
  --host 127.0.0.1 --port 19138 \
  --db-path .zig-cache/ci-repro/kvstore-data \
  --backup-dir .zig-cache/ci-repro/kvstore-backups --quiet
```

Run from the separate capnp-deno root:

```sh
mise exec -- deno run --allow-net --allow-sys \
  examples/kvstore_stress_2/kvstore_stress_client.ts \
  --host=127.0.0.1 --port=19138 --concurrency=32 --duration-seconds=30
```

The local receipts in capnp-zig's `.zig-cache/ci-repro/` are `deno-stress.log`,
`deno-lifecycle.log`, `deno-recover.log`, and `deno-evolution-red.log`. The bounded
lifecycle harness is `external_kvstore_validation.ts`; the failing full-evolution
variant is preserved as `external-evolution-full-red.ts`, with its generated
client and schema in `evolved-client-full/` and `evolved-schema-full/`.

The normal-shutdown probe is `run_graceful_probe.py`, which supervises the
finite-run executable and invokes `external_kvstore_graceful.ts` using the
external repository's pinned Deno. Its receipts are
`graceful-process-receipt.log`, `deno-graceful-active.log`,
`deno-graceful-recover.log`, and `graceful-server-{active,recover}.log`.
`finite-cli-{red,green}.log` records the public CLI regression. Re-run from the
capnp-zig root with `mise exec -- python3 .zig-cache/ci-repro/run_graceful_probe.py`.
