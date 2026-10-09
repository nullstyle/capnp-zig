# HANDOFF — quic-zig: a connection "at rest" skips the stream GC

> **Status: FIXED in quic-zig v0.37.2 (tag `51a34c0`, 2026-10-08).** Found
> by capnp-zig on its move from quic-zig v0.32.0 to v0.37.1. quic-zig now
> marks a connection when a stream can be reclaimed (a new `stream_gc`
> timer, due at once, so the ready API ticks it too), and it also fixed a
> second defect this test found: a server connection never came to rest.
> capnp-zig moved to v0.37.2 and dropped the workaround below. Without it,
> the tests that failed on v0.37.1 (19 in Debug, 3 in ReleaseSafe) pass.
> The text below is the report as sent.

For the agent working on nullstyle/quic-zig. Self-contained; the evidence
comes from capnp-zig (same machine, /Users/nullstyle/prj/zig/capnp-zig).

## The defect

v0.36.0 added the rest cache: once `Connection.atRest()` holds, `tick`
and `nextTimerDeadline` answer from `rest_deadline` until `touch`, and a
Debug build runs `tickFull` as well and asserts that it left the cache
valid (`Connection.zig:5352`, `std.debug.assert(self.rest_deadline_valid)`).

`atRest()` does not look at the stream table. A stream whose halves have
ended (a local uni stream whose FIN was acknowledged, a peer uni stream read
to its FIN, a bidi stream with both, a stream the application stopped) is
work for the next tick: `gcClosedStreams` frees it, records its end in the
`RecvEndRing`, advertises the peer's stream credit
(`maybeAdvertiseStreamCredit`), and calls `touch` because `n > 0`.

A connection can reach that state and then be declared at rest:

1. A datagram arrives: `handle` / `Server.feed` touches. It carries the
   ACK of our FIN (or the application then reads a stream to its end, or
   stops one).
2. The embedder drains: `pollDatagram` sends what is due, then returns
   null with `atRest()` true, which primes the cache
   (`Connection/send.zig:119`).
3. The embedder ticks: the cache is valid and not due.
   - Debug: `tickFull` runs, the GC frees the stream and touches, and the
     assert fires.
   - Release: `tick` returns at once. The stream stays until something
     touches the connection. Its stream id does not come back to the
     peer, and its `streamRecvEnd` note is not written.

Feed, drain, tick is a natural loop order, and it is the order capnp-zig
must use: its engines read streams before the tick (the v0.28.0
stream-end rule), and it sends between the service pass and the tick.

## Evidence (capnp-zig, quic-zig v0.37.1, macOS arm64)

- Debug, without a workaround: 19 of 202 QUIC tests crash on the assert
  above, from `tick` in capnp-zig's connection loop and in a raw
  quic-zig client fixture (feed, `advance`, drain, `tick`).
- ReleaseSafe, without a workaround: 3 of 202 fail. Two are native
  connections that carry 10,240 large frames over unidirectional streams
  (window 8, and window 1): they stop at 10,185 and 10,177 frames with no
  error, both sides at rest, the sender out of stream ids that the
  receiver's GC never gave back.
- A probe at capnp-zig's tick sites (before its workaround's touch)
  counted 25,619 ticks over one Debug run of the suite where the rest cache
  was valid and at least one stream was reclaimable by the predicate of
  `gcClosedStreams`.
- On v0.32.0 the same suites pass (no rest cache).

## What would fix it (any one)

1. `atRest()` returns false while a stream is reclaimable (or has
   `recv_stopped`). The cheapest form: a counter or flag set at each
   transition that makes a stream reclaimable (the ACK that completes a
   send half, the read that reaches a FIN or consumes a RESET, the stop),
   cleared by the GC.
2. Those transitions call `touch`.
3. The shortcut in `tick` still runs `gcClosedStreams` when it has work.

A test in the shape of the trigger: open a uni stream, write, finish, feed
the ACK of the FIN, drain to null, tick; in Debug that asserts today, and
in release `streams.get(id)` is still non-null after the tick.

## capnp-zig's workaround

Every tick in capnp-zig's loops goes through
`src/rpc/transport/quic/quic_zig_adapter.zig`: `tickConnection` touches the
connection, then ticks; `tickServer` touches every slot's connection, then
calls `Server.tick`. That is the full tick of v0.35.0 and earlier, so
capnp-zig gets none of the rest cache's savings for ticks. Its embedder
docs ask a host that ticks its own `Server` to do the same. When quic-zig
fixes the defect, capnp-zig can drop the touches.

The ready API has the same gap, and no `Server.tick` to put a touch in
front of. `tickDue` ticks only the slots whose deadline has passed, through
the same `Connection.tick`, so a slot that a drain left at rest with a
reclaimable stream keeps it until its next timer or datagram. quic-zig's
own `runUdpServer` loop (`on_iteration` hook, then `tickDue`, then the
`takeReady` drain; `transport/udp_server.zig`) is such a host. capnp-zig's
embedder docs ask a ready-API host to touch and then tick each connection
that carries a capnp-zig seat after its service pass; the touch also puts
the slot on the ready list (`wake_hook`), so the drain sends what the tick
queued. A fix has to reach this path as well as `Server.tick`: a
reclaimable stream must get a tick that runs the GC even when no deadline
has passed.
