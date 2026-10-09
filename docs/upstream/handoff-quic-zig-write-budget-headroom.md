# HANDOFF — quic-zig: a local streamWrite fills the whole memory budget

> **Status: FIXED in quic-zig v0.38.0 (tag `77be067`, 2026-10-09).** Found
> by capnp-zig on its move from quic-zig v0.32.0 to v0.37.1. In v0.38.0
> `streamWrite` stops short of the receive side's share (the connection
> window, never below the window cap, 16 MiB or half the budget, whichever
> is smaller), and under pressure the
> receive buffers give back their consumed prefix before quic-zig refuses a
> peer's frame, so an honest peer does not meet EXCESSIVE_LOAD. A post-tag
> audit of capnp-zig v0.24.0 found that fix 1 below alone (a reserve of one
> window) was not a guarantee, because of the 2x receive charge; v0.38.0
> handles that with the compaction. capnp-zig v0.24.0 pins v0.37.2 and keeps
> its half-budget cap (below). capnp-zig main has since moved to v0.38.0 and
> dropped the cap: the engines and the embedded seat write straight to
> quic-zig again. One rule stays with the embedder: a connection window
> larger than half of the budget takes from the writes (a window as large as
> the budget leaves none), so capnp-zig's servers announce at most half of
> `max_connection_memory` (`transportParamsWithinBudget` in
> `src/rpc/transport/quic/options.zig`). The text below is the report as sent.

For the agent working on nullstyle/quic-zig. Self-contained; the evidence
comes from capnp-zig (same machine, /Users/nullstyle/prj/zig/capnp-zig).

## The defect

Since v0.33.0, `streamWrite` treats `max_connection_memory` as
back-pressure for the application's own writes: it takes what the budget
leaves and returns short (`Connection/streams.zig`, `streamWrite`:
`want = min(data.len, headroom, max_connection_memory - bytes_resident)`).
That is the right signal, but the write takes all of the budget that is
free. The same budget holds what the peer sends, and there running out is
a fault: when a STREAM frame grows a receive buffer and
`tryReserveResidentBytes` fails, the connection closes with
`transport_error_excessive_load` ("excessive resource use";
`Connection/recv_data_handlers.zig`, the reconcile after `recv`).

So one large local write, followed by any STREAM byte from the peer before
ACKs free part of the send buffer, closes the connection. The peer did
nothing wrong: in RPC terms it sent a Finish, a Release or a pipelined call
in the middle of a large reply. The application gets no error from its own
write, and the close it sees is a transport error it did not cause.

`connectionWindowCap` already keeps the receive window to half of
`max_connection_memory`, because a receive buffer is charged up to twice
its unread bytes until it compacts. Nothing keeps local writes out of that
half.

## Evidence (capnp-zig, quic-zig v0.37.1, macOS arm64)

The capnp-zig QUIC transport suite has the trigger as tests: a server with
`max_connection_memory = 256 KiB` answers one request with a 1 MiB frame
while the client sends a small frame every millisecond. Owned loop and
embedded seat (a host-owned `Server` with `quic.app.Driver`), baseline and
native mode. With capnp-zig's workaround removed, all four fail, within 2 to
4 ms in ReleaseSafe and 6 to 7 ms in Debug: the server closes with code 0x1,
reason "excessive resource use", the client sees a peer close, and no reply
arrives. The same shape with a
client frame every 20 ms passed in capnp-zig's runs. With the default
32 MiB budget and one stream capnp-zig could not reach it (one stream's
send buffer stops at 16 MiB).

On v0.32.0 the setup failed earlier, on the server's own write
(`streamWrite` returned `error.ExcessiveLoad`).

## What would fix it (any one)

1. `streamWrite` leaves the receive side its share: it stops at
   `max_connection_memory - connectionWindowCap(conn)` (half of the budget
   by default) rather than at the full budget.
2. A separate budget, or a reserved share, for locally written bytes, so a
   local write can never make a peer's in-window frame fail.
3. At the least, a documented headroom rule on `streamWrite` and
   `max_connection_memory`, so an embedder knows to cap its own writes.

A test in the shape of the trigger: a connection with a small budget,
`streamWrite` of more than the budget on a stream whose peer credit allows
it, then feed a STREAM frame of a few bytes from the peer on another
stream (or the same bidi stream) before any ACK. Today the connection
closes with EXCESSIVE_LOAD.

## capnp-zig's workaround

Every stream write of capnp-zig's engines goes through `streamWrite` in
`src/rpc/transport/quic/quic_zig_adapter.zig`, which passes quic-zig only
as many bytes as keep `bytes_resident` at or below half of
`max_connection_memory` (zero bytes when the connection already holds
half; quic-zig's own path for a full budget). A short count stays
back-pressure. When quic-zig leaves the receive side its share itself,
capnp-zig can drop the cap.
