# QUIC RPC Transport Guide

capnp-zig's QUIC transport is optional. Normal builds expose a disabled
`rpc.transport.quic` facade with dependency-free framing helpers; build with
`-Dquic=true` when an application wants `rpc.transport.quic.Connection`. The
package manifest declares `quic` (lazy) so opt-in builds are reproducible,
but default builds do not fetch, resolve, or instantiate that dependency.

```bash
zig build -Dquic=true test-rpc-quic --summary all
```

For the same non-vacuity checks used by CI, run both targeted evidence modes:

```bash
just test-rpc-quic-evidence Debug
just test-rpc-quic-evidence ReleaseSafe
```

The native Zig evidence executable scans the complete QUIC test directory,
rejects `SkipZigTest`, and then requires exactly four runnable roots: transport,
public API, internal implementation, and real `Peer`-over-QUIC behavior. Its
per-root floors are 43 + 1 + 36 + 32 = 112 tests (raised to the actual counts on
2026-10-03, so a silently dropped test fails the gate). The build step fails immediately
when `-Dquic=true` is absent, and each root is a direct build dependency, so the
gate neither parses test output nor relies on a CI-only shell or package.

CI is configured to run the Debug + ReleaseSafe pair from each operating
system's native shell on Linux, macOS, and Windows. Linux also runs the full
repository build/check/test/API/docs surface against the QUIC-enabled root;
that broader root-wide gate is not duplicated on macOS or Windows.

Current evidence is intentionally stated narrowly: macOS passes the four
evidence roots in both Debug and ReleaseSafe (per-root minimum test counts are
enforced by `tools/quic_test_evidence.zig`, so a silently shrunken root fails
the gate; exact totals grow with the suites and are not restated here). The Windows QUIC tree passes
full-tree test cross-compilation (113/113), but that is not runtime evidence.
The native Windows no-skip lane remains a hosted acceptance gate after
capnp-zig itself is pushed; do not infer Windows runtime parity from the
dependency pushes or cross-compilation result.

The transport uses ALPN `capnp-rpc/1`. One QUIC connection represents one
Cap'n Proto RPC vat session. The payload above the QUIC transport is still the
standard `rpc.capnp` message stream; QUIC changes how complete RPC frames move
between peers, not the RPC protocol that `Peer` handles.

The manifest pins the `quic` package at annotated tag `v0.32.0` (commit
`ffdb251`), one step from v0.30.1 (v0.31.0 and v0.31.1 are included, not
pinned). None of the three changes the wire, the embedder API or the option
map.

- **A dead peer is noticed one idle timeout after it stops answering, not
  two to three (v0.31.1).** From quic-zig v0.30.1, which capnp-zig v0.21.0
  pins, every datagram sent restarted the idle timer, and the probes for
  data a dead peer never acknowledges kept the connection alive until one
  backed-off probe gap was longer than the timeout. The timer now restarts
  as RFC 9000 section 10.1 says: when a packet is received and processed,
  and at the first ack-eliciting send after that. A peer that stops
  answering mid-call (a frozen process, a partition) now ends the session
  with `DisconnectCause.idle_timeout` one idle timeout after the first
  unanswered send: measured 2,001 ms at a 2 s timeout, against 4,201 to
  4,431 ms on v0.30.1 (the transport suite's dead-peer test). Datagrams
  that do not open (spoofed ones, say) no longer restart it either. The
  idle timeout is also at least three probe timeouts: a
  `max_idle_timeout_ms` below that is raised to it, about 3 s on a fresh
  connection with no RTT sample, a few tens of milliseconds once there are
  samples.
- **The receive window kept is the one announced (v0.32.0).** Through
  v0.31.1 the credit after the initial window was a fixed 1 MiB per stream
  and 16 MiB per connection, whatever the transport parameters said.
  `defaultTransportParams()` (`src/rpc/transport/quic/options.zig`)
  announces exactly those (`initial_max_stream_data_*` 1 MiB,
  `initial_max_data` 16 MiB), so nothing changes with the defaults. A
  `transport_params` that announces more now gets it for the whole stream
  (a slow reader can then hold that much per stream, still bounded by
  `max_connection_memory`); one that announces less gets less.
- **A stream's send buffer is a quic-zig knob (v0.32.0).** quic-zig's
  `Client.Config`, `Server.Config` and `Connection` have
  `max_buffered_send` (default 1 MiB, the old fixed value): the bytes a
  stream holds that the peer has not acknowledged in order, which bounds a
  single stream to one buffer per round trip. capnp-zig does not pass it
  through; every QUIC stream it opens keeps the 1 MiB default. To go past
  it on a path whose bandwidth-delay product is larger, the sender needs
  this buffer raised and the receiver a matching announced window.
- ACK frames carry up to 64 ranges (were 16), and the loss thresholds that
  widened for reordering shrink back after 16 round trips with no spurious
  loss (v0.32.0). A client confirms its handshake on an ACK of a 1-RTT
  packet of its own, and `Server.feed` leaves a datagram it reports as
  `.dropped` as it came (v0.31.0).

v0.30.1 made a probe timeout no loss (RFC 9002 section 6.2.4, so an ACK
that was only late is no longer a window cut), bounded the handshake's probe
timeout at about a second, made `quic.unixWallClockUs` compile on Zig 0.17.0,
and added `Server.adoptLoopThread()`: capnp-zig calls it so that a
server-side `Connection` or `Listener` can move to another thread again in a
Debug build ("Rotation" below). Do not pin v0.30.0: it did not compile on
Windows. v0.29.0 has no wire change. A
connection costs about 91 KB on the heap, not 1.09 MB. A late packet is no
longer counted as a lost one, and a client completes its handshake through
loss. It adds the previous ticket key,
the NEW_TOKEN clock and the client ticket lifetime that "Session-ticket key"
below describes. v0.28.1 is v0.28.0 with a build fix: v0.28.0 did not compile
for a 32-bit target, so do not pin it. v0.28.0 added
`Connection.streamRecvEnd`, so the end of a stream is not lost when `tick`
runs before the read (see
[Embedder rules](#embedder-rules)). v0.27.0 before it made session-ticket keys
server config, and made a client send its 0-RTT data again after a Retry. The
tag in turn pins the published boringssl-zig commit `ff30fe99` (boringssl
0.6.7). That BoringSSL wrapper links Windows sockets as `ws2_32` with package-config lookup
disabled, removing the native-shell and Git Bash `pkg-config.BAT` failure path.
A process can hold only one `quic` module: every package that links quic-zig
into the same build must pin the same tag and pass the same dependency options
(`.target`, `.release`, `.@"sanitize-c" = "trap"`), or the build makes two quic
modules, each with its own BoringSSL. The quic packages move to a new quic-zig
tag together. http3-zig v0.5.4 and qmsg v0.8.1 pin v0.30.1, as capnp-zig
v0.21.0 does, and no release of theirs on v0.32.0 is known yet; until one is,
a build that links this pin next to either makes two quic modules. Connection and server session loops drive
`Connection.advance()` before waiting on datagrams and again during active
service, then tick timers and drain outbound datagrams.

The public API matches the TCP transport's shape. Most applications need only
two calls: `rpc.transport.quic.connect` returns a `ClientSession` (a QUIC
`Connection` plus its `Peer`), and `rpc.transport.quic.serve` returns a
`PeerServer` that gives every accepted QUIC session its own `Peer`. See
[One-call sessions](#one-call-sessions-connect-and-serve). Below them,
`rpc.transport.quic.Connection` is the transport for one client/server session,
and `rpc.transport.quic.Server` hosts up to
`ServerOptions.max_concurrent_connections` sessions on one UDP listener, with
one `ServerSession` transport driver per accepted QUIC connection. The whole
QUIC module remains Experimental.

## Modes

`rpc.transport.quic.ClientOptions.mode` and `rpc.transport.quic.ServerOptions.mode` default to
`.baseline`. Both sides must choose the same mode explicitly when using
`.native`; the mode is not negotiated with a separate ALPN.

| Mode | Stream Layout | Use When |
| --- | --- | --- |
| `baseline` | Client-initiated bidirectional stream 0 carries 32-bit little-endian length-delimited RPC frames. | You want the most conservative QUIC port of the TCP transport. This is the default. |
| `native` | Bidirectional stream 0 carries a native preface, versioned hello, and ordered control envelopes. Small RPC frames are inline; large RPC frames move over one-shot unidirectional data streams referenced by ordered control frames. | You want QUIC-native stream routing and are comfortable opting both peers into the newer wire shape. |

Baseline mode is the compatibility baseline. It preserves the TCP transport's
single ordered byte stream above the QUIC handshake, so every RPC frame is still
delimited by the same 32-bit little-endian length prefix before being handed to
`Peer`. Use it for first deployments, interop bring-up, and any peer set where a
mode mismatch would be difficult to roll back quickly.

Native mode is an explicit opt-in wire shape for QUIC-specific stream use. It
keeps stream 0 as the ordered control stream and uses additional unidirectional
streams only for large frame bodies. The RPC layer still observes complete
frames in Cap'n Proto E-order; native mode changes only how the transport moves
those frames internally.

Native mode preserves Cap'n Proto E-order. Control frames are processed in
control-stream order. If a `data_rpc` control frame is next but the referenced
unidirectional data stream has not completed, later control frames are not
dispatched yet: up to one control frame's worth stays buffered, and the rest
stays unread in the QUIC stream, held back by flow control.

QUIC DATAGRAM is not used by either mode. Telemetry and sideband data should be
designed as a transport-general facility, not as a QUIC-only extension.

## Recommended Mode Defaults

Use the defaults unless you have a concrete reason to diverge:

- Keep `mode = .baseline` for production rollouts and mixed-version fleets.
- Set `mode = .native` only when both peers are deployed from builds that
  intentionally support the native QUIC wire shape.
- Leave `alpn_protocols = &.{rpc.transport.quic.alpn}` unless you are integrating with a
  private deployment that has a documented ALPN policy.
- Keep client certificate verification enabled. `ClientOptions.insecure_skip_verify`
  exists for local tests and controlled interop with self-signed peers only.
- Keep 0-RTT disabled for RPC servers unless every bootstrap operation and
  early call path is safe to replay.
- Keep `reveal_close_reason_on_wire = false` outside local debugging.

## Opting Into Native Mode

Use `rpc.transport.quic.NativeOptions` on both client and server:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.transport.quic;

fn initServer(allocator: std.mem.Allocator, io: std.Io) !quic.Connection {
    return try quic.Connection.initServer(allocator, io, .{
        .listen_addr = .{ .ip4 = .{
            .bytes = .{ 127, 0, 0, 1 },
            .port = 7000,
        } },
        .tls_cert_pem = server_cert_pem,
        .tls_key_pem = server_key_pem,
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
    });
}

fn initClient(
    allocator: std.mem.Allocator,
    io: std.Io,
    server_addr: std.Io.net.IpAddress,
) !quic.Connection {
    return try quic.Connection.initClient(allocator, io, .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .insecure_skip_verify = true, // local self-signed example certificate
        .mode = .native,
        .native = .{
            .inline_frame_threshold = 64 * 1024,
            .max_control_frame_bytes = quic.default_native_max_control_frame_bytes,
            .max_pending_data_streams = 16,
            .max_pending_data_bytes = quic.default_native_max_pending_data_bytes,
        },
    });
}
```

The connection callback shape is the same as baseline mode: `sendFrame()` takes
one complete Cap'n Proto RPC frame, and inbound callbacks receive one complete
RPC frame. Higher-level RPC code can therefore use the same `Peer` attachment
path in both modes.

The end-to-end `Peer` cases exercise, among others, a verified-CA baseline
session; native Bootstrap/Call/Return/Finish; a pipelined call on the returned
capability; a native large-frame data stream; graceful and abrupt close;
two-session fanout with each server `Peer` attached from the accept hook;
fanout close isolation; and `serve` plus `connect` with no hand-written session
loop. The fanout server allocates sessions at stable heap addresses before a
`Peer` borrows the transport, and it detaches that binding before reaping a
session.

## One-call sessions: `connect` and `serve`

These are the QUIC versions of `rpc.transport.tcp.ClientSession` and
`rpc.transport.tcp.ServerSession`. They own the wiring every QUIC consumer
used to write by hand: construct the transport, attach a `Peer`, apply the
secure defaults, start, run, and tear down in the one safe order.

| | TCP | QUIC |
| --- | --- | --- |
| Client | `tcp.connect(gpa, io, address, .{...})` returns `*ClientSession` | `quic.connect(gpa, io, .{ .conn = client_options, ... })` returns `*ClientSession` |
| Server | `tcp.ServerSession.accept(gpa, &listener, .{...})`, one connection per session | `quic.serve(gpa, io, server_options, .{ .on_accept = ... })` returns `*PeerServer`, one `Peer` per accepted session |
| Bootstrap | `Iface.setBootstrap(&session.peer, &impl)` before `run()` | the same call, inside `on_accept` |
| Accept event | `WorkerPool` `on_accept` | `ServeOptions.on_accept`, or `Server.setOnSessionAccepted` one level down |
| Loop | `run()` blocks; `requestStop()` is the one thread-safe call | the same |
| Defaults | call deadline 30 s, drain 5 s, Join lease 30 s, OS-entropy embargo ids | the same |

The client:

```zig
const quic = capnpc.rpc.transport.quic;

const session = try quic.connect(gpa, io, .{
    .conn = .{
        .remote_addr = server_addr,
        .server_name = "localhost",
        .ca_pem = server_ca_pem, // keep verification on
    },
});
defer session.deinit(); // only after run() returns
_ = try PingPong.Client.fromBootstrap(&session.peer, &state, onBootstrap);
session.run(); // returns once the connection has closed
```

Inside callbacks, `quic.ClientSession.fromPeer(peer)` recovers the session;
call `close()` on it to end `run()`. `closeCause()` reports the typed
`DisconnectCause` after the connection dies.

The server:

```zig
fn onAccept(ctx: ?*anyopaque, session: *quic.PeerServer.Session) anyerror!void {
    const impl: *PingPong.Server = @ptrCast(@alignCast(ctx.?));
    _ = try PingPong.setBootstrap(&session.peer, impl);
}

const server = try quic.serve(gpa, io, .{
    .listen_addr = listen_addr,
    .tls_cert_pem = server_cert_pem,
    .tls_key_pem = server_key_pem,
    .max_concurrent_connections = 64,
}, .{ .ctx = &impl, .on_accept = onAccept });
defer server.deinit(); // only after run() returns
// On a dedicated thread; `server.requestStop()` from any thread ends it.
server.run();
```

`on_accept` runs once per accepted session, on the `run()` thread, after the
session's `Peer` is built and before the session's first frame is handled.
Returning an error rejects the session. Optional `on_error` and `on_close`
hooks report per-session failures and closes. `PeerServer.Session` carries the
`peer`, a free `user_data` slot, `close()`, `closeCause()`, and
`Session.fromPeer(peer)` for generated handlers. The server frees each
session's `Peer` once the QUIC connection has finished draining, not when
`on_close` fires: a closing connection can still reach its `Peer`.

[`examples/rpc_pingpong_quic.zig`](../examples/rpc_pingpong_quic.zig) is the
complete program. `zig build -Dquic=true example-rpc-quic` runs it, and the
QUIC CI lane runs it on Linux, macOS and Windows.

### The accept hook

`Server.setOnSessionAccepted(ctx, hook)` is the same event for code that
drives `Server` directly, and `serve` is built on it. The contract:

- The hook fires exactly once for every `ServerSession` the server adopts, on
  the loop thread, inside the step that adopted it.
- The session is already listed (`sessionCount()` and `sessionById()` see
  it), and the step has not serviced it yet, so callbacks attached in the
  hook see its first frame.
- Adoption happens when the listener creates a QUIC connection for a fresh
  Initial, before the handshake completes. Anyone who can reach the port can
  cause adoptions, up to `max_concurrent_connections` at once and subject to
  the listener rate gates, so keep the hook's work bounded.
- Returning an error rejects the session; the server closes it in the same
  step. The close reaches the client during the handshake, and a capnp-zig
  client ends with `DisconnectCause.peer_close` within a round trip, also
  when it queued frames (its Bootstrap) before the handshake. Through
  quic-zig v0.25.0 the client could not read that close and waited for its
  own handshake timeout.
- The hook must not step, run or deinit the server.

`Server.runWithAfterStep(ctx, after_step)` is `run()` with a callback after
every step. A session's close callback fires inside a step, while its
connection can still be stepped during draining, so state that the session's
callbacks borrow must be freed from `after_step` once `sessionById(id)`
returns null. `after_step` runs after every step and `sessionById` is a
linear scan, so check only sessions whose close callback has fired; no other
session can be gone. `PeerServer` frees its peers this way, and does no
check at all while no closed session is waiting.

## Concurrency Model

One QUIC connection carries one Cap'n Proto vat session, and the RPC protocol
requires every frame of a session to arrive in order (E-order). Native mode
moves large frames over separate QUIC streams, but it still hands them to the
`Peer` in order. So concurrency comes from four places, from cheapest to
most isolated:

- **Many calls in flight.** A `Peer` never waits for a Return before it sends
  the next call. By default up to 4096 questions can be outstanding on one
  session (`PeerLimits.max_outbound_questions`), and Returns complete them as
  they arrive.
- **Promise pipelining.** Call a method on a result before that result exists.
  The pipelined call goes out right behind the first one, without waiting for
  its Return, and saves a round trip per hop. Native mode is tested with a
  pipelined call on the bootstrap capability.
- **`-> stream` methods.** For bulk transfer, a generated `StreamClient` keeps
  a bounded window of calls in flight (64 calls and 1 MiB by default) and
  applies backpressure instead of growing queues. It runs in the RPC layer,
  above either transport mode. See [streaming.md](streaming.md).
- **Multiple connections.** Separate connections are separate sessions, with
  no ordering or head-of-line blocking between them. A large frame delays
  every later frame on its own session, so give independent bulk traffic its
  own connection. One `serve` loop thread drives all of a server's sessions;
  a client can open several `ClientSession`s. Capabilities belong to one
  session (three-party handoff is the exception).

Per-message independence is a different model: unordered messages, cancel by
stream reset, optional unreliable delivery. That is the qmsg messaging
framework's model, not Cap'n Proto RPC's, and the two are not merged on the
wire. When an application needs both, run qmsg as the per-message lane next
to `capnp-rpc/1` on the same UDP endpoint, routed by ALPN (`EmbeddedSession`
is the capnp seat for a foreign `quic.app.Driver` host). Cap'n Proto
serialization, without RPC, also works as a qmsg body codec. Do not tunnel RPC
frames through qmsg messages or QUIC DATAGRAM: a dropped or reordered RPC
frame ends the session.

## Windows UDP Receive Bridge

Windows `std.Io` sockets use AFD handles and do not support the timed UDP
receive path used on POSIX. QUIC therefore keeps one ordinary blocking receive
in an `io.concurrent` future. Only the owner thread advances QUIC, processes a
datagram, or invokes callbacks; an `Io.Condition` wakes it for completion,
timer expiry, explicit wake, or close. A timer tick leaves a still-valid receive
in flight. Teardown alone cancels and reaps the future, which drives the kernel
cancellation path exactly once before the socket or callbacks are destroyed.

The bridge carries the original receive buffer with its completion, so a timer
return followed by a call with another buffer cannot mis-slice the retained
datagram. Poll, wake-before/during-wait, timer-with-pending-receive, completion,
truncation, buffer retention, start failure, exactly-once cancellation, and
repeated close/deinit are deterministic regressions. A compile-time tripwire
keeps Windows QUIC from calling `Socket.receiveTimeout()`.

## Oversized Inbound Datagrams

A datagram larger than `udp_rx_buffer_size` is a **per-datagram fault, not an
endpoint fault**, on every platform and on every receive path — the
single-connection loop, `Server`, and the bare `Listener`. It is dropped and
serving continues. UDP is unauthenticated, so failing the receive would let
any host that can reach the port take down the endpoint — and, on a fanout
server, every session on it — with one spoofed packet. Socket-fatal errors
still propagate.

The two platforms detect it differently and neither hands back anything
usable: POSIX sets `MSG_TRUNC` and returns only the prefix that fit, while
Windows fails the receive with `STATUS_BUFFER_OVERFLOW` and discards the
payload *and* the sender address. The Windows receive bridge normalizes both
into one `truncated` outcome, and every drop then routes through a single
policy in `src/rpc/transport/quic/datagram_drop.zig`.

Each drop is:

- **counted** — `Server.droppedDatagramCount()` / `Listener.droppedDatagramCount()`,
  per UDP endpoint, and `StepResult.dropped_datagram` for the step that saw it;
- **logged** — one `warn` on the `rpc_quic` scope naming the buffer size;
- **published** — a redacted `events.Observer` `resource_rejection` carrying
  `Resource.udp_datagram_bytes`, `limit` = the rx buffer size, and
  `err = error.DatagramTooLarge`. `attempted` is deliberately `null`: neither
  platform can report the datagram's true size.

`receiveOne` returns `null` for a drop, the same as a timeout or a wake, since
none of the three is actionable by the caller.

Watch the counter. A spoofed oversized datagram and a legitimate peer behind a
path MTU this endpoint is not sized for look identical on the wire, and both
are now silent to the application. The 64 KiB default `udp_rx_buffer_size` sits
above the 65507-byte IPv4 UDP payload ceiling, so it cannot be exceeded over
IPv4; if you tune it down toward the path MTU, a rising drop count with healthy
sessions means the buffer is too small, not that you are under attack.

## Server Fanout And Session Boundary

`rpc.transport.quic.Connection.initServer()` is the compatibility entry point for the
one-session transport. It requires
`ServerOptions.max_concurrent_connections == rpc.transport.quic.compatibility_max_concurrent_sessions`.
Internally it owns a `rpc.transport.quic.Listener`, accepts the first server-side
`rpc.transport.quic.AcceptedSession`, and drives that session through the existing
`Connection.start()` callbacks.

`rpc.transport.quic.Server` is the fanout API. It owns the same listener/socket root, adopts
each accepted QUIC slot into a `rpc.transport.quic.ServerSession`, announces
each one through the accept hook (`setOnSessionAccepted`), lets callers attach
callbacks per session, and drives either one chosen session or all sessions.
`rpc.transport.quic.serve` wraps it with one `Peer` per session. It keeps the wire behavior identical to `Connection`: the ALPN
is still `capnp-rpc/1`, and each session independently uses either `.baseline`
or `.native` according to the server options.

The lower-level boundary remains public for focused transport tests and bespoke
embedding:

- `rpc.transport.quic.Listener` owns the UDP socket and `quic_zig.Server`.
- `rpc.transport.quic.Session` is a borrowed handle for one accepted server slot.
- `rpc.transport.quic.AcceptedSession` carries the borrowed session plus its listener slot
  ordinal.
- `rpc.transport.quic.AcceptedSessionDriver` attaches, drives, and reaps the one accepted
  session used by the compatibility connection.
- `rpc.transport.quic.Server` owns a listener plus independent `ServerSession` transport
  drivers for fanout.
- `rpc.transport.quic.ServerSession` has the familiar `start()`, `sendFrame()`,
  `requestClose()`, and `closeStatus()` shape for one accepted server-side
  session.
- `rpc.transport.quic.EndpointDriver` is the shared run-loop boundary for endpoint-specific
  socket, timer, inbound datagram, outbound datagram, and session-reaping work.
- `rpc.transport.quic.ServerEndpoint` pairs a listener with the accepted-session driver
  currently attached to the compatibility connection.

Internally, the transport keeps the mode-specific frame mechanics behind narrow
helper modules:

- `baseline_engine.zig` owns baseline stream 0 open/read/write behavior and the
  length-delimited frame queue.
- `native_engine.zig` owns native preface/hello state, ordered control frames,
  unidirectional data-stream sends, and native pending-data budgets.
- `length_framer.zig` and `native_framer.zig` encode/decode transport frames
  without owning socket or peer lifecycle.
- `endpoint.zig`, `client_endpoint.zig`, `server_endpoint.zig`,
  `datagram_io.zig`, and `scheduler.zig` keep socket datagrams, timers,
  endpoint stepping, and wake decisions out of the mode engines.
- `close.zig`, `close_controller.zig`, and `termination.zig` centralize close
  code selection, reason redaction, and terminal state transitions.
- `options.zig` is the public configuration boundary; prefer adding documented
  knobs there instead of threading private constants through examples.

Use `rpc.transport.quic.serve` when `ServerOptions.max_concurrent_connections`
is greater than one, or `rpc.transport.quic.Server` directly when you need
transport-level control. Keep `Connection.initServer()` for compatibility tests
and single-session peers.

### Embedder rules

An `EmbeddedSession` runs in a loop that you own: your UDP socket, your
`quic_zig.Server` and your `quic.app.Driver`. Your loop must obey two rules.
capnp-zig's own loops (`Connection`, `Server`, `connect` and `serve`)
already obey both.

**1. Feed, then service, then tick.** Do these steps in each pass:

1. Give the received datagrams to quic-zig (`Server.feed`).
2. Call `driver.service`, then call `service(now_us)` on each
   `EmbeddedSession`.
3. Call `Server.tick`.

The reason is quic-zig's stream GC. `tick` frees a stream when its receive
half has ended. Sometimes every byte of a stream is read, and then its FIN or
RESET arrives alone. If `tick` runs before the service pass, the stream is
gone before the Driver reads it, and a read gets `StreamNotFound`. Through
quic-zig v0.27.0 the Driver then reported `.reaped`, so a clean end and a cut
stream looked the same, and the RESET error code was lost.

Since quic-zig v0.28.0 the connection keeps a note of how each freed stream
ended. `Connection.streamRecvEnd(id)` gives it: `fin_seen`, `reset_code`,
`final_size`, `read_offset`, `stopped` and `isClean()`. It gives the same
answer before and after the tick that frees the stream, at least through the
next tick. The Driver reads the note and reports `.fin` or `.reset`. For a
host that ticks first, this gives:

- A RESET of stream 0 reaches the seat as `.reset`, and the seat closes the
  session (the E-order contract), as in the safe order. Through v0.27.0 the
  seat got `.reaped`, and the session stayed up with no control stream.
- A native data stream that the peer reset keeps the final size and the
  error code of its RESET. The seat judges the frame as the owned loops do:
  a final size other than the announced length fails it (`InvalidFrame`),
  missing bytes fail it (`DataStreamReset`), and all the bytes complete it.
  Through v0.27.0 the seat lost the RESET in this order.
- A data stream that ended clean keeps its bytes until the engine reads
  them, as before.

`.reaped` now means "no clean end the Driver can vouch for": the teardown
pass, a stream that this side stopped, or a freed stream whose note is gone.
The seat treats a freed stream with no note as cut. Its bytes in hand are its
final size: all the announced bytes complete the frame, and fewer fail it at
once with `InvalidFrame`. On stream 0 such an end closes the session.

**The stopped-stream signal.** The seat refuses a peer stream that has no
place in the protocol with STOP_SENDING (`peer_streams.zig`). After the stop,
quic-zig throws away the bytes that still arrive on the stream. Since
quic-zig v0.28.0 a stream that this side stopped ends in one of two ways,
also while it is still live:

- As `.reset`, when the peer answers the stop with RESET_STREAM. RFC 9000
  §3.5 tells the peer to do this, and quic-zig does it, so this is the usual
  case. Do not take it for a reset that the peer started.
- As `.reaped`, when the stream ended before a RESET_STREAM arrived (for
  example, the FIN left the peer before the stop reached it). Such a stream
  has `streamRecvWasReaped(id) == false`, so that call alone does not tell
  it from the teardown pass.

Before you act on `.reset` or `.reaped`, read
`if (conn.streamRecvEnd(id)) |e| e.stopped`. Do not unwrap the result with
`.?`: in the teardown pass, `streamRecvEnd` is null for a stream whose
receive half has not ended. The seat drops a stopped data stream and never
takes its bytes as a complete data stream. This is also true for a data
stream that your host stops. If your host stops stream 0, the seat closes
the session, because stream 0 lost bytes.

capnp-zig survives the wrong order. A native data frame announces its length
on the control stream (stream 0), and the seat keeps the bytes of a data
stream after its end. The owned loops read the note too: when `tick` freed a
data stream after a RESET and bytes are missing, the frame fails at once
with `DataStreamReset`, and the reset code goes to the debug log. Obey the rule all the same. The note holds
the last 256 ends of a connection. A host that reads late, after many
streams ended, gets "end not known" (`streamRecvEnd` null and
`streamRecvWasReaped` true), which it must treat as a cut stream. Other
protocols on the same Driver need the same care.

**2. One socket for a `Server` and your own dials: give each datagram to your
dials first.** Since quic-zig v0.26.0, the first flight of a server is 1200
bytes, so it passes the Initial size gate of your `Server`. If the `Server`
gets the answer to your dial first, it takes it as a new connection (`feed`
returns `.accepted`). Your dial then never gets its answer, and the `Server`
holds a half-open connection. Do these steps for each datagram:

1. Find a live dial whose peer address is the source of the datagram and
   that owns the destination connection ID of the datagram
   (`Connection.ownsLocalCid`).
2. If you find one, give the datagram to that dial (`Connection.handle`).
3. If not, give the datagram to the `Server` (`Server.feed`).

capnp-zig never puts a `Server` and a dial on one socket: `Listener` binds
its own socket, and each `Connection.initClient` binds its own socket too.

## Native Resource Budgets

Native mode has the normal QUIC send queue budgets plus native-specific stream
budgets:

- `inline_frame_threshold`: frames at or below this size are encoded directly in
  ordered control frames.
- `max_control_frame_bytes`: maximum native control-envelope payload size. It
  must fit the native RPC envelope header plus the largest selected inline
  payload.
- `max_pending_data_streams`: maximum queued outbound large frames waiting on
  one-shot unidirectional data streams.
- `max_pending_data_bytes`: maximum queued outbound data-stream bytes. The
  inbound side also rejects any referenced data RPC frame larger than this
  budget.

QUIC's stream windows bound the same traffic from the other side.
`transport_params.initial_max_streams_uni` (default 8) is how many
unidirectional streams the peer may have open at once (quic v0.24.0 and
later; there is no lifetime cap), so it is how many large frames can be in
flight in each direction. An id comes back once its stream is fully closed,
about one round trip after it opened, so a window of `W` carries about
`W / RTT` large frames per second. When the window is full, the frame stays
at the head of the outbound queue and is retried after the next pump; this
never fails a connection, at any stream count. The native control stream is
the client's bidirectional stream 0 and holds no unidirectional slot.

A full window arrives as a burst, and the receiving kernel's UDP queue must
hold it. A datagram the kernel drops there is silent loss, and on Linux it
collapsed native bulk throughput 10-30x in our measurements. So the window
and the socket buffer go together:

- The transport asks for a 4 MiB `SO_RCVBUF` and `SO_SNDBUF` on every UDP
  socket it binds (`udp_socket_recv_buffer_bytes` and
  `udp_socket_send_buffer_bytes` on `ClientOptions` and `ServerOptions`;
  null keeps the OS default). The request is best effort. macOS grants it.
  Linux grants it only up to `net.core.rmem_max` / `wmem_max` (stock
  208 KiB, which the kernel doubles to 416 KiB) unless the process has
  `CAP_NET_ADMIN`. Windows keeps its OS default.
- The default window of 8 is the largest that held on a stock Linux server
  with that capped 416 KiB buffer. A window of 16 collapsed there in most
  runs, and so did a window of 8 with the 208 KiB OS default.
- On a Linux server, raise `net.core.rmem_max` and `net.core.wmem_max` to at
  least 4 MiB (or grant `CAP_NET_ADMIN`). With the full 4 MiB, a window of
  16 nearly doubles bulk throughput (33 vs 18 MB/s at 20 ms RTT with
  64 KiB frames), so raise `initial_max_streams_uni` on such hosts.
- `EmbeddedSession` runs on the embedder's socket, so size that socket
  yourself, for example with quic-zig's `transport.applyServerTuning`.

The doc comment on `defaultTransportParams` in
`src/rpc/transport/quic/options.zig` records the measurements behind both
defaults, and `bench-quic --transport native --mode bulk --rtt-ms N
--uni-window N --udp-buffer BYTES` reproduces them. The Linux build of the
bench also reports each socket's kernel drops.

Streams the protocol never uses are refused, so they cannot hold a place in
the window: any peer-opened bidirectional stream except the client's stream
0, and in baseline mode any peer-opened unidirectional stream. The transport
sends STOP_SENDING and, for a bidirectional stream, RESET_STREAM, both with
application error code `ApplicationCloseCode.protocol_error`
(`0x434e5002`). The connection stays up. `EmbeddedSession` refuses the same
streams from its `onStreamOpen` hook.

Invalid native budgets fail during `Connection.initClient`,
`Connection.initServer`, `Listener.init`, or `serverConfigFromOptions` with a
specific error:

- `error.NativeControlFrameLimitTooSmall`
- `error.NativePendingDataStreamLimitRequired`
- `error.NativePendingDataByteLimitRequired`
- `error.NativeInlineFrameExceedsControlFrameLimit`
- `error.NativeControlFrameLimitExceedsWireLimit`

Runtime native frame violations close the QUIC connection with
`rpc.transport.quic.ApplicationCloseCode.frame_error`. Locally, `Connection.closeStatus()`
records the typed close code and the underlying error. Detailed close reasons
are hidden on the wire by default; enable `ServerOptions.reveal_close_reason_on_wire`
only in controlled debugging environments.

## Production Defaults

For internet-facing QUIC servers, start from
`rpc.transport.quic.withProductionServerHardening()` and then opt into native mode if the
peer also supports it. The hardening preset enables Retry/NEW_TOKEN, listener
rate gates and stateless resets, and keeps 0-RTT and detailed wire close
reasons disabled. Its three keys are `retry_token_key`, `stateless_reset_key`
(both required) and `new_token_key` (optional).

```zig
const options = quic.withProductionServerHardening(.{
    .listen_addr = listen_addr,
    .tls_cert_pem = server_cert_pem,
    .tls_key_pem = server_key_pem,
    .mode = .native,
    .native = .{},
}, .{
    .retry_token_key = retry_key,
    .stateless_reset_key = try loadOrCreateResetKey(io, state_dir, "stateless-reset.key"),
    .new_token_key = new_token_key,
});
```

Recommended hardening posture:

- Provide stable, secret `retry_token_key` material and rotate it with your
  deployment's normal key-rotation process.
- Provide a persisted `stateless_reset_key` (see below). The preset requires
  it. Share one key between instances only when the load balancer routes by
  connection ID; otherwise give each instance its own.
- Provide `new_token_key` when you want returning clients to avoid Retry after
  address validation has already succeeded.
- Leave the preset's listener gates enabled, then tune
  `initial_source_rate_limit`, `listener_datagram_rate_limit`,
  `listener_byte_rate_limit`, and `source_byte_rate_limit` from production
  telemetry instead of disabling them during load tests. Each is a three-state
  `RateLimit`: `.default` (the library recommendation), `.disabled` (opt out),
  or `.{ .limit = n }`. There is deliberately no `null` — an optional could not
  distinguish "unset" from "turn this DoS mitigation off", and capnp-zig
  shipped exactly that confusion before v0.9.0.
- Keep `max_connection_memory`, `max_message_bytes`,
  `max_outbound_queue_items`, and `max_outbound_queue_bytes` bounded. Raise them
  only with matching application-level size limits.
- Register `log_callback` or `qlog_callback` for diagnostics in controlled
  environments, and rate-limit exposed log paths with
  `log_source_rate_limit`.
- For native mode, keep the default `NativeOptions` first. If large application
  frames are common, prefer raising `max_pending_data_bytes` within your message
  budget over making every frame inline.

### Stateless-reset key

When a server crashes and restarts, its clients still hold connections that the
new process knows nothing about. With a `stateless_reset_key`, the restarted
server answers their next packet with a stateless reset (RFC 9000 §10.3). The
client then closes with `DisconnectCause.stateless_reset`. That proves an
instance holding this key received the packet and had no state for the
connection. It proves that the server lost its state (a crash-restart) only
when packets of a live connection can reach no other instance that holds the
key; see "Sharing the key" below. Without the key the server drops those
packets silently. The client can prove nothing, waits for its idle timeout
(30 s by default), and closes with `DisconnectCause.idle_timeout`.
By default `WarmRedialClient` redials on `.stateless_reset` only, so without
the key it never heals. `Policy.redial_on_idle_timeout` makes it redial on
`.idle_timeout` too, but that heals only after the full idle timeout, on a
close that does not prove the server lost its state.

The key works only if a restarted server holds the **same** 32 bytes as the
process that crashed. A new key invalidates every token the old process issued.
So generate the key once from a CSPRNG and persist it next to the server's
other state. Keep it secret: anyone who has it can reset this server's
connections.

**Sharing the key.** Every instance that holds the key can make the reset token
for any connection ID, and a reset carries no other proof of its sender. So
RFC 9000 §21.11 requires that instances which share a static key are arranged
so that a packet with a given connection ID always reaches an instance that has
the connection's state, unless the connection is no longer active. A load
balancer that hashes the UDP address and port does not meet this. After a NAT
rebinding or a client migration, packets of a live connection reach a sibling
instance, which sends a valid reset and kills the connection. The client
certifies `.stateless_reset` for a server that never died, and
`WarmRedialClient` redials. An attacker who can change the source address of a
client's packets can cause the same reset on purpose. Choose one of these
layouts:

- One instance behind the address: persist one key and load it on every
  restart, as the recipe below does.
- Several instances behind one address, with routing by connection ID
  (QUIC-LB, draft-ietf-quic-load-balancers): the instances can share one key.
  A packet then reaches an instance without the connection's state only after
  the connection is gone. `ServerOptions` does not expose quic-zig's QUIC-LB
  connection-ID encoding (`Server.Config.quic_lb`) yet, so this needs a load
  balancer that tracks connection IDs itself.
- Several instances with any other routing: give each instance its own key,
  and persist each key with that instance's identity, so a restarted instance
  loads its own key again. A sibling's reset then carries the wrong token, and
  the client ignores it.

<!-- verbatim: tests/docs/quic_transport_snippets_test.zig -->
```zig
/// Owner read/write only, where the platform has POSIX modes.
const key_file_permissions: std.Io.File.Permissions =
    if (@hasDecl(std.Io.File.Permissions, "fromMode")) .fromMode(0o600) else .default_file;

/// Load this server's stateless-reset key, creating it on the first start.
/// Every later start, including a restart after a crash, reads back the
/// same 32 bytes.
fn loadOrCreateResetKey(io: std.Io, dir: std.Io.Dir, sub_path: []const u8) !quic.StatelessResetKey {
    if (try readResetKey(io, dir, sub_path)) |key| return key;

    var key: quic.StatelessResetKey = undefined;
    try io.randomSecure(&key);
    // Write a temporary file, then link it into place: a crash cannot leave
    // a short key file, and when two first starts race, the loser reads
    // the winner's key.
    var file = try dir.createFileAtomic(io, sub_path, .{ .permissions = key_file_permissions });
    defer file.deinit(io);
    try file.file.writeStreamingAll(io, &key);
    try file.file.sync(io);
    file.link(io) catch |err| switch (err) {
        error.PathAlreadyExists => return (try readResetKey(io, dir, sub_path)) orelse
            error.InvalidStatelessResetKeyFile,
        else => |e| return e,
    };
    // `sync` above made the bytes durable, not the new name. Sync the
    // directory that holds the name, or a power loss right after the first
    // start can drop the file, and the next start mints a new key. Name it
    // from `sub_path`: on Linux, `file.dir` can be `dir` itself even when
    // `sub_path` has directories.
    try syncDir(io, dir, std.fs.path.dirname(sub_path) orelse ".");
    return key;
}

/// Flush the entries of the directory at `dir_path` to disk. Opened as a
/// file because a `Dir` handle may be path-only (O_PATH on Linux), which
/// cannot be synced. Windows has no directory sync; NTFS journals the
/// entry itself.
fn syncDir(io: std.Io, dir: std.Io.Dir, dir_path: []const u8) !void {
    if (@import("builtin").os.tag == .windows) return;
    const handle = try dir.openFile(io, dir_path, .{});
    defer handle.close(io);
    try handle.sync(io);
}

fn readResetKey(io: std.Io, dir: std.Io.Dir, sub_path: []const u8) !?quic.StatelessResetKey {
    var key: quic.StatelessResetKey = undefined;
    var buf: [key.len + 1]u8 = undefined;
    const bytes = dir.readFile(io, sub_path, &buf) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => |e| return e,
    };
    if (bytes.len != key.len) return error.InvalidStatelessResetKeyFile;
    @memcpy(&key, bytes);
    return key;
}
```

A damaged key file is an error, never a silently regenerated key. The same
recipe works for `retry_token_key` and `new_token_key`, which are also 32
bytes. `tests/docs/quic_transport_snippets_test.zig` runs this recipe and
checks which directory it syncs (`zig build docs-smoke` fails if the block
above stops matching it), and `tests/rpc/transport/quic/rpc_quic_peer_test.zig`
crash-restarts a server built with the preset and checks that its client
certifies `.stateless_reset`.

### 0-RTT and warm restore

The preset refuses 0-RTT by default (`ServerProductionHardening.early_data =
.disabled`). A client that resumes with a session ticket still connects: its
early frames are sent again at 1-RTT, which costs one round trip and loses no
data.

To let a warm restore answer without that round trip, opt in with
`.early_data = .restore_only`:

```zig
const options = quic.withProductionServerHardening(base_options, .{
    .retry_token_key = retry_key,
    .stateless_reset_key = reset_key,
    .new_token_key = new_token_key,
    .early_data = .restore_only,
});
```

This sets `ServerOptions.early_data = .without_replay_protection` and
`ServerOptions.early_dispatch = .restore_only` together; the preset never sets
one without the other. 0-RTT data can be replayed by an attacker, and there is
no replay tracker here. So the server executes only the idempotent restore
prefix (Bootstrap frames and calls on the Restorer interface) before the
handshake completes. Every other frame, and everything behind it, waits for
the handshake, which a replay can never complete. Your Restorer must therefore
be idempotent, as the vat restore convention already requires. Native mode
holds every early frame until the handshake. Also set `new_token_key`: a
returning client that presents a valid NEW_TOKEN skips Retry, and so the
round trip a Retry costs. A Retry no longer costs the early restore: since
quic-zig v0.27.0 the client sends its 0-RTT data again after the Retry, and
the restore still runs before the handshake completes. A NEW_TOKEN is valid
only from the IP address and port that it was issued to, so
`WarmRedialClient` redials from the port of its previous connection; see
"Retry and NEW_TOKEN" below.

**The opt-in alone does not make a heal after a crash-restart ride 0-RTT.**
BoringSSL encrypts session tickets with a key that belongs to the server's
TLS context (one `SSL_CTX`). Unless the server installs a key, each process
makes its own random key, keeps it only in memory, and rotates it every 2
days. So a restarted server cannot decrypt the tickets that the crashed
process issued, and the first redial after a crash-restart (the redial
`WarmRedialClient` makes on `.stateless_reset`) takes a full handshake: no
resumption and no 0-RTT. That handshake issues a new ticket, so later
redials to the same process can resume. "Session-ticket key" below shows
how to persist the key, and states what that costs.

### Session-ticket key

**Status: Experimental.** `ServerOptions.session_ticket_key:
?*const SessionTicketKey` (48 bytes) persists the key, and
`ServerProductionHardening` has the same field. `previous_session_ticket_key`
(same type, in both) keeps the key before it open in a process that starts
after a key change ("Rotation" below). `serverConfigFromOptions`
copies the key into quic-zig's `Server.Config.session_ticket_key`, and
quic-zig installs it on every TLS context it builds: at `init`, and again on
a `.pem` reload (`replaceTlsContext`). `Listener.init` (so also `Server.init`,
`serve` and `Connection.initServer`) zeroes its copy of the config once
quic-zig's server is built, and keeps no pointer to yours: you may zero your
copy when `init` returns. A server that loads the same key on every start
decrypts the tickets that its crashed predecessor issued, so the heal
resumes, and with `.early_data = .restore_only` BoringSSL accepts its 0-RTT
data and the restore runs before the restarted server's handshake completes.
That holds behind a Retry too (quic-zig v0.27.0; "Retry and NEW_TOKEN"
below). `WarmRedialClient.Outcome.zero_rtt_generations` counts the
generations whose first flight, with its Restore, rode 0-RTT (the verdict was
`.accepted`). `Outcome.retried_generations` counts the generations whose dial
got a Retry and so paid one more round trip; a generation in both restored in
0-RTT behind a Retry. Under the preset, with `new_token_key` persisted too, a
heal after a crash-restart skips the Retry: it redials from the port that
earned its NEW_TOKEN, and the restarted server's token clock continues from
its predecessor's. A heal whose old port was taken dials from a new port,
gets a Retry, still restores early, and counts in `zero_rtt_generations`,
`retried_generations` and `Outcome.port_fallback_generations`. The key is
opt-in: it defaults to null, in the preset too. Set it only when a 0-RTT heal
is worth what a stolen key costs.

The security trade-off below was reviewed before the code (item 16 of
`docs/sprint-plan-2026-10-04.md`). Wire the key into the preset like this,
and load it with `quic.loadTicketKeyFile(io, state_dir,
"session-ticket.key")` on every start:

<!-- verbatim: tests/docs/quic_transport_snippets_test.zig -->
```zig
/// The hardened preset for a warm-restore server that persists its
/// session-ticket key. Load every key from its own file on every start.
/// `ticket_key` must stay alive until `Server.init` returns; the server keeps
/// no pointer to it after that.
fn warmRestartOptions(
    base_options: quic.ServerOptions,
    retry_key: quic.ServerRetryTokenKey,
    reset_key: quic.StatelessResetKey,
    new_token_key: quic.ServerNewTokenKey,
    ticket_key: *const quic.SessionTicketKey,
) quic.ServerOptions {
    return quic.withProductionServerHardening(base_options, .{
        .retry_token_key = retry_key,
        .stateless_reset_key = reset_key,
        // Persist it with the ticket key: a returning client with a valid
        // NEW_TOKEN skips the Retry and its round trip.
        .new_token_key = new_token_key,
        .early_data = .restore_only,
        .session_ticket_key = ticket_key,
    });
}
```

**What a thief who copies the key file can do:**

- Decrypt the recorded 0-RTT data of every dial whose ticket this key
  sealed. A ticket carries its session's resumption secret, and the 0-RTT
  keys derive from that secret and the ClientHello alone. In a
  `WarmRedialClient` heal, the 0-RTT data is the Bootstrap and Restore
  frames, with the sturdy ref that the Restore carries. When a sturdy ref is
  a bearer secret, the thief can then restore that capability. In a resumed
  dial, any other frame that the client queues before its handshake
  completes also travels in 0-RTT, although the server holds it until the
  handshake. This covers all traffic recorded while the key was in use, also
  after a rotation, for as long as the thief keeps the file.
- Impersonate the server to a client that offers a ticket this key sealed,
  until that ticket expires. A resumed handshake authenticates the server by
  the resumption secret, not by its certificate. The thief must also receive
  the client's packets: be on the path, or redirect them.
- Mint tickets that the server accepts. This gives the thief nothing that an
  ordinary dial does not: capnp-zig servers do not authenticate clients
  through TLS (`ServerOptions` has no client-certificate option), so a
  ticket carries no client identity to forge.

**What the thief cannot do:**

- Read recorded 1-RTT traffic, of resumed or full handshakes. BoringSSL
  resumes a TLS 1.3 ticket only in `psk_dhe_ke` mode, so every resumed
  handshake also mixes in a fresh (EC)DHE secret, which a recording does not
  contain. The server's answers to an early restore are 1-RTT traffic too.
- Impersonate the server to a client that has no ticket, or a ticket that
  another key sealed. That client runs a full handshake, which needs the
  certificate's private key.
- Forge stateless resets, Retry tokens or NEW_TOKEN tokens. Each has its own
  key.

Without the persisted key, the same theft needs the server process's memory,
and the key found there is rotated every 2 days.

**Rules for the key:**

- Generate all 48 bytes from a CSPRNG. BoringSSL
  (`SSL_CTX_set_tlsext_ticket_keys`) reads them as a 16-byte key name, a
  16-byte HMAC-SHA256 key and a 16-byte AES-128 key. The name travels in
  clear at the front of every ticket and tells the server which key sealed
  it.
- Keep the key in a file of its own: exactly 48 bytes, mode 0600, written
  atomically. The reset-key recipe above creates one with the key type
  changed to `quic.SessionTicketKey`. A damaged file is an error, never a
  silently regenerated key. `quic.loadTicketKeyFile(io, dir, sub_path)`
  reads exactly 48 bytes, refuses an all-zero file
  (`error.InvalidSessionTicketKeyFile`), and, on POSIX, refuses a file that
  the group or others may access, any of the mode bits 0o077
  (`error.SessionTicketKeyFilePermissions`), as ssh does for a private key.
  On Windows there are no mode bits to check: give the file an ACL that
  grants access to the service account only.
- Never derive it from the reset key, or the reset key from it. The two
  keys have opposite sharing rules. Instances may share a reset key only
  when the load balancer routes by connection ID ("Sharing the key" above).
  A ticket key may be shared by every instance behind one address, so that
  a client resumes on whichever instance it reaches; an instance with its
  own ticket key only costs a resuming client one full handshake. A key
  derived from the other would also leak with it.
- Persist `new_token_key` with it, and load both on every start. With Retry
  on (the preset always sets `retry_token_key`), only a valid NEW_TOKEN lets
  a returning client skip Retry and the round trip it costs. A new
  `new_token_key` at each boot invalidates every NEW_TOKEN, so every
  restarted client gets a Retry. The early restore survives either way; see
  "Retry and NEW_TOKEN" below.
- A `.pem` reload keeps the key. capnp-zig never reloads the TLS context
  itself. If you call quic-zig's `Server.replaceTlsContext(.{ .pem = ... })`
  through `Listener.server` (on the loop thread), quic-zig builds the new
  context with the configured key, and with the previous key after a
  rotation, so a certificate change costs no client its 0-RTT. Do not pass a
  context you built as `.override`: quic-zig refuses a key together with
  `tls_context_override`, and adds none of its TLS 1.3 pin, ALPN list,
  early-data setting or anti-replay hook to such a context.

**What is refused.** `serverConfigFromOptions`, and so `Listener.init`,
returns `error.InvalidConfig` for an all-zero key, for a key together with
`.early_data = .with_anti_replay` (see below), and for a
`session_ticket_lifetime_s` outside 1 second to 2 days. It refuses a
`previous_session_ticket_key` ("Rotation" below) with no
`session_ticket_key`, an all-zero one, and one whose name (first 16 bytes)
is the name of `session_ticket_key`. quic-zig's `Server.init` refuses the
key and previous-key cases as well. Until quic-zig v0.27.0,
capnp-zig also refused a key with Retry on and `new_token_key == null`,
because a Retry then cost the early restore; now it costs one round trip
per returning client, so the pair is allowed (the transport test "session
ticket key with Retry on and no new_token_key" pins what it costs).

`serverConfigFromOptions` returns a quic-zig config that holds a copy of the
key, and of the previous key, by value. An embedder that builds its own
quic-zig server from it (for example to host `EmbeddedSession`s) gets the
keys and lifetime with it: call `quic.zeroServerConfigSecrets(&config)` once
`quic.Server.init` has returned, as `Listener.init` does, and keep the config
out of logs. An embedder that feeds its own clock to that server also owns
the clock rules below.

A restarted server accepts 0-RTT data only when it runs with the same ALPN,
transport mode and `early_dispatch` as the process that issued the ticket.
The server binds the mode and `early_dispatch` into quic-zig's
`early_data_application_context` (quic-zig adds the primary ALPN and the
replay-relevant transport parameters), and BoringSSL refuses early data
whose context differs, or whose negotiated ALPN differs; the session still
resumes.

**Rotation.** `Server.rotateSessionTicketKey(&new_key)` changes the key with
no restart and no lost ticket (quic-zig v0.27.0). New tickets are sealed
under `new_key` at once. The key that sealed until then still opens tickets,
0-RTT included, for one ticket lifetime (`session_ticket_lifetime_s`, or 2
days), counted on the listener's clock (`Listener.nowUs`); then quic-zig
clears it. A second rotation before that drops it at once: the server holds
two keys, not more. A `.pem` reload keeps both. `error.InvalidConfig`, and
nothing changes, for a server built with no key, an all-zero key, or a key
whose name (first 16 bytes) is the current key's.

Call it on the loop thread, because quic-zig reads the keys inside
handshakes on that thread and takes no lock: from the `after_step` hook of
`Server.runWithAfterStep` or from the accept hook. To rotate on a signal
from another thread (a timer, an admin command), set a flag there, call
`server.wake()`, and rotate from `after_step`. Debug builds check the
thread; release builds check it only with `runtime_thread_checks`. Since
quic-zig v0.29.0, a Debug build of quic-zig checks it too: the first feed,
tick or rotation fixes its loop thread, and a call on any other thread
asserts. A rotation before `run` may still run on any thread, provided
`run` starts after it (spawning the loop thread after the rotation is
enough): the `Server`'s first step makes its thread quic-zig's loop thread
too, through quic-zig v0.30.1's `Server.adoptLoopThread()`. After that
first step the loop thread is fixed for good, in capnp-zig's check and in
quic-zig's. A rotation must END before `run` starts: a rotation on another
thread that is still running when the loop takes its first step is a data
race, and no check catches it (the first step moves quic-zig's latch to the
loop thread). `Listener.rotateSessionTicketKey` is the same call for code that
drives a `Listener` itself; it must run on the thread that feeds it.
A server-side `Connection` or `Listener` can move to another thread at a
quiescent point: the old thread has stopped, and the move happens-after its
last call (a thread join, say). On the new thread, call
`Connection.adoptOwnerThread` (for a server-side connection it also moves
quic-zig's loop thread) or `Listener.adoptLoopThread` before the first step.
With quic-zig v0.29.0 this panicked inside quic-zig in a Debug build once
the server had received a datagram. Release builds do not check, and a
client-side `Connection` has no quic-zig latch.
`serve` (`PeerServer`) has no loop-thread hook of its own: rotate through
`PeerServer.server` before `run`, or drive a `Server` with
`runWithAfterStep`. A rotation hook on `PeerServer` is a possible addition;
it is not built.

A rotation lives in one process. The next process starts with the keys you
give it: `session_ticket_key` seals new tickets, and
`previous_session_ticket_key` (quic-zig v0.29.0) still opens the tickets of
the key before, 0-RTT included. Persist a rotation like this:

1. Write the new key file atomically, and keep the old one.
2. Rotate the running server to the new key.
3. If the process restarts within one ticket lifetime after the rotation,
   start it with the new key as `session_ticket_key` and the old key as
   `previous_session_ticket_key`. Set `previous_session_ticket_key_until_us`
   to the time of the rotation plus one ticket lifetime, in microseconds
   since the Unix epoch (the listener's clock, `Listener.nowUs`): the old
   key then ends when it would have ended in the process before. Null gives
   it one more lifetime from the restart. A process that starts with the new
   key alone cannot open the tickets of the old one, and its clients each
   pay one full handshake (no outage).
4. After one ticket lifetime, delete the old key file, and stop passing it.

Every server of a pool, and the next process, needs the same change. A
restart with a new key file and no previous key is still valid; it costs
each client one full handshake, which issues a ticket under the new key.
`ServerProductionHardening` takes the pair as its own
`previous_session_ticket_key` and `previous_session_ticket_key_until_us`
fields, and sets both with `session_ticket_key`. Before quic-zig v0.29.0
the only way to keep the old tickets was to start with the old key and
rotate to the new one before the first datagram.

Rotate the key at least every 7 days, and at once when the file may have
leaked or when a host that held it is retired. BoringSSL already makes each
client run a full handshake at least every 7 days
(`SSL_DEFAULT_SESSION_AUTH_TIMEOUT`), so a weekly rotation adds no full
handshake through `rotateSessionTicketKey`, and at most one per client per
week through a restart with a new key file. The ticket lifetime bounds the
exposure after a rotation: tickets are valid for 2 days by default
(`SSL_DEFAULT_SESSION_PSK_DHE_TIMEOUT`), a client never offers an expired
ticket, and every ticket the old key sealed expires within one lifetime of
the rotation. So a thief with the old key can
impersonate the server to resuming clients for at most that long after the
rotation. Tickets that the thief hands out while it impersonates do not
extend this past 7 days: a client never resumes more than 7 days after its
last full handshake. Recorded 0-RTT data stays readable; a rotation only
limits how much traffic one key seals.

`ServerOptions.session_ticket_lifetime_s` shortens the 2 days, for the
tickets the server issues and for the time a rotated-out key keeps opening
them (1 second up to `quic.max_session_ticket_lifetime_s`, 2 days). A client
caps a ticket at the lifetime the server advertised with it. quic-zig accepts
up to 7 days, but capnp-zig keeps 2 days as the maximum: a BoringSSL client,
and so every quic-zig and capnp-zig client, keeps a ticket for 2 days unless
its own limit is raised (a 7-day server lifetime was measured stored as
172800 s), so a longer lifetime buys these clients nothing and keeps a
stolen key's tickets valid for longer.

`ClientOptions.session_ticket_lifetime_s` (Experimental, quic-zig v0.29.0)
is that client limit: the client keeps each ticket for the smaller of it and
the server's lifetime, and does not offer an older ticket. It takes 1 second
up to the same 2 days, so it only shortens. quic-zig's
`Client.resumptionTicketLifetimeSeconds(envelope)` reads the lifetime of a
saved envelope as the client keeps it.

**Why the key is refused together with anti-replay.** With `.early_data =
.with_anti_replay`, capnp-zig dispatches every early frame at once, because
the tracker promises that a flight runs only once; only
`.without_replay_protection` arms the hold. But the tracker lives in the
server's memory. Each instance has its own, and a crash loses every entry
added since the embedder last saved it (quic-zig can serialize a tracker).
Without a persisted key this is sound: a restart invalidates every
earlier ticket, and an instance cannot decrypt a sibling's tickets. With a
persisted key, a first flight recorded before the crash still resumes after
the restart. BoringSSL accepts its early data if the replay arrives within
about 60 seconds of the original (its ticket-age check), which covers a fast
restart. The empty tracker reports the flight as fresh, and every call in
it runs a second time. Instances that share the key are open to the same
replay from each other. The postures that remain do not depend on a
tracker: under `.restore_only` a replay can run only the idempotent restore
again, and under `.hold_until_handshake` it runs nothing, because a replay
never completes a handshake.

**Retry and NEW_TOKEN.** A server with Retry on (the preset) answers a
client's first flight with a Retry unless the client presents a valid
NEW_TOKEN, and it drops the 0-RTT packets of that flight, because no
connection exists for them yet. Both halves of what this used to cost are
closed:

- **The client sends its 0-RTT data again after a Retry (quic-zig
  v0.27.0).** RFC 9000 (section 17.2.5.3) lets a client send 0-RTT packets
  again after a Retry, to the connection ID that the Retry gives, and the
  quic-zig client now does. The restarted server reads them with the Initial
  that carries the Retry token, and runs the restore before its handshake
  completes. A Retry therefore costs one round trip, never the early restore:
  whatever the client's port, the token clock or the `new_token_key`, as long
  as the ticket key is the same. Through quic-zig v0.25.0 the client sent the
  data again only after the handshake, as 1-RTT, while BoringSSL still
  reported `.accepted` (quic-zig's finding F8; the history is in
  `docs/upstream/handoff-quic-zig-ticket-keys.md`).
- **The client skips the Retry and its round trip (capnp-zig v0.20.0).**
  quic-zig binds a NEW_TOKEN to the client's IP address and port, and a
  capnp-zig dial binds a new ephemeral port unless `ClientOptions.local_addr`
  names one. So `WarmRedialClient` dials every generation after the first
  from the local port of the generation before it: with `base.local_addr`'s
  IP address, or the unspecified address when `base.local_addr` is null. A
  `base.local_addr` that names a port is used as it is. The token clock also
  survives a restart (below). Under the preset, with the ticket key and
  `new_token_key` both persisted, a heal after a crash-restart therefore
  presents a valid NEW_TOKEN and gets no Retry. The same holds for a redial
  to a server process that is still running. The server checks the address
  it sees, so behind a NAT this works only when the NAT maps the reused local
  port to the same external port.

Under the preset these dials still get a Retry, and so pay one more round
trip before their early restore:

- A heal whose previous port another socket took. `WarmRedialClient` falls
  back to an ephemeral port and counts the dial in
  `Outcome.port_fallback_generations` (and in `retried_generations`).
- The first dial of a client: a new process binds a new port unless
  `ClientOptions.local_addr` names one. This includes a client seeded with
  `seedWarmState`, because the warm-state envelope carries no port.
- A heal whose first generation died before the server's NEW_TOKEN arrived
  (the server sends it after the handshake).
- A dial to a server with a new `new_token_key`, or with a NEW_TOKEN that
  expired or that the restarted server's clock reads as not yet valid (below).

The token clock survives a restart. quic-zig stamps a NEW_TOKEN's issue
and expiry times with the clock that the listener feeds it,
`Listener.nowUs`. That clock starts at the wall clock when the listener
starts, then advances on the monotonic clock, so it never goes backwards
within a process. A restarted listener's clock continues from its
predecessor's, and it accepts its predecessor's NEW_TOKENs within their
lifetime (`new_token_lifetime_us`, 24 hours by default). By default
quic-zig checks these times with no clock-skew allowance. If the wall clock
steps backwards between the two starts, or the predecessor's monotonic clock
ran ahead of the wall clock over a long uptime (on macOS it is not
NTP-disciplined), the restarted server reads the newest tokens as not yet
valid by that much, and those clients get a Retry.
`ServerOptions.new_token_max_clock_skew_us` (Experimental, quic-zig v0.29.0)
allows that much: a token from the future by at most this many microseconds
is taken, and one past its expiry by at most this much too. A few seconds
cover a small step. Before v0.20.0 the clock counted from `Listener.init`,
so a restarted server read every token from its predecessor as not yet valid
until its own uptime passed the predecessor's uptime at the time of issue.

`ServerOptions.new_token_clock` (Experimental, quic-zig v0.29.0) gives the
tokens a clock of their own, a `fn () u64` in microseconds; the timers keep
the listener's clock. capnp-zig's listener clock already goes on across a
restart, so leave it null unless every process must agree on another clock.
quic-zig's `unixWallClockUs` compiles from v0.30.1 (at the v0.29.0 pin it
called `std.time.microTimestamp`, which Zig 0.17.0 does not have, so it did
not compile). In embedded mode the host feeds its own clock to its quic-zig
server, so the host's clock needs the same property, or the host sets
`new_token_clock`. (quic-zig's own bundled loop,
`quic.transport.runUdpServer`, still feeds a clock that starts at zero, so
there a persisted `new_token_key` needs `new_token_clock`; capnp-zig does
not use that loop.)

So a heal after a crash-restart runs its restore early whenever the
restarted server loads the same ticket key (and the same ALPN, transport
mode and `early_dispatch`). With both keys persisted and the heal redialing
from the port that earned its NEW_TOKEN, it also skips the Retry.

The tests pin this. In the transport suite, "a server crash-restarted with
the same key accepts the resumed dial's 0-RTT" (no Retry), "after a Retry
the resumed dial's restore still arrives in 0-RTT (quic-zig F8 fixed)" (a
new port), "a new new_token_key after a crash-restart costs a Retry, not the
early restore" and "session ticket key with Retry on and no new_token_key"
count, at the server, the restore bytes that arrived in 0-RTT packets and
whether the restore ran before the handshake. In the peer suite,
"WarmRedialClient heal after a crash-restart of a hardened server with a
session-ticket key rides 0-RTT from the port that earned its NEW_TOKEN" and
"WarmRedialClient heal falls back to an ephemeral port when its previous port
is taken, and pays a Retry" pin the client's counters. The transport tests "a
server restarted with a new key and the old one as
previous_session_ticket_key resumes the old ticket in 0-RTT", "a
previous_session_ticket_key whose time is over opens no ticket" and
"new_token_clock and new_token_max_clock_skew_us" pin the v0.29.0 options
across a restart.

To see which case a client is in, compare the counters:
`WarmRedialClient.Outcome.zero_rtt_generations` counts the generations whose
restore rode 0-RTT, `retried_generations` counts the generations that paid a
Retry (a generation in both rode 0-RTT behind the Retry), and
`port_fallback_generations` counts the generations that could not reuse the
previous port.

### Self-healing clients

`rpc.transport.quic.WarmRedialClient` keeps a restored capability alive across
server crash-restarts. When a connection ends with `.stateless_reset`, it dials
a new connection (offering the latest session ticket, which a restarted server
cannot accept unless it loads the same session-ticket key; see "Session-ticket
key" above), restores the saved sturdy ref again, and hands the new capability
to `on_rebind`. It redials on `.idle_timeout` only when
`Policy.redial_on_idle_timeout` is set.

Each redial binds the local UDP port of the connection before it again, so
the NEW_TOKEN that connection received stays valid and a server with Retry
on lets the redial skip the Retry and its round trip ("Retry and NEW_TOKEN"
above). The client keeps only the port: the IP address is
`base.local_addr`'s, or the unspecified address when `base.local_addr` is
null, and a `base.local_addr` that names a port is used as it is. When the
port cannot be bound (another socket took it), the redial falls back to an
ephemeral port and counts it in `Outcome.port_fallback_generations`; its
restore still rides 0-RTT, behind a Retry.

`Policy.max_redials` (default 3) counts **consecutive** failures, not a
lifetime total. Each redial spends one. A generation resets the count to zero
when the server proves that it stayed alive for `Policy.min_healthy_ms`
(default 10 s) after the rebind: an authenticated packet from the server
arrives at least that long after the rebind
(`Connection.lastAuthenticatedReceiveNs`). The time the client needs to detect
a death does not count. A stateless reset is not authenticated, and an idle
connection receives nothing. So a client that calls only every 15 s, or that
waits out an idle timeout, cannot make a dead generation look healthy. A
long-lived client heals every crash that a proven-healthy period separates
from the last one. A server that dies in every generation (a crash loop) still
makes the client give up after `max_redials` redials, however late the client
notices each death.

The cost: a generation counts as healthy only when the application has traffic
with the server `min_healthy_ms` after the rebind. A client that stays idle
spends one redial on each death, as a lifetime budget would. With
`redial_on_idle_timeout`, each idle timeout spends one too. `Outcome.redials`
is the streak at exit; `Outcome.total_redials` counts every redial.

## Current Limits

- One server `rpc.transport.quic.Connection` owns one listener and represents one active
  QUIC session. Use `rpc.transport.quic.serve` (or `rpc.transport.quic.Server`)
  for multi-session fanout.
- A lost CONNECTION_CLOSE is sent again only when the peer sends another
  packet (quic-zig v0.29.0: the first packet after the close earns it again,
  then two more, then four). A client whose refusal close is lost during the
  handshake gets it again at its next probe. A peer that sends nothing more
  waits for its own handshake timeout (`ClientOptions.handshake_timeout_ms`,
  30 s by default), or after the handshake for its idle timeout. Keep both
  timeouts.
- capnp-zig sends no keep-alive of its own. A peer that dies silently (no
  CONNECTION_CLOSE, no stateless reset) is noticed only by the idle timeout
  (`transport_params.max_idle_timeout_ms`, 30 s by default): one idle
  timeout after the first send it leaves unanswered, or after the last
  packet received when nothing is sent. With quic-zig v0.30.1 (capnp-zig
  v0.21.0) a session that kept sending took two to three idle timeouts. The
  timeout is at least three probe timeouts, about 3 s before the first RTT
  sample.
- A QUIC stream's send buffer is quic-zig's `max_buffered_send` default,
  1 MiB; capnp-zig does not expose the knob.
- Native mode carries complete RPC frames only. It does not yet expose
  application-level streaming parameters or results.
- Mode mismatch is treated as malformed transport input and closes cleanly.
- 32-bit x86 (for example `x86-linux-musl`): capnp-zig builds quic-zig and
  BoringSSL with `sanitize-c = "trap"`. There, BoringSSL's 32-bit P-256 code
  (`third_party/fiat/p256_32.h`, under ECDSA verify) can trap in some TLS
  handshakes (quic-zig records it in its v0.28.1 notes, for boringssl-zig to
  fix; boringssl-zig is unchanged through quic-zig v0.32.0, so it is still
  open). capnp-zig's CI compiles QUIC for 32-bit x86 but does not run it there.
  64-bit targets are not affected.
