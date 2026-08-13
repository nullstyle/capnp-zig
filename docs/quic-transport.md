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
per-root floors are 26 + 1 + 17 + 8 = 52 tests. The build step fails immediately
when `-Dquic=true` is absent, and each root is a direct build dependency, so the
gate neither parses test output nor relies on a CI-only shell or package.

CI is configured to run the Debug + ReleaseSafe pair from each operating
system's native shell on Linux, macOS, and Windows. Linux also runs the full
repository build/check/test/API/docs surface against the QUIC-enabled root;
that broader root-wide gate is not duplicated on macOS or Windows.

Current evidence is intentionally stated narrowly: macOS passes all 61 tests
from the four roots in both Debug and ReleaseSafe. The Windows QUIC tree passes
full-tree test cross-compilation (113/113), but that is not runtime evidence.
The native Windows no-skip lane remains a hosted acceptance gate after
capnp-zig itself is pushed; do not infer Windows runtime parity from the
dependency pushes or cross-compilation result.

The transport uses ALPN `capnp-rpc/1`. One QUIC connection represents one
Cap'n Proto RPC vat session. The payload above the QUIC transport is still the
standard `rpc.capnp` message stream; QUIC changes how complete RPC frames move
between peers, not the RPC protocol that `Peer` handles.

The manifest pins the `quic` package at annotated tag `v0.12.0` (commit
`0a0dbed`), which in turn pins
the published boringssl-zig commit `292c70a`. That BoringSSL wrapper links
Windows sockets as `ws2_32` with package-config lookup disabled, removing the
native-shell and Git Bash `pkg-config.BAT` failure path. Connection and server
session loops drive `Connection.advance()` before waiting on datagrams and again
during active service, then tick timers and drain outbound datagrams.

The public API is intentionally close to the TCP transport while the QUIC layer
is still maturing. Applications should treat `rpc.transport.quic.Connection` as the primary
entry point for one client/server session; the whole QUIC module remains
Experimental. Servers that need one UDP listener to
host multiple sessions should use `rpc.transport.quic.Server`, which accepts up to
`ServerOptions.max_concurrent_connections` and exposes one `ServerSession`
transport driver per accepted QUIC connection.

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
unidirectional data stream has not completed, later control frames stay buffered
and are not dispatched yet.

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

The eight end-to-end `Peer` cases exercise a verified-CA baseline session;
native Bootstrap/Call/Return/Finish; a pipelined call on the returned
capability; a native large-frame data stream; graceful and abrupt close;
two-session fanout; and fanout close isolation. The fanout server allocates
sessions at stable heap addresses before a `Peer` borrows the transport, and it
detaches that binding before reaping a session.

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
each accepted QUIC slot into a `rpc.transport.quic.ServerSession`, and lets callers poll
for sessions, attach callbacks per session, and drive either one chosen session
or all sessions. It keeps the wire behavior identical to `Connection`: the ALPN
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

Use `rpc.transport.quic.Server` when `ServerOptions.max_concurrent_connections` is greater
than one. Keep `Connection.initServer()` for compatibility tests, examples, and
single-session peers.

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
peer also supports it. The hardening preset enables Retry/NEW_TOKEN and listener
rate gates while keeping 0-RTT and detailed wire close reasons disabled.

```zig
const options = quic.withProductionServerHardening(.{
    .listen_addr = listen_addr,
    .tls_cert_pem = server_cert_pem,
    .tls_key_pem = server_key_pem,
    .mode = .native,
    .native = .{},
}, .{
    .retry_token_key = retry_key,
    .new_token_key = new_token_key,
});
```

Recommended hardening posture:

- Provide stable, secret `retry_token_key` material and rotate it with your
  deployment's normal key-rotation process.
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

## Current Limits

- One server `rpc.transport.quic.Connection` owns one listener and represents one active
  QUIC session. Use `rpc.transport.quic.Server` for multi-session fanout.
- `rpc.transport.quic.Server` is poll-driven. It does not yet provide a high-level accept
  event abstraction; callers inspect `sessionCount()`/`sessionAt()` and attach
  callbacks to accepted `ServerSession` values.
- Native mode carries complete RPC frames only. It does not yet expose
  application-level streaming parameters or results.
- Mode mismatch is treated as malformed transport input and closes cleanly.
