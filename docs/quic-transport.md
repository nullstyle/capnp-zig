# QUIC RPC Transport Guide

capnp-zig's QUIC transport is optional. Normal builds keep `quic-zig` and
BoringSSL out of the dependency graph; build with `-Dquic=true` when an
application wants `rpc.quic.Connection`.

```bash
zig build -Dquic=true test-rpc-quic --summary all
```

The transport uses ALPN `capnp-rpc/1`. One QUIC connection represents one
Cap'n Proto RPC vat session. The payload above the QUIC transport is still the
standard `rpc.capnp` message stream; QUIC changes how complete RPC frames move
between peers, not the RPC protocol that `Peer` handles.

## Modes

`rpc.quic.ClientOptions.mode` and `rpc.quic.ServerOptions.mode` default to
`.baseline`. Both sides must choose the same mode explicitly when using
`.native`; the mode is not negotiated with a separate ALPN.

| Mode | Stream Layout | Use When |
| --- | --- | --- |
| `baseline` | Client-initiated bidirectional stream 0 carries 32-bit little-endian length-delimited RPC frames. | You want the most conservative QUIC port of the TCP transport. This is the default. |
| `native` | Bidirectional stream 0 carries a native preface, versioned hello, and ordered control envelopes. Small RPC frames are inline; large RPC frames move over one-shot unidirectional data streams referenced by ordered control frames. | You want QUIC-native stream routing and are comfortable opting both peers into the newer wire shape. |

Native mode preserves Cap'n Proto E-order. Control frames are processed in
control-stream order. If a `data_rpc` control frame is next but the referenced
unidirectional data stream has not completed, later control frames stay buffered
and are not dispatched yet.

QUIC DATAGRAM is not used by either mode. Telemetry and sideband data should be
designed as a transport-general facility, not as a QUIC-only extension.

## Opting Into Native Mode

Use `rpc.quic.NativeOptions` on both client and server:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");

const quic = capnpc.rpc.quic;

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

## Listener And Session Boundary

`rpc.quic.Connection.initServer()` is the compatibility entry point for the
current one-session transport. Internally it owns a `rpc.quic.Listener`, accepts
the first server-side `rpc.quic.AcceptedSession`, and drives that session through
the existing `Connection.start()` callbacks.

The lower-level boundary is also public for future fanout work:

- `rpc.quic.Listener` owns the UDP socket and `quic_zig.Server`.
- `rpc.quic.Session` is a borrowed handle for one accepted server slot.
- `rpc.quic.AcceptedSession` carries the borrowed session plus its listener slot
  ordinal.
- `rpc.quic.AcceptedSessionDriver` attaches, drives, and reaps the one accepted
  session used by the compatibility connection.
- `rpc.quic.EndpointDriver` is the shared run-loop boundary for endpoint-specific
  socket, timer, inbound datagram, outbound datagram, and session-reaping work.
- `rpc.quic.ServerEndpoint` pairs a listener with the accepted-session driver
  currently attached to the compatibility connection.

`ServerOptions.max_concurrent_connections` must remain
`rpc.quic.supported_max_concurrent_sessions` for now. Raising that value will
need a separate accept loop that hands sessions to independent peer transports.

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
`rpc.quic.ApplicationCloseCode.frame_error`. Locally, `Connection.closeStatus()`
records the typed close code and the underlying error. Detailed close reasons
are hidden on the wire by default; enable `ServerOptions.reveal_close_reason_on_wire`
only in controlled debugging environments.

## Production Defaults

For internet-facing QUIC servers, start from
`rpc.quic.withProductionServerHardening()` and then opt into native mode if the
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

## Current Limits

- One server `rpc.quic.Connection` owns one listener and represents one active
  QUIC session. Broad server fanout is still a future transport layer.
- Native mode carries complete RPC frames only. It does not yet expose
  application-level streaming parameters or results.
- Mode mismatch is treated as malformed transport input and closes cleanly.
