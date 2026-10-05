# HANDOFF — quic-zig: session-ticket keys as a server config

> **Status: OPEN (written 2026-10-04, against quic-zig v0.25.0 and
> boringssl-zig 0.6.7 / BoringSSL `aef0e2df`).** This is a document for
> the quic-zig maintainers, not a filed issue. Nothing here blocks
> capnp-zig: it ships a bridge (below) and will drop it when quic-zig
> ships the config field.

For the agent working on nullstyle/quic-zig. Self-contained; the
evidence comes from capnp-zig (same machine at
/Users/nullstyle/prj/zig/capnp-zig). Design and security trade-off:
"Session-ticket key" in capnp-zig's `docs/quic-transport.md`.

## Why a downstream needs it

A capnp-zig server that crash-restarts cannot decrypt the session
tickets its predecessor issued, because BoringSSL's default ticket key is
random per `SSL_CTX` and lives only in memory. So the client's first
redial after a crash-restart (capnp-zig's `WarmRedialClient` heal) takes
a full handshake: no resumption, no 0-RTT. Persisting the 48-byte ticket
key (`SSL_CTX_set_tlsext_ticket_keys`) fixes that. quic-zig v0.25.0 has
no way to configure it.

## What capnp-zig ships today (the bridge)

`ServerOptions.session_ticket_key: ?*const [48]u8` (and
`session_ticket_lifetime_s: ?u32`). capnp-zig's `Listener.init` calls
`quic.Server.init`, then, before the first datagram is fed, installs the
key on `server.tls_ctx.inner` through `boringssl.raw` (the boringssl
module quic-zig exports) and reads it back to compare
(`src/rpc/transport/quic/session_ticket.zig`). It also forwards its own
`Config.early_data_application_context` (transport mode +
`early_dispatch`).

The bridge couples capnp-zig to the field `Server.tls_ctx`. A rename
breaks the build; a change in meaning (another context serving
handshakes) is caught by capnp-zig's tests, which resume across a
restart and expect BoringSSL to accept the 0-RTT. It cannot survive
`replaceTlsContext`: a `.pem` reload builds a context with a fresh random
key.

## The asks

### 1. A config field installed in `buildServerContext`

Add `Server.Config.session_ticket_key: ?*const [48]u8 = null` (or a
by-value `?[48]u8`) and install it in `buildServerContext`
(`src/Server/tls_lifecycle.zig:172`), right after the context is built:

```c
SSL_CTX_set_tlsext_ticket_keys(ctx, key, 48)  // returns 1; 0 if len != 48
```

Because `replaceTlsContext({ .pem = ... })` already rebuilds through
`buildServerContext` (`tls_lifecycle.zig:219`), a `.pem` reload then keeps
the key, and tickets survive a certificate rotation as well as a restart.
Refuse an all-zero key with `InvalidConfig`; a zeroed buffer is a missing
key. Reading the key back (`SSL_CTX_get_tlsext_ticket_keys`) and
comparing is cheap and catches a setter that silently did nothing.

Facts for the doc comment (BoringSSL `aef0e2df`):

- The 48 bytes are a 16-byte key name (sent in clear at the front of
  every ticket), a 16-byte HMAC-SHA256 key and a 16-byte AES-128 key
  (`ssl/ssl_lib.cc:1795-1817`).
- A manually set key never auto-rotates (`next_rotation_tv_sec = 0`);
  the default key rotates every 2 days
  (`SSL_DEFAULT_TICKET_KEY_ROTATION_INTERVAL`, `ssl_session.cc:272-317`).
- TLS 1.3 resumes only in `psk_dhe_ke` mode, so a stolen key does not
  decrypt recorded 1-RTT traffic. It does decrypt recorded 0-RTT data,
  and it lets the thief impersonate the server to clients that offer a
  ticket it sealed, until those tickets expire.
- Refuse the key together with `EarlyData.with_anti_replay`, or document
  why not: the tracker is per-process memory. With a persisted key, a
  first flight recorded before a crash resumes after the restart within
  BoringSSL's 60 s ticket-age window, and the empty tracker calls it
  fresh. capnp-zig refuses the combination.

### 2. Rotation on the loop thread that keeps the previous key

`SSL_CTX_set_tlsext_ticket_keys` holds ONE key: it drops the previous one
(`ctx->ticket_key_prev.reset()`, `ssl_lib.cc:1815`) and takes no lock.
So today a rotation is a restart with a new key file, and every client
pays one full handshake. Please add something like
`Server.rotateSessionTicketKey(new_key)` that:

- runs on the loop thread (document it; the setter takes no lock, and a
  handshake on another thread may read `ticket_key_current`);
- encrypts new tickets under the new key and still decrypts tickets that
  the previous key sealed, until they expire. That needs the callback
  form (`SSL_CTX_set_tlsext_ticket_key_cb`, `ssl.h:2447-2471`) or an
  `SSL_TICKET_AEAD_METHOD` (`ssl.h:2521`), matching on the 16-byte name;
- documents the cadence it is built for (capnp-zig tells operators to
  rotate at least every 7 days, BoringSSL's
  `SSL_DEFAULT_SESSION_AUTH_TIMEOUT`).

### 3. A ticket-lifetime setting

Add `Server.Config.session_ticket_lifetime_s: ?u32` and apply it in
`buildServerContext` with `SSL_CTX_set_session_psk_dhe_timeout`. The
default is 2 days (`SSL_DEFAULT_SESSION_PSK_DHE_TIMEOUT`, `ssl.h:2209`).
With a persisted key, the lifetime bounds how long a thief can
impersonate the server after a rotation, so operators want to shorten
it. A TLS 1.3 client caps a ticket at the advertised lifetime
(`tls13_client.cc:1266-1270`). capnp-zig accepts 1 s to 2 days.

### 4. Fix the `.override` advice in the `replaceTlsContext` doc

`src/Server.zig:1692-1701` ("Resumption note") tells embedders who need
cross-reload resumption to configure ticket keys on a context of their
own and pass it as `.override`. Following that advice silently drops
what `buildServerContext` sets: the TLS 1.3 pin, the ALPN list, the
early-data flag and the anti-replay hook. It is also refused when the
server was initialized with `client_ca_pem`. Once ask 1 exists, point the
note at the config field and a `.pem` reload instead. Until then, the
safe advice is "install the key on the new context on the loop thread,
before the next datagram is fed", which is what capnp-zig documents.

## Related, separate: the client drops its 0-RTT after a Retry

Not part of the ticket-key field, but it decides whether a persisted key
saves the round trip. When a server answers a resumed client's first
flight with a Retry, the server drops the first flight's 0-RTT packets
(no connection exists for them yet). The v0.25.0 client then keeps those
packets as in flight and sends the data again only after the handshake,
as 1-RTT:

- `handleRetry` (`src/Connection/recv_packet_handlers.zig:290-331`) calls
  `resetInitialRecoveryForRetry` (`src/Connection.zig:3632-3648`), which
  re-queues only the Initial CRYPTO data;
- the 0-RTT packets stay in the application-space tracker, whose probe
  timer is held until the handshake is confirmed (RFC 9002 6.2.1);
- `requeueRejectedEarlyData` (`Connection.zig:3714`) runs only on a TLS
  rejection (`refreshEarlyDataStatus`, `:3694-3698`), never on a Retry,
  although `canSendEarlyData` (`:3651-3657`) still allows 0-RTT.

RFC 9000 17.2.5.3 lets the client send 0-RTT again after a Retry, to the
Retry's connection ID (and RFC 9002 6.3 says a Retry resets loss
recovery). BoringSSL still reports the early data as accepted, so the
client's `earlyDataStatus()` says `.accepted` while the data arrived
late.

Evidence: a capnp-zig scratch probe patched a copy of the v0.25.0 client
to call `requeueRejectedEarlyData()` at the end of `handleRetry` (after
`resetInitialRecoveryForRetry`). The unpatched v0.25.0 server accepted
the resent 0-RTT, which arrives with the token-bearing Initial, and
dispatched it before its handshake completed: after a restart with the
same ticket key, from a new port, and with a new `new_token_key`. A
restart without the key still rejected the early data and delivered the
data exactly once. 14 cases x 2 rounds, identical on macOS and arm64
Linux. capnp-zig's test "session ticket key: a new new_token_key after a
crash-restart costs the early restore" pins today's late delivery, and
will go red when the client changes.

A small related ask: a public accessor such as
`Connection.retryAccepted() bool` on the client. Because `.accepted` does
not mean the early data rode 0-RTT, capnp-zig's `WarmRedialClient` counts a
generation as 0-RTT only when its dial accepted no Retry, and counts the
others in `Outcome.retried_generations`. v0.25.0 has no accessor, so
capnp-zig reads the `Connection.retry_accepted` field
(`src/rpc/transport/quic/warm_redial.zig`); a rename breaks its build.

## The NEW_TOKEN clock: fixed in capnp-zig, three small asks

quic-zig stamps a NEW_TOKEN's issue and expiry times with the `now_us`
that the embedder feeds, and checks them against it
(`Server/dos.zig:367`, `max_clock_skew_us = 0`). The same `now_us` drives
every recovery timer. capnp-zig used to feed a clock that counted from its
listener's start, so a restarted process read its predecessor's tokens as
not yet valid until its own uptime passed the issue time. capnp-zig now
(v0.20.0) starts that clock at the wall clock in `Listener.init` and
advances it on the monotonic clock, so a token survives a restart. Its
test "a NEW_TOKEN from before a crash-restart skips the restarted server's
Retry" pins this. What is left belongs to quic-zig:

- quic-zig's own loop has the old bug. `transport/udp_server.zig` feeds a
  `now_us` that counts from the loop's start (`:563-569`), so a restarted
  `udp_server` sends every returning client a Retry until its uptime passes
  the issue time. Anchor that clock to the wall clock at start, as
  capnp-zig does, or see the next ask.
- Stamp and check NEW_TOKEN times with a wall clock of their own (for
  example a `Config` clock callback, or a wall-clock argument to `feed`),
  apart from the monotonic timer clock. One `now_us` cannot be both a
  timer clock that never jumps and a clock that agrees across processes;
  capnp-zig's anchor is a compromise (next ask).
- Allow clock skew when checking a NEW_TOKEN. `applyRetryGate` passes no
  `max_clock_skew_us`, so the check allows none. A restarted server whose
  wall clock stepped back, or whose predecessor's monotonic clock ran fast
  over a long uptime (macOS `CLOCK_UPTIME_RAW` is not NTP-disciplined:
  parts per million of the uptime), reads the newest tokens as not yet
  valid and sends a Retry. A `Config.new_token_max_clock_skew_us` of a few
  seconds would absorb that. It also extends the expiry edge by the same
  amount, which the 24-hour default lifetime makes negligible.

## When quic-zig ships asks 1-3

capnp-zig moves `session_ticket_key` and `session_ticket_lifetime_s` onto
the new config fields, deletes `session_ticket.zig`'s post-init install,
and keeps its own refusals (all-zero key, key + anti-replay, key + Retry
without `new_token_key`). Embedded mode (an embedder-owned quic-zig
server) becomes in scope at the same time.
