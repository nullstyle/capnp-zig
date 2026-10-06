# HANDOFF — quic-zig: session-ticket keys as a server config

> **Status: DELIVERED in quic-zig v0.27.0 (tag `9d2ab6e`, 2026-10-05);
> capnp-zig moved onto it on the same pin, and then to v0.28.1, which keeps
> it. The three candidates at the end were DELIVERED in quic-zig v0.29.0
> (tag `b9a15e6`, 2026-10-06), and capnp-zig adopted them on that pin.
> The one ask made after that, a way to move v0.29.0's Debug loop-thread
> latch, was DELIVERED in quic-zig v0.30.1 (tag `ccf6ae2`, 2026-10-06) as
> `Server.adoptLoopThread()`; capnp-zig pins v0.30.1 and does not call it
> yet (section at the end).** Written 2026-10-04 against
> quic-zig v0.25.0 and boringssl-zig 0.6.7 / BoringSSL `aef0e2df`. This is
> a document for the quic-zig maintainers, not a filed issue. The asks
> below are kept as written; "Delivered" and "Left as candidates" at the
> end record the outcome.

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

## What capnp-zig shipped before v0.27.0 (the bridge, removed)

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

### The F8 repro at capnp-zig's seam

quic-zig's finding F8 ("no 0-RTT resend after a Retry") has a repro in
capnp-zig that asserts today's behavior:

- File: `tests/rpc/transport/quic/rpc_quic_transport_test.zig`
  (root `test-rpc-quic`, so `zig build -Dquic=true test-rpc-quic`).
- Test: `session ticket key: after a Retry the resumed dial's restore
  arrives at 1-RTT, not 0-RTT (quic-zig F8)`.
- Setup: the hardened preset (Retry on, `new_token_key`, `.restore_only`)
  with a persisted ticket key. Dial 1 earns a session ticket and a
  NEW_TOKEN; the server crash-restarts with the same keys; dial 2 resumes
  with both from another local port, so its NEW_TOKEN is not valid and the
  restarted server sends a Retry. Dial 2 enqueues one RPC frame (48 bytes)
  before its loop starts, so it goes out as 0-RTT.
- What it counts at the server: `early_bytes`, the RPC frame bytes the
  server dispatched before its handshake completed (a server reads no 1-RTT
  data before then, RFC 9001 5.7, so these arrived in 0-RTT packets), and
  `stream_saw_early_data`, quic-zig's own `streamArrivedInEarlyData(0)` on
  the RPC stream. A same-port control in the same test gets no Retry and
  counts the whole frame: `early_bytes == 48`, `stream_saw_early_data`.

The exact flip. Today (v0.25.0) the retried dial asserts `retries_sent ==
1`, `status == .accepted`, `early_bytes == 0`, `!stream_saw_early_data` and
`!restored_before_handshake`. When the client sends its 0-RTT again after a
Retry (RFC 9000 17.2.5.3; model: `requeueRejectedEarlyData()` at the end of
`handleRetry`), the last three flip to `early_bytes == frame_len` (48),
`stream_saw_early_data` and `restored_before_handshake`; `retries_sent`
stays 1 and `status` stays `.accepted`. Two other capnp-zig tests go red at
the same time, as intended:

- `session ticket key: a new new_token_key after a crash-restart costs the
  early restore` (same file): `!fresh.restored_before_handshake` fails.
- `WarmRedialClient heal falls back to an ephemeral port when its previous
  port is taken, and pays a Retry`
  (`tests/rpc/transport/quic/rpc_quic_peer_test.zig`): `early_restores`
  becomes 1 instead of 0.

Checked on 2026-10-04: a scratch copy of v0.25.0 with that one-line patch,
used through `zig build --fork=<copy>`, turns exactly these three tests red
and nothing else, on macOS and in the arm64 Linux container
(aarch64-linux-musl). The F8 test with the flipped assertions passes
against the patched copy (run on macOS). When a quic-zig release fixes F8,
capnp-zig flips these assertions and counts a retried dial whose restore
ran early in `WarmRedialClient.Outcome.zero_rtt_generations`.

capnp-zig's own half of the gap is closed (v0.20.0): `WarmRedialClient`
redials from the local port of the connection before it, so a heal under
the preset presents a valid NEW_TOKEN and skips the Retry. The F8 fix still
matters for every dial that gets a Retry anyway: a heal whose old port was
taken, a client's first dial from a new process, a new `new_token_key`, or
an expired token.

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

*Done on the v0.27.0 pin, with one change: the key + Retry without
`new_token_key` refusal was dropped, because after ask 5 a Retry costs a
round trip, not the early restore.*

## Delivered in quic-zig v0.27.0

| Ask | quic-zig v0.27.0 | capnp-zig |
|---|---|---|
| 1. Config field | `Server.Config.session_ticket_key: ?SessionTicketKey` (48 bytes, by value), installed on every context the Server builds, a `.pem` reload included. `InvalidConfig` for 48 zero bytes, a key with `tls_context_override`, a key with `.with_anti_replay`. | `serverConfigFromOptions` copies `ServerOptions.session_ticket_key` into it; `Listener.init` zeroes its copy after `Server.init`. The bridge (`session_ticket.install`, the read-back) and the library's `boringssl` import are gone; a QUIC test root still imports quic's exported `boringssl` to read a ticket's lifetime. |
| 2. Rotation that keeps the previous key | `Server.rotateSessionTicketKey(new_key, now_us)`: new tickets under `new_key`, the old key opens tickets for one lifetime from `now_us`, two keys at most, a `.pem` reload keeps both. Feed thread only; not checked. | `Server.rotateSessionTicketKey(&key)` (loop-thread check, `Listener.nowUs` as `now_us`) and `Listener.rotateSessionTicketKey`. |
| 3. Ticket lifetime | `Server.Config.session_ticket_lifetime_s: ?u32`, 1 to 604800. | Passed through; capnp-zig keeps its 2-day maximum, because a BoringSSL client keeps a ticket 2 days at most (604800 was stored as 172800). |
| 4. The `.override` advice | The `replaceTlsContext` note points at the config field and a `.pem` reload. | `docs/quic-transport.md` says a `.pem` reload keeps the key. |
| 5. 0-RTT again after a Retry (F8) | The client queues its 0-RTT data again after a Retry. | The three F8-pinned assertions flipped exactly as listed above (transport: "after a Retry the resumed dial's restore still arrives in 0-RTT (quic-zig F8 fixed)", "a new new_token_key after a crash-restart costs a Retry, not the early restore"; peer: the port-fallback heal's `early_restores` 0 -> 1). `WarmRedialClient` counts such a dial in `zero_rtt_generations`. |
| The accessor | `Connection.retryAccepted() bool`. | `warm_redial.zig` reads it; the field coupling is gone. |

The same tag (and v0.26.0, which capnp-zig skipped as a pin) changed two
more things that capnp-zig's tests saw:

- A close during the handshake reaches the client (v0.26.0). capnp-zig's
  accept-hook rejection tests now see `DisconnectCause.peer_close` instead
  of `.handshake_timeout`. It also exposed a capnp-zig liveness gap: the
  client loop ended a closed connection only once its outbound queue was
  empty, and frames queued before the handshake never leave. Fixed on the
  capnp-zig side (`connection_loop.closedForGood`).
- A resumed client opens at most the remembered number of streams before
  its handshake (v0.27.0). capnp-zig's native outbound queue already treats
  `StreamLimitExceeded` as transient; a native test stages five data frames
  against a remembered uni window of two, and all arrive.

## Left as candidates (not built in v0.27.0)

These stayed open through quic-zig v0.28.1, and v0.29.0 delivered all
three ("Delivered in v0.29.0", at the end). None blocked capnp-zig.
quic-zig v0.28.1 (capnp-zig's pin for v0.20.0) builds none of them. It
documents the first two and the bundled loop's clock:
`rotateSessionTicketKey` says that the Server does not check the thread
and that the old key's time counts from `now_us`; its doc and EMBEDDING.md give the restart recipe (start with the OLD key, then
rotate again before the first datagram); and `Config.new_token_key` says
that the bundled loop's clock starts at zero.

- **A thread check in `rotateSessionTicketKey`.** quic-zig documents "call
  it on the thread that calls `feed`" but does not check it; a call from
  another thread races the handshakes that read the keys. capnp-zig checks
  on its side (`Server.assertLoopThread`, Debug always, release with
  `runtime_thread_checks`), which does not cover an embedder that drives
  quic-zig directly.
- **The previous key at start.** `Config.session_ticket_key` holds one key,
  so a process that restarts within one ticket lifetime after a rotation
  must start with the OLD key and rotate before its first datagram, or lose
  the old key's tickets. A `Config` field for the previous key (and the
  time it expires) would let a restart carry both keys directly.
- **The NEW_TOKEN clock asks** (section above): `transport/udp_server.zig`'s
  bundled loop still feeds a clock that starts at zero, so a persisted
  `new_token_key` does not survive a restart there; NEW_TOKEN times share
  the monotonic timer clock; and the check allows no clock skew
  (`new_token_max_clock_skew_us`). capnp-zig's `Listener.nowUs` anchor
  works around the first two for capnp-zig servers only.

## Delivered in v0.29.0

quic-zig v0.29.0 (tag `b9a15e6`) builds all three candidates, and
capnp-zig's pin moved to it:

- `Server.rotateSessionTicketKey` checks the thread in a Debug build. The
  first `feed`, `tick` or rotation fixes the loop thread. capnp-zig's tests
  rotate on the thread that steps the server, so none trips it.
- `Server.Config.previous_session_ticket_key` (and `_until_us`).
  capnp-zig passes them through as `ServerOptions` and preset fields, with
  quic-zig's three refusals checked first.
- `Server.Config.new_token_clock` and `new_token_max_clock_skew_us`, passed
  through as `ServerOptions` fields.
- Also: `Client.Config.session_ticket_lifetime_s` (a `ClientOptions` field
  in capnp-zig) and `Client.resumptionTicketLifetimeSeconds`, which replaced
  the last `boringssl.raw` read in capnp-zig's tests. No capnp-zig root
  imports the `boringssl` module now.

One defect, found on adoption: `quic.unixWallClockUs`, the clock that the
v0.29.0 docs name for `new_token_clock`, does not compile with Zig 0.17.0.
It calls `std.time.microTimestamp`, and the 0.17.0 `std.time` has only unit
constants and `epoch`. Any reference fails with "root source file struct
'time' has no member named 'microTimestamp'" at `src/root.zig:75`. quic-zig's
own tests do not call it, so its gates do not see it. capnp-zig does not
reference it, and its docs tell users not to. Fixed in quic-zig v0.30.1:
it reads libc's `clock_gettime` (`RtlGetSystemTimePrecise` on Windows),
and a test keeps it compiled.

## Asked after v0.29.0: a way to move the loop thread

The v0.29.0 Debug check fixes the loop thread at the first `feed`, `tick`
or rotation, and nothing moves it after that. capnp-zig documents a thread
handoff at a quiescent point (`Connection.adoptOwnerThread`: connect or
accept on one thread, run on another). Since v0.29.0, a server-role
connection that has received one datagram cannot do this in a Debug build:
the next `feed` on the new thread asserts at `Server.zig:1298`
(`std.debug.assert(loop == me)`). The same handoff passed on v0.28.1.
Release builds are not affected.

The ask: a call that moves the latch at a quiescent point, for example
`Server.adoptLoopThread()`, which sets the loop thread to the calling
thread. capnp-zig's `Connection.adoptOwnerThread` would call it for the
server role. Until then, capnp-zig documents the limit (CHANGELOG,
`Connection.adoptOwnerThread`, `Listener`, `docs/quic-transport.md`
"Rotation", `docs/rpc_runtime_design.md`). It does not block capnp-zig.

Delivered in quic-zig v0.30.1 (2026-10-06): `Server.adoptLoopThread()`
makes the calling thread the loop thread, for an embedder that hands a
Server to another thread at a quiescent point. Only the latch moves; two
threads running the Server at once stay a programming error. capnp-zig
pins v0.30.1, and `Connection.adoptOwnerThread` does not call it yet, so
the documented limit stands until it does.
