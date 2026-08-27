# HANDOFF — quic-zig: add a handshake liveness timer

For the agent working on nullstyle/quic-zig. Self-contained; the
evidence comes from capnp-zig (the primary downstream, same machine at
/Users/nullstyle/prj/zig/capnp-zig if you want to read the guards it
shipped).

## The gap

A `Connection` whose handshake never completes, and whose peer goes
quiet, NEVER dies. The idle-timeout machinery does not cover the
pre-handshake-complete phase, and there is no handshake timer. Verified
against v0.16.1 behaviorally (not a misconfiguration — no knob exists).

## Why it matters (measured downstream, 2026-08-27)

- **Server side — the QUIC SYN-flood analog.** In capnp-zig's fanout
  soak, abandoned dials left server connections parked `.open` forever.
  They accumulated until EVERY `max_concurrent_connections` slot was a
  zombie (state census: all slots `.open`, zero closing/draining/
  closed), after which every new Initial was silently dropped
  (`table_full`) and the endpoint was mute to the world. Reachable by
  ordinary churn + loss, or deliberately by one hostile source sending
  bare Initials.
- **Client side — the eternal dial.** A client whose Initials are all
  silently dropped sends exactly its Initial retransmission budget
  (observed via nettop: 10,800 bytes = ~9 x 1200, bytes_in = 0), then
  goes silent and stays alive indefinitely (observed 600+ seconds). No
  timer fires; `closeEvent()` never populates.

## Repro sketch

Connect a client; deliver its first flight to a server connection; then
drop all further packets in both directions and keep stepping/ticking
both endpoints. Neither ever leaves `.open`; neither produces a
CloseEvent. (Variant: deliver NOTHING to the client — it stops sending
after its retransmit budget and idles forever.)

## Suggested design

- A per-connection handshake deadline: armed at connection creation
  (server: at slot open from the Initial; client: at connect), disarmed
  at handshake confirmation, driven by the existing tick/timer path.
- On expiry: close the connection fully — it must reach `.closed` so
  `Server.reap()` frees the slot (that is the whole point server-side;
  a close that parks in `.closing` forever would not fix the table).
- Emit a distinguishable sticky `CloseEvent` (a dedicated
  `CloseSource` variant, e.g. `handshake_timeout`, or a documented
  local-close error code). Downstream maps CloseEvent through a
  first-write-wins cause latch (capnp-zig
  `src/rpc/transport/quic/close.zig` `disconnectCauseFor`) and already
  has a `DisconnectCause.handshake_timeout` waiting — a typed source
  makes that mapping exact. CloseSource is consumed non-exhaustively
  downstream, so an additive variant is safe.
- Config knobs on both `Client.Config` and the server config. Sensible
  defaults beat opt-in (the hazard is a silent default): downstream
  chose 30s client / 10s server for its own guards. RFC 9000 permits
  closing at any time; most stacks bound the handshake (commonly tied
  to the idle timeout, applied from the first flight).

## Interaction with capnp-zig (do not break)

capnp-zig v0.15.0 ships embedder-level guards
(`handshake_timeout_ms` on its Client- and ServerOptions: the fanout
server sweeps half-opens; clients abort dead dials). Those stay as
defense in depth. When the upstream timer lands:

- fire the upstream timer's CloseEvent BEFORE the embedder guard's
  window when both are set to the same value is fine either way — the
  downstream latch is first-write-wins;
- capnp-zig will map the new CloseSource variant in
  `disconnectCauseFor` and can then relax its own defaults. Ping the
  capnp-zig side when the CloseEvent shape is decided so the pin bump
  carries the mapping in the same change.

## Acceptance criteria

1. The repro above: both endpoints reach `.closed` within the
   configured deadline; the server slot is reaped; CloseEvent carries
   the distinguishable cause.
2. A completing handshake never trips the timer (disarm proven under a
   slow-but-progressing handshake near the deadline).
3. Server table under abandoned-dial load stays bounded (the capnp-zig
   soak shape: high churn with a fraction of dials abandoned — its
   `--abrupt-death-every-ms` + feed-outcome counters make this directly
   observable if you want a full-stack check after a pin bump).
4. Resumed (0-RTT) dials: the deadline covers them too — a rejected or
   stalled resumption must not become a new immortality class.
