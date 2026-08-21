# QUIC durable capabilities — substrate map and prototype plan

Status: design + prototype #1 (2026-08-20). Nothing here is wired into the
RPC runtime yet. This document records (a) the design being pursued, (b) a
verified map of what quic-zig and capnp-zig already provide for it, and
(c) the prototype sequence, with prototype #1 landed upstream.

The design itself came out of an August 2026 exploration into using QUIC
connection-ID machinery as the transport substrate for Cap'n Proto durable
capabilities, aimed at an eventual QUIC netlayer (a concrete `VatNetwork`)
for this repo.

## The design in one paragraph

DCIDs and sturdy refs rhyme: both are opaque, receiver-minted names for
state that outlives the channel that minted them. The synthesis is a
**resolution ladder**:

```
objectId (durable, secret, only ever inside TLS)
  → session ticket (warm, 0-RTT restore)
    → active CID set (live, path-mobile)
```

Each layer resolves down to the next; each failure falls back up one rung.
Governing invariant: **a CID names a route, crypto authorizes.** Durable
identity never appears on the wire in cleartext.

Four mechanics carry it:

1. **Level 3 rendezvous** — vat C mints a provision ticket
   {address hints, dictated initial DCID, reset token, nonce}; vat B dials
   C using the dictated initial DCID; C's front end statelessly routes that
   handshake to the pending provision. The initial DCID is the
   Provide/Accept rendezvous token.
2. **Warm restore** — a warm sturdy ref is {ref, sessionTicket}; restore
   rides 0-RTT early data; the replay hazard maps onto restore's
   idempotency requirement.
3. **Stateless reset as death certificate** — stale-CID traffic draws a
   verifiable per-CID "this state is gone"; the RPC layer translates it to
   Disconnected-with-proof instead of timeout heuristics.
4. **CID as routable shadow** — QUIC-LB-style encrypted CIDs carry
   shard/epoch; object migration walks the client onto new-home CIDs via
   NEW_CONNECTION_ID + Retire Prior To while live caps keep resolving.

Rejected (do not revisit without new information): per-capability CIDs as
demux. CIDs select connections, not streams, and a stable
capability-derived CID is a linkability beacon.

## Substrate map (verified against source, 2026-08-20)

Nine parallel readers swept quic-zig (post-v0.13.1 working tree) and
capnp-zig. Condensed verdicts; PRESENT means implemented and tested.

### quic-zig — mostly ready

| Mechanic needs | Status | Where |
|---|---|---|
| NEW_CONNECTION_ID send/receive with Retire Prior To | PRESENT | `src/Connection/cids.zig` (`queueNewConnectionId`, `replenishConnectionIds`, `registerPeerCid`) |
| Deliberate migration walk | PRESENT | `Server.rotateLiveSlotCids` (`src/Server/routing.zig`) — needs `quic_lb` + `stateless_reset_key` both set |
| QUIC-LB draft-21 encrypted CIDs (shard/epoch substrate) | PRESENT | `src/lb/` — all three modes + LB-side decoder; shard/epoch maps to server_id/config_id |
| Per-CID stateless reset tokens, restart-survivable | PRESENT | `src/conn/stateless_reset.zig` — HMAC-SHA256(static key, cid) |
| Stateless reset **detection** (consumer half) | PRESENT | close event `CloseSource.stateless_reset` |
| Stateless reset **emission** (producer half) | **ABSENT** | `Server.feed` silently drops unknown-CID datagrams; no reset packet encoder exists |
| Session tickets + 0-RTT + anti-replay | PRESENT | `src/tls/resumption_state.zig` ("QZRS" envelope), `AntiReplayTracker` ("QZAR" persistence); e2e-proven in `tests/e2e/zero_rtt_wrapper.zig` |
| Early data flagged to the app (idempotency enforcement) | PRESENT | per-stream, per-datagram, and one-shot connection event |
| Dictated client initial DCID | **PRESENT as of prototype #1** | `Client.Config.initial_dcid` (was length-only before) |
| Pre-accept DCID observation / provision routing hook | ABSENT in Server; seam exists outside | peek via `quic.wire.header.peekLongCommon` before `Server.feed`; bind on `.accepted` via `Slot.initial_dcid` |
| preferred_address server side | PRESENT | client registers but does not auto-migrate (embedder drives the flip) |
| Multipath (per-path CID spaces, traffic-class pinning later) | PRESENT | draft-ietf-quic-multipath-21, path bring-up manual |

Notable fleet-scale gaps (matter later, not for prototypes):
session-ticket keys are per-SSL_CTX with no bridging API (warm restore
across hosts/restarts needs embedder-managed ticket keys);
`AntiReplayTracker` is single-process; QUIC-LB config rotation is
single-active-config. Known upstream doc rot: comments reference
`provideConnectionId`, which does not exist (`replenishConnectionIds` is
the real API).

### capnp-zig — anchors shipped, plumbing absent

- **Level 2 exists end to end** (Experimental): opaque sturdy-ref bytes,
  `Persistent.save`, and restore via the Restorer convention
  (`0xac47e3f6453b50f3`) on the bootstrap cap. `Bootstrap.deprecated_object`
  is parsed and ignored.
- **Level 3 exists end to end** (Experimental): Provide/Accept,
  `thirdPartyHosted` emission, `ProvisionIndex` + `Vat` facade, and the
  two-function `VatNetwork` vtable (`src/rpc/vat/network.zig`):
  `mint_introduction → {to_await, to_contact, nonce}` and
  `connect_to_introduced(contact) → live *Peer`. All tokens are opaque
  AnyPointers — **exactly where a provision ticket rides**. The only
  implementation is the in-process Loopback; a QuicVatNetwork would be the
  first real one. Note `connect_to_introduced` is synchronous — a QUIC
  dial must come from a pre-established pool or block.
- **The QUIC transport adapter forwards none of the ladder's client
  mechanics**: 8 of ~25 `Client.Config` fields
  (`endpoint_factory.zig:38-50`); no `resumption_state` /
  `new_session_callback`, no `initial_dcid`, no NEW_TOKEN. Close cause is
  discarded at the adapter boundary (never polls quic-zig's CloseEvent,
  so `.stateless_reset` is invisible to the peer layer). Both RPC engines
  gate the first write on `handshakeDone()`, which blocks 0-RTT restore.
  `ClientEndpoint.handleDatagram` drops datagrams from any source other
  than the configured remote, which precludes migration/preferred-address
  dialing as-is.

## Prototype sequence

### Prototype #1 — rendezvous front end (DONE, upstream)

The one mechanic with no prior art, now demonstrated in quic-zig
(uncommitted on its working tree as of this writing):

- `Client.Config.initial_dcid: ?[]const u8` — dictate the initial DCID
  bytes (8..20 validated; random mint unchanged when unset). Two
  InvalidConfig tests.
- `tests/e2e/rendezvous_frontend.zig` — toy front end: pending-provision
  table keyed by dictated DCID, pre-decrypt RFC 8999 §5.1 header peek
  before `Server.feed`, claim-on-`.accepted` binding to the new `Slot`,
  and an explicit claim/confirm split. Five tests prove: (1) a
  dictated-DCID dial claims its provision and the nonce confirms it on
  the routed connection; (2) random-DCID dials pass through untouched;
  (3) claims are single-use — a replayed DCID never rebinds; (4) a wrong
  nonce (or the right nonce from the wrong slot) never confirms; (5) the
  Retry limitation below, pinned as a deliberately-failing-loudly test.

Design facts the prototype pinned down (several found by adversarial
review of the first draft, not by the first draft):

- **Claim vs confirm must be separate states.** The claim latches at
  `.accepted` on plaintext-observable data (the DCID), so it is only
  ever routing — anyone who learns the DCID can trigger it. The
  authorization bit is set exclusively by a constant-time nonce check
  inside the TLS channel, bound to the claimed slot's id. A spent
  provision is never reopened on a failed confirm; the minter issues a
  fresh ticket.
- Claim on `.accepted`, not on first sight — first-flight retransmits
  carry the same DCID and must keep routing.
- **Retry breaks the peek seam, silently.** With `retry_token_key` set,
  the slot-creating second Initial carries the server-minted Retry SCID
  as its DCID; the dictated value survives only as the ODCID inside the
  server's own Retry token. The provision misses while the connection
  completes fine. A production front end must not Retry provision dials,
  or must recover the ODCID from its token. Pinned as test (5).
- Keep dictated DCIDs at 8 bytes: the Retry-token plaintext budget caps
  addr+ODCID+SCID at 45 bytes, so a 20-byte dictated ODCID + IPv6 does
  not fit.
- **Dictated DCIDs must be CSPRNG-minted by the ticket issuer.** The
  random mint the field replaces was the RFC 9000 §7.2 unpredictability
  guarantee; Initial protection keys derive from the DCID (RFC 9001
  §5.2), so a guessable value enables off-path Initial forgery. The
  `Client.Config.initial_dcid` doc carries this contract.

### Prototype #2 — warm restore (client half DONE 2026-08-21)

Landed on main: `ClientOptions` carries the resumption surface
(`resumption_state`, `new_session_callback`, `new_token(+cb)`), and the
stream engines open the RPC stream pre-handshake on resumed dials
(`early_open`), so frames enqueued before the loop starts ride 0-RTT.
Two e2e tests prove it end to end through the RPC adapter: the resumed
dial's first frame is ACCEPTED 0-RTT, and a stale ticket against a
fresh server is rejected but the staged frame still arrives at 1-RTT
(quic-zig's requeue-on-rejection contract held exactly as documented).
Landing it also flushed out a latent adapter bug (frames buffered
before callbacks bound were never dispatched without new bytes) and,
via the new QUIC soak variant, a real gap: **the QUIC transport has no
`on_tick` plumbing, so Peer call-deadlines never fire over QUIC** —
tracked as its own fix.

Remaining (server half, unblocked by the v0.14.0 pin):

1. Idempotency gate: only `restore` (and explicitly idempotent
   methods) may execute off streams flagged
   `streamArrivedInEarlyData`; server early-data posture decision for
   the hardened preset.
2. Persist {ticket envelope + NEW_TOKEN} together as the warm half of a
   sturdy ref (quic-zig deliberately keeps them separate channels).

### After both: the ladder into the RPC runtime

- QuicVatNetwork implementing the `VatNetwork` seam; provision ticket =
  {address hints, dictated DCID, reset token, nonce} rides the opaque
  `to_contact` AnyPointer; `completion == await` nonce check unchanged.
- Stateless-reset death certificate needs two things quic-zig lacks: a
  reset **emitter** (packet encoder + a `Server.feed` outcome/hook for
  unroutable short-header DCIDs) and, in capnp-zig, plumbing close-cause
  (CloseEvent/CloseSource) through the adapter to break questions with
  proof instead of one collapsed exception string.
- Migration walk: `rotateLiveSlotCids` exists; the capnp adapter must
  stop dropping datagrams from unexpected sources first.

## Open questions carried forward (from the design handoff)

- Server-side connection handoff (serialize transport+TLS state; ticket
  keys across a fleet; retire_prior_to timing vs in-flight calls).
- Proof format for sturdy refs: swiss number vs MAC(vatSecret,
  objectId ‖ epoch) vs signature; epoch publication.
- Does capnp_host_abi surface transport events to the WASM guest, or does
  the host own the ladder? (Leaning: host owns it.)
- 0-RTT budget under amplification limits for a realistic restore payload
  (note: quic-zig has no max_early_data plumbing; the cap is BoringSSL's).
- Provision anti-replay at scale: the prototype's single-use table is the
  degenerate answer; time-boxing is the follow-up.
- Browser asymmetry: WebTransport hides CIDs, so browser peers get the
  warm-restore rung only; the netlayer API should degrade along that line.
