# QUIC durable capabilities — substrate map and prototype plan

Status (refreshed 2026-10-03): the ladder's first three rungs are in the RPC
runtime, all Experimental. Warm restore works in both halves, the stateless
reset reaches the peer as a typed death certificate, and `WarmRedialClient`
heals a restored capability across a server crash-restart. The hardened server
preset now carries the reset key, and the redial budget counts consecutive
failures. "Ledger" below lists what landed (each commit checked with
`git log`), and "Open rungs" lists what is left, with file:line anchors. The
sections after them are the original 2026-08-20 plan and its running notes,
kept for the design reasoning. Where they describe a gap as open, the ledger
is authoritative.

The design itself came out of an August 2026 exploration into using QUIC
connection-ID machinery as the transport substrate for Cap'n Proto durable
capabilities, aimed at an eventual QUIC netlayer (a concrete `VatNetwork`)
for this repo.

## Ledger (done, verified 2026-10-03)

| Rung | Commit | What landed |
|---|---|---|
| Substrate map + prototype plan | `d981181` | This document. |
| Prototype #2, client half | `0c0b6b7` | `ClientOptions` resumption surface; RPC stream opens pre-handshake on resumed dials, so frames ride 0-RTT. |
| On-tick parity | `858bc14` | The QUIC transport drives `Peer` call deadlines (the gap prototype #2 found). |
| 0-RTT replay-execution window | `58d6756` | With `.without_replay_protection`, early frames wait for the handshake. |
| QuicVatNetwork v1 | `5a543fc`, `28587e8` | Provision-ticket introductions over a pre-established peer pool. |
| Death certificate | `090c6bd`, `fa31651`, `f918272` | `rpc.events.DisconnectCause` from the QUIC close event to `Peer.lastDisconnectCause()`; `ServerOptions.stateless_reset_key` forwarded. |
| Fixed-token footgun | `8ec89c3`, `1466177`, `062fc64` | Hand-set reset tokens refused (quic v0.16.1). |
| Abrupt-death soak mode | `a6893a2` | `--abrupt-death-every-ms` kills and restarts the QUIC server mid-run. |
| Auto warm redial | `182b6ce`, `8a2aaf2` | `WarmRedialClient`: redial on `.stateless_reset`, re-restore, `on_rebind`. |
| **Warm restore, server half** | **`c093648`** | `ServerOptions.early_dispatch = .restore_only` executes only the idempotent prefix (Bootstrap + Restorer calls) inside the replay window; `quic.warm_state` persists {ticket, NEW_TOKEN} as one blob; `WarmRedialClient.exportWarmState`/`seedWarmState`. Closes prototype #2's "Remaining" items 1 and 2. |
| **Soak healing workers** | **`4c563e4`** | `--heal-workers K`: persistent `WarmRedialClient`s heal across every abrupt death (first run: 56 redials, 64 rebinds, 0 give-ups). Closes "wire the redial path into the soak's workers". |
| **Half-open handshake guard** | **`0f99d89`** | `handshake_timeout_ms` on server (10 s) and client (30 s), certified `DisconnectCause.handshake_timeout`, `Server.feedOutcomeCounts()`, batched receive. Made heal-under-death hold at churn scale. |
| Nightly self-healing soak lane | `08fc53b` | The heal soak runs in Nightly. |
| Embedded 0-RTT parity | `dece43c` | `EmbeddedSession` gets the same replay-hold posture. |
| Lifetime stream cap removed | `bf9a2e7`, `15b86ae` | quic v0.24: stream limits are an open-at-once window; `stream_limit_exhausted` is gone. |
| Hardened preset carries the death certificate; consecutive redial budget | this sprint (item 8) | `ServerProductionHardening.stateless_reset_key` is required; `.early_data = .restore_only` is the explicit 0-RTT opt-in (sets `.without_replay_protection` + `.restore_only` together); `WarmRedialClient.Policy.min_healthy_ms` (10 s) resets `redials` after a healthy generation. Crash-restart e2e against the preset. |

## Open rungs (as of 2026-10-03)

1. **Session-ticket keys do not survive a restart.** BoringSSL mints tickets
   under a per-`SSL_CTX` key, so after a crash-restart the redial's ticket is
   always rejected: the heal works, but it pays a full handshake and never
   rides 0-RTT. `ServerOptions` has no ticket-key field
   (`src/rpc/transport/quic/options.zig:327`); quic-zig leaves ticket keys to
   the embedder (`Server.replaceTlsContext`, "Resumption note"). Needs a
   persisted ticket key, like the reset key, and then an e2e that a
   crash-restart redial is ACCEPTED 0-RTT.
2. **Provision dials vs the hardened preset's Retry.** The preset requires
   `retry_token_key` (`src/rpc/transport/quic/options.zig:460`), and a Retry
   replaces a dictated initial DCID on the wire, so a VatC behind the preset
   misses every provision (`ServerSession.initialDcid`,
   `src/rpc/transport/quic/server.zig:880-882`). Needs a no-Retry carve-out
   for ticketed DCIDs, or ODCID recovery from the Retry token.
3. **VatC-side admission.** Matching `initialDcid()` against expected tickets
   at adoption, with a single-use claim, is still embedder policy
   (`src/rpc/transport/quic/server.zig:882`).
4. **Dial-on-miss for QuicVatNetwork.** `connectToIntroduced` redeems only
   from the pre-established pool and fails with `error.NoPathToVat`
   (`src/rpc/vat/quic_network.zig:345-353`). Both seam consumers run inside
   frame dispatch and need a live peer synchronously, so this needs its own
   design (pool warm-up from ticket hints).
5. **Provision-ticket reset token.** `reset_token` rides empty
   (`src/rpc/vat/quic_network.zig:29`, `:91`, `:165`): quic-zig still has no
   client-side knob to preinstall an expected stateless-reset token for a
   dial. The §18.2 transport parameter covers the handshake CID.
6. **Migration walk.** `ClientEndpoint.handleDatagram` drops datagrams from
   any source other than the configured remote
   (`src/rpc/transport/quic/client_endpoint.zig:78`), which rules out
   `rotateLiveSlotCids`-driven migration and preferred-address dialing.
7. **Redial backoff is fixed.** Every client of a crashed server redials
   after the same `Policy.backoff_ms` (50 ms,
   `src/rpc/transport/quic/warm_redial.zig:64`): a thundering herd at fleet
   scale. Needs jitter, and probably exponential growth within a failure
   streak.
8. **Anti-replay at scale.** The hardened preset offers 0-RTT only as
   `.restore_only` without a tracker (`ProductionEarlyData`,
   `src/rpc/transport/quic/options.zig:440`), because quic-zig's
   `AntiReplayTracker` is single-process. A fleet-wide tracker would allow
   `.with_anti_replay` and early dispatch of more than the restore prefix.
9. **The soak does not run the hardened preset.** The abrupt-death soak
   builds raw `ServerOptions` with a fixed reset key
   (`tools/soak_rpc.zig:1241-1263`), so Retry, NEW_TOKEN and the rate gates
   are not exercised under churn. The preset's crash-restart behavior is
   proven only by the e2e in `tests/rpc/transport/quic/rpc_quic_peer_test.zig`.

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

Historical snapshot: the gaps below were true on 2026-08-20. Most are
closed now; see "Ledger" and "Open rungs" above.

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
fixed in `858bc14`.

Server half: **DONE in `c093648`** (2026-08-26). It was:

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

#### QuicVatNetwork v1 (LANDED 2026-08-21, Experimental)

`rpc.vat.quic_network` — the first out-of-process `VatNetwork` shape.
What shipped:

- **Provision ticket codec** (`encodeTicket`/`decodeTicket`): a struct-rooted
  standalone message `{version u16, dcid Data, nonce Data, vat_key Data,
  reset_token Data (reserved, empty), packed addr hints Data}`. One
  deterministic encoder; bounds-checked decode that fails cleanly on foreign
  (Data-rooted loopback) tokens. Address hints are raw bytes+port — no text
  grammar to drift.
- **Await/completion token** (`encodeAwaitToken`): `{version, nonce, dcid}`,
  produced by the SAME function on the mint side (`to_await`) and the
  redemption side (completion), which is what upholds the seam's
  byte-identity invariant (VatC keys its provide table on serialized bytes).
- **`QuicVatNetwork(Peer)`**: vat directory (`addVat(key, hints)`) for
  minting; pre-established pool (`registerPeer(key, peer)`) for redemption.
  `mint_introduction(recipient_hint = vat key)` mints a 16-byte nonce and an
  8-byte dictated DCID from a fail-closed CSPRNG (explicit seed or
  `io.randomSecure`; no entropy → `error.EntropyUnavailable`). Verified
  against quic-zig HEAD 2026-08-21: upstream `connect` validates ONLY the
  8..20 length — unpredictability is entirely the minter's job, so the mint
  owns it.
- **Adapter plumbing**: `ClientOptions.initial_dcid` (validated 8..20,
  forwarded to `quic_zig.Client.connect`) and
  `ServerSession.initialDcid()` (reads `Slot.initial_dcid` — the hook a
  rendezvous embedder matches accepted sessions against tickets).
- **Proofs**: unit suite (codec round-trip/determinism, malformed/foreign
  tickets, pool errors, OOM-clean, ablation-verified) plus two QUIC e2e:
  dictated-DCID observability at the accepted session, and the FULL L3
  three-party handoff over three real QUIC connections (C = fanout server,
  two sessions sharing one ProvisionIndex; B mints a ticket naming "vat-c";
  A auto-picks-up from its pool; direct `getNumber()` returns 42;
  vine drains). Ablation-verified (detaching A's network goes red).

Deliberate v1 boundaries (each is the next rung, not an oversight):

- `connect_to_introduced` redeems ONLY from the pre-established pool — no
  dial-on-miss. Both seam consumers run inside frame dispatch and demand a
  same-thread live peer synchronously; an async pre-dial rung needs its own
  design (pool warm-up driven by ticket hints).
- `reset_token` rides empty: quic v0.16 has no client-side knob to
  preinstall an expected stateless-reset token; the §18.2 transport param
  covers the primary CID.
- Provision dials must not be Retried (dictated DCID survives only as ODCID
  in the Retry token); the fanout server does not yet enforce a
  no-Retry-for-ticketed-DCIDs carve-out.
- VatC-side admission (match `initialDcid()` against expected tickets at
  adoption, claim single-use) is embedder policy for now; the accessor is
  the hook. The upstream rendezvous front-end e2e remains the reference for
  a pre-accept routing front end (peek-before-feed), which our fanout
  server deliberately does not embed yet.
- Stateless-reset death certificate needs two things quic-zig lacks: a
  reset **emitter** (packet encoder + a `Server.feed` outcome/hook for
  unroutable short-header DCIDs) and, in capnp-zig, plumbing close-cause
  (CloseEvent/CloseSource) through the adapter to break questions with
  proof instead of one collapsed exception string.

  **LANDED 2026-08-21 (Experimental).** quic-zig shipped the emitter in
  v0.15; the capnp-zig half now reads the sticky `closeEvent()` on both
  terminal paths and carries `rpc.events.DisconnectCause` to
  `Peer.lastDisconnectCause()` (set before cancelled-question callbacks
  fire). The exception reason text deliberately stays "disconnected" for
  every cause — the certificate rides the peer, not the string — so TCP
  and existing matchers are untouched. `ServerOptions.stateless_reset_key`
  is forwarded (enabling both the emitter and the §18.2 handshake-CID
  token that makes client-side proof possible), and
  `Server.statelessResetsSent()` + the soak's `unroutable_dcid` counters
  are the instrument for the churn field data upstream wants. Measured so
  far: graceful churn produces all-`local_close` sessions and zero resets,
  which is correct but not the interesting case — an abrupt-death chaos
  mode is what will actually exercise the reset path. Proven by a crash-restart e2e (no close ceremony, same
  port + key, next call → `.stateless_reset` at every observation point).
  **The integration rung LANDED 2026-08-26 (Experimental):**
  `rpc.transport.quic.WarmRedialClient` ACTS on the proof — on
  `.stateless_reset` it dials a fresh Connection+Peer generation resumed
  via the latest captured session ticket and re-restores the saved
  sturdy ref, with bootstrap+restore pipelined
  (`Peer.sendRestorePipelined` aims restore at the promised bootstrap
  answer) so both frames ride 0-RTT on the resumed dial; the app
  receives the healed capability through an `on_rebind` callback (import
  ids die with their peer — healing is at the sturdy-ref layer, by
  design). Only `.stateless_reset` redials by default; `.idle_timeout`
  is opt-in. Proven by a crash-restart heal e2e (2 generations, 1
  redial, healed cap answers, server reset counter advances) plus a
  zero-budget ablation (detects but does not heal, give-up carries the
  certified cause). The abrupt-death soak mode (`--abrupt-death-every-ms`)
  is the churn-scale instrument for it; the soak's healing workers
  (`--heal-workers`, `4c563e4`) run the redial path under it.
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
