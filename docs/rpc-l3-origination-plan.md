<!--
Planned 2026-07-04. Status: PROPOSED design for Level-3 three-party handoff ORIGINATION.
Receive-side handlers + wire builders already exist; this covers the missing Peer orchestration
+ VatNetwork seam. Cross-impl is Zig<->Zig-only initially (see Part 4).
-->

# Cap'n Proto RPC Level-3 Three-Party Handoff ORIGINATION — Implementation Plan

## Executive summary

The receive-side L3 machinery in capnp-zig is **substantially complete and well-factored**, and — importantly — **the wire encoders/builders for every origination message already exist**. What is missing is entirely at the **Peer orchestration layer**: there are no public `sendProvide`/`sendAccept`/`sendJoin` entry points, no code path that *emits* a `thirdPartyHosted` descriptor, and — the crux — **no abstraction by which a peer can name or reach a third vat**. The runtime is two-party: a `Peer` is bound to exactly one transport and one remote. Origination inherently requires a peer to hold *two* connections (to the host and to the recipient) and to mint addressing tokens the other end can redeem. That is the invasive part, not the message encoding.

---

## Part 1 — The current receive-only L3 implementation (file:line)

### The four inbound handlers (`src/rpc/peer/mod.zig`)

All four are thin dispatchers into `provide/peer_provide_join_orchestration.zig` and `third_party/peer_third_party_adoption.zig`, wiring per-peer state maps and comptime hook function-pointers.

- **`handleProvide` (`mod.zig:3844`)** — the peer here plays the **capability host** (Vat C in the spec's Alice/Bob/Carol example). Rejects question-id reuse against inbound answers and pending joins (`3849-3852`), then calls `peer_provide_join_orchestration.handleProvide` (`3854`). That routine (`peer_provide_join_orchestration.zig:48-117`): captures `provide.recipient` (a `ThirdPartyToAwait` AnyPointer) into an owned byte key via `captureAnyPointerPayload`; rejects duplicate provide question-id (`73`) and duplicate recipient key (`77`); enforces `ensureProvideBudget`; resolves `provide.target` (`MessageTarget`) to a `ResolvedCap` and builds a `ProvideTarget` (`makeProvideTarget`, `mod.zig:3797` — captures origin-tag + cap_id, or an owned promised-answer); writes into **`provides_by_question`** (question_id → `ProvideEntry{recipient_key, target}`) and **`provides_by_key`** (recipient_key → question_id). No Return is sent — the question is held open until Finish (spec `rpc.capnp:834-847`).

- **`handleAccept` (`mod.zig:3880`)** — the peer plays the **host** again; a *recipient* is picking up the provided cap. Calls `peer_provide_join_orchestration.handleAccept` (`peer_provide_join_orchestration.zig:119-162`): captures `accept.provision` (a `ThirdPartyCompletion`) into a key; looks it up in `provides_by_key`→`provides_by_question` to recover the `ProvideTarget`; if `accept.embargo` is present, defers via `queueEmbargoedAccept` (`mod.zig:3810`) into `pending_accepts_by_embargo`; otherwise sends the provided cap back via `sendReturnProvidedTarget` (`mod.zig:2684`), which encodes the target as an **origin-tagged capability** in the Return payload (`2698-2708`).

- **`handleJoin` (`mod.zig:3900`)** — Level 4, but wired. Collects `JoinKeyPart`s into `pending_joins`/`pending_join_questions` until `part_count` parts arrive, then `completeJoin` returns the agreed target. (`peer_provide_join_orchestration.zig:164-254`.)

- **`handleThirdPartyAnswer` (`mod.zig:3945`)** — the peer plays the **caller** (Vat B receiving results directly from Vat C after a `sendResultsTo=thirdParty` call). Calls `peer_third_party_adoption.handleThirdPartyAnswer` (`peer_third_party_adoption.zig:49-109`): validates the callee-allocated answer-id range (`isThirdPartyAnswerId`: bit 30 set, bit 31 clear — `adoption.zig:7-9`, matching `rpc.capnp:936-941`); captures `completion`; either adopts an already-parked await (`pending_third_party_awaits`) or records into `pending_third_party_answers` for later adoption. Adoption (`adoptThirdPartyAnswer`, `adoption.zig:11-47`) inserts the callee-chosen answer-id into `questions`, records `adopted_third_party_answers`, and replays any `pending_third_party_returns` frame buffered before adoption.

### The L3 state tables (declared `mod.zig:359-380`, documented `mod.zig:265-274`)

| Table | Declared | Role in receive path |
|---|---|---|
| `provides_by_question` (`AutoHashMap(u32, ProvideEntry)`) | `mod.zig:362` | host: question_id → provided target |
| `provides_by_key` (`StringHashMap(u32)`) | `mod.zig:364` | host: recipient key → question_id (Accept lookup) |
| `pending_joins` / `pending_join_questions` | `mod.zig:366,368` | host: L4 join accumulation |
| `pending_third_party_awaits` (`StringHashMap(PendingThirdPartyAwait)`) | `mod.zig:374` | caller: outbound handoff awaiting ThirdPartyAnswer |
| `pending_third_party_answers` (`StringHashMap(u32)`) | `mod.zig:376` | caller: completion key → answer_id, awaiting adoption |
| `pending_third_party_returns` / `adopted_third_party_answers` | `mod.zig:378,380` | caller: buffered returns / adopted-id mapping |
| `send_results_to_third_party` (`AutoHashMap(u32, ?[]u8)`) | `mod.zig:396` | callee: inbound calls whose results go to a third party; payload is serialized recipient |

Lifecycle: all are init'd (`mod.zig:569-583`), deinit'd (`mod.zig:837-887`) with owned-key/frame freeing, and budget-checked (`mod.zig:1102-1170`, `2806-2816`). This is production-grade bookkeeping — the origination path can and should reuse these exact tables in the mirror-image roles.

### The module dirs

- **`src/rpc/peer/provide/`** — `peer_provide_join_orchestration.zig` (inbound handleProvide/Accept/Join), `peer_provides_state.zig` (map put/get/clear), `peer_join_state.zig` (join accumulation).
- **`src/rpc/peer/third_party/`** — `peer_third_party_adoption.zig` (ThirdPartyAnswer adoption + `handleReturnAcceptFromThirdParty`), `peer_third_party_pending.zig` (await/answer map helpers), `peer_third_party_returns.zig` (pre-adoption return buffering).
- **`src/rpc/peer/third_party.zig`** — re-export barrel + the *callee*-side `noteCallSendResults`, `buildForwardedCallDestination`, `applyForwardedCallSendResults`. This is where the outbound `sendResultsTo=thirdParty` encoding on **forwarded** calls already lives — a partial origination that only fires in a proxy topology.

### The cap descriptor encoder (`src/rpc/caps/*`)

- **`outbound.zig`** — `encodePayloadCaps` (`225-283`) walks payload pointers, classifies each cap via `resolveCapEntry`→`classifyCap` (`115-139`) into one of `senderHosted`/`senderPromise`/`receiverHosted`/`receiverAnswer`, and emits the descriptor (`261-271`). **`thirdPartyHosted` is not in `CapDescriptorTag` classify/emit** — this is exactly where outbound emission must hook in.
- **The wire encoder already exists**: `protocol.CapDescriptor.writeThirdPartyHosted(builder, id: AnyPointerReader, vine_id)` and `writeThirdPartyHostedNull` (`protocol.zig:125-142`). So emission is a *classification + plumbing* problem, not a serialization problem.
- **Shapes** (spec `rpc.capnp:1215-1238`, generated `gen/capnp/rpc.zig:1637`, wire `protocol.zig:147-158`):
  - `ThirdPartyCapDescriptor { id: ThirdPartyToContact (AnyPointer), vineId: ExportId }`
  - `ThirdPartyToContact`/`ThirdPartyToAwait`/`ThirdPartyCompletion` are all **`using X = AnyPointer`** (`rpc.capnp:1441-1497`) — opaque, network-defined. There is **no `VatId` type in this spec version**; `VatId` appears only in the commented-out worked examples (`rpc.capnp:1505-1780`). This is the modern (post-2023) three-party redesign where addressing is entirely opaque and owned by the VatNetwork.

**Receive-side subtlety that shapes the design:** on the *receive* side, `caps/inbound.zig:176-180` treats an inbound `thirdPartyHosted` as **the vine import** (`table.noteImport(vine_id)`; returns `.imported`). That is the Level-1/2 proxy fallback — capnp-zig receiving a handoff today proxies through the vine, it does **not** connect to the third party. Origination must produce a descriptor the *peer* can honor, but true peer-to-peer pickup on the receive side is itself not yet implemented (it currently falls back to the vine).

---

## Part 2 — The protocol for origination (grounded in `rpc.capnp`)

Canonical topology (spec `rpc.capnp:878-904`): **Alice@VatA** holds a promise toward **Bob@VatB**; it resolves to **Carol@VatC**. VatB wants VatA to talk to VatC directly.

1. **VatB → VatC: `Provide`** (`rpc.capnp:824-854`). `target` = Carol (a `MessageTarget` local to C), `recipient` = a `ThirdPartyToAwait` naming VatA. VatB holds the question open (no Return) until Finish.
2. **VatB → VatA: `Resolve`** whose resolved descriptor is `thirdPartyHosted{ id: ThirdPartyToContact (names VatC + nonce), vineId }` (`rpc.capnp:1102-1143`). The **vine** is an export VatB mints so a Level-1/2 VatA can still proxy, and so refcount/liveness is anchored (`rpc.capnp:1223-1237`).
3. **VatA → VatC: `Accept`** (`rpc.capnp:856-904`). `provision` = a `ThirdPartyCompletion` matching the nonce; optional `embargo` if VatA has in-flight calls on the promise (the full ordering dance, `rpc.capnp:878-903`).
4. **VatC → VatA: `Return`** for the Accept, carrying Carol as a normal cap descriptor. VatA now has a direct import.
5. VatA drops the vine → VatB learns pickup completed and Finishes its `Provide` (`rpc.capnp:836-842`, 1232-1237).

The `ThirdPartyAnswer` + `sendResultsTo=thirdParty` variant (`rpc.capnp:906-942`, `450-568`) is the redirected-return optimization: a callee returns results directly to a third party via `ThirdPartyAnswer` after the introducer sent `awaitFromThirdParty`.

### The abstraction capnp-zig lacks: a VatNetwork / third-party addressing layer

**This is the crux.** Every arrow above except (1) and the vine involves a peer reaching a vat it is *not* currently connected to, or minting a token another vat will redeem on a *different* connection:

- In step 2, VatB must construct a `ThirdPartyToContact` that, when VatA parses it, tells VatA **how to open/find a connection to VatC** and **which nonce** to present. capnp-zig has no type or callback that maps "the peer I'm provided to" → "a `Peer` connected to VatC."
- In step 3, VatA must take that `ThirdPartyToContact`, **obtain a `Peer` for VatC** (dial or look up an existing connection), and send `Accept` on *that* peer — a different `Peer` object than the one the Resolve arrived on.

A `Peer` today owns one `TransportBinding` (`mod.zig:313`) and knows nothing of peer identity or a registry of peers. **There is no `VatId`, no connection registry, no dial callback.** Confirmed: zero references to any vat-network/VatId/introduce concept in `src/rpc/` outside the commented spec and unrelated substrings.

**How invasive:** moderately. It does *not* require rewriting the two-party core. It requires a new, thin **VatNetwork** seam — an application-supplied vtable that (a) mints/parses the opaque addressing tokens and (b) resolves a `ThirdPartyToContact` to a `*Peer`. The `Peer` gains an optional pointer to it. Everything else (question allocation, cap encoding, embargo) reuses existing machinery. The invasiveness is in **cross-peer coordination** (two `Peer`s cooperating within one process/event-loop for the Zig↔Zig slice), not in the message layer.

---

## Part 3 — The origination design

### VatNetwork abstraction (minimal shape)

A comptime-or-vtable seam, application-supplied, modeled on the existing `TransportBinding` pattern (`peer/transport.zig`, `mod.zig:299-313`). Proposed new file **`src/rpc/vat/network.zig`**:

```
pub const VatNetwork = struct {
    ctx: *anyopaque,
    // Host (VatB) side: produce the (ThirdPartyToAwait, ThirdPartyToContact) pair + nonce.
    // Called when originating a Provide+Resolve. Returns owned AnyPointer payloads.
    introduce: *const fn (ctx, host_peer: *Peer, recipient_hint) anyerror!Introduction,
    // Recipient (VatA) side: given a ThirdPartyToContact from an inbound Resolve,
    // resolve to a live Peer connected to VatC (dial or registry lookup), plus the
    // ThirdPartyCompletion to present in the Accept.
    connectToIntroduced: *const fn (ctx, contact: AnyPointerReader) anyerror!Introduced, // { peer: *Peer, completion: []u8 }
    // Host (VatC) side is already covered by inbound handleProvide/handleAccept.
};
pub const Introduction = struct { to_await: []u8, to_contact: []u8, nonce: []u8 };
```

The concrete transport wiring (dialing over QUIC/TCP, auth) is a follow-up; the **loopback VatNetwork** for tests just returns nonces as tokens and looks up peers in an in-process registry.

### Public API on `Peer` (all in `mod.zig`, mirroring `sendBootstrap`/`sendCall` at `mod.zig:1238`/`1993`)

```
// VatB, host-of-provided-cap side. Registers a held-open question (no Return
// expected), mints a vine export, sends Provide to the host-of-recipient
// (`self`), and returns the vine export id + provide question id so the
// caller can drive the paired Resolve.
pub fn sendProvide(self: *Peer, target_id: u32, recipient: AnyPointerReader,
                   ctx: *anyopaque, on_pickup: QuestionCallback) !ProvideHandle;

// VatA, recipient side, sent on the Peer connected to VatC. Allocates a
// question, sends Accept{provision, embargo?}, returns question_id; the
// QuestionCallback delivers the accepted cap Return.
pub fn sendAccept(self: *Peer, provision: AnyPointerReader, embargo: ?[]const u8,
                  ctx: *anyopaque, on_return: QuestionCallback) !u32;

// L4, deferred. Same question+callback shape.
pub fn sendJoin(self: *Peer, target_id: u32, key_part: AnyPointerReader,
                ctx: *anyopaque, on_return: QuestionCallback) !u32;

// Callee → third party (redirected return). Sends ThirdPartyAnswer{completion, answer_id}
// where answer_id is minted in the callee-allocated [2^30,2^31) range.
pub fn sendThirdPartyAnswer(self: *Peer, completion: AnyPointerReader, answer_id: u32) !void;
```

Each is a ~30-line function: `assertThreadAffinity` + shutdown check + `allocateQuestion` (or vine `allocExportId`) + one of the **already-existing** `MessageBuilder.buildProvide/buildAccept/buildJoin/buildThirdPartyAnswer` (`protocol.zig:1092-1169`) + `sendBuilder`. The scaffolding is all present.

### `thirdPartyHosted` descriptor emission (in `caps/outbound.zig`)

The Resolve/payload encoder must emit `thirdPartyHosted` when a cap being sent is being *handed off* rather than proxied. Two additions:

1. Add a **`third_party_hosted` variant to the classification** (`classifyCap`/`resolveCapEntry`, `outbound.zig:115-139`) — but a bare id can't carry the `ThirdPartyToContact` + `vineId`. So emission needs an **out-of-band map**: `CapTable.markThirdPartyHosted(cap_id) → {contact_payload, vine_id}`, consulted in the emit loop (`outbound.zig:259-271`) to call `protocol.CapDescriptor.writeThirdPartyHosted` (`protocol.zig:132`) instead of a two-party variant.
2. This is naturally driven by `sendProvide`: it mints the vine export and registers the handoff mapping keyed by the promise/cap being resolved, so a subsequent Resolve emits `thirdPartyHosted`.

**Refcount/lifecycle (the delicate part, per `rpc.capnp:1223-1237`):**
- The **vine** is a real export (`CapTable.allocExportId`/`noteExport`, `lifecycle.zig:52-56`) with a wire ref held by the descriptor. The originator (VatB) must **not** release the underlying provided cap while the vine is live.
- VatB closes the `Provide` question (sends Finish) only when it sees a `Call` or `Release` on the vine (spec `rpc.capnp:1236-1237`) — i.e., existing `handleRelease`/`handleCall` on the vine export must trigger the paired Provide-Finish. This is a **new cross-message-type interaction** to wire: vine-export-release → finish provide question.
- The recipient (VatA) holds the vine import until Accept completes, then releases it — driving VatB's Finish. Existing `releaseImport` (`lifecycle.zig:146`) is the mechanism.

### Integration with the hard parts (subtle-bug hotspots)

1. **Embargo/disembargo ordering during handoff (highest risk).** Spec `rpc.capnp:878-903` is an intricate 3-connection ordering guarantee: VatA sends `Accept{embargo}` to VatC *and* `Disembargo{context.accept}` to VatB on the same promise, and VatC must not Return the Accept until the disembargo arrives *via VatB*. capnp-zig's receive side already queues embargoed accepts (`queueEmbargoedAccept`, `mod.zig:3810`; `peer_embargo_accepts.zig`) and has the `Disembargo.context.accept` machinery. **Origination must generate the embargo id, emit both messages on two different peers, and correlate the release** — and this only works correctly if the calling app knows there is an in-flight call on the promise. This is where "subtle bugs in disembargo ordering" will bite: it spans two `Peer`s and requires the app to observe promise state. Hotspot files: `peer/disembargo.zig`, `peer/peer_embargo_accepts.zig`, `peer/resolve.zig`.

2. **Promise resolution → handoff.** The natural trigger for a handoff is `Peer.resolvePromiseExportToImport` (`mod.zig`, per commit `534ad13`) resolving to a *third-vat* cap. Today that path only handles caller-hosted (loopback) resolution. Emitting a `thirdPartyHosted` Resolve instead of a two-party descriptor is a **new branch in the resolve path** (`peer/resolve.zig`) and interacts with the reflected-loopback logic already flagged as fragile (supported-surface Known limitation #4). Hotspot: `peer/resolve.zig`, `peer/forward/`.

3. **Capability transfer lifecycle.** The vine refcount coupling (above) plus the fact that the same cap now exists as: an export-with-vine on VatB, a proxy-import on VatA (pre-Accept), and a direct import on VatA (post-Accept) — with a window where both the vine and the direct import are live. Getting the release ordering wrong leaks the export or drops the cap early. Hotspot: `caps/lifecycle.zig`, `caps/outbound.zig`, the new vine-release→finish coupling.

---

## Part 4 — Sequencing + minimal testable slice

### Phases (with sizing and precedence)

**Phase 0 — VatNetwork seam + loopback (S).** New `src/rpc/vat/network.zig`; add optional `vat_network: ?VatNetwork` to `Peer` (`mod.zig:305`). In-process registry + trivial nonce-token loopback network for tests. No wire changes. *Precedes everything.*

**Phase 1 — `sendProvide` + vine export + `thirdPartyHosted` emission (M).** `Peer.sendProvide` using existing `buildProvide`; mint vine via `allocExportId`; add `CapTable.markThirdPartyHosted` map + emit branch in `caps/outbound.zig:259-271` using existing `writeThirdPartyHosted`. Wire vine-release → Provide-Finish coupling. *Depends on P0.* **This is the smallest slice that puts a real `Provide` on the wire.**

**Phase 2 — `sendAccept` + accepted-cap Return handling (M).** `Peer.sendAccept` using `buildAccept`; register question with a callback that imports the accepted cap from the Return. The host side (`handleAccept`→`sendReturnProvidedTarget`, `mod.zig:2684`) already produces the correct origin-tagged Return, so the accept-originator's return handling is mostly standard import-from-return. *Depends on P1.* **P1+P2 together = the smallest end-to-end `Provide`+`Accept` handoff.**

**Phase 3 — Resolve→handoff trigger, no embargo (M).** Branch in `peer/resolve.zig` to emit a `thirdPartyHosted` Resolve when resolving to a third-vat cap; drive P1/P2 from it. *Depends on P1, P2.*

**Phase 4 — Embargo/disembargo during handoff (L, highest risk).** Generate embargo id on Accept; emit paired `Disembargo{context.accept}` on the host peer; correlate release. *Depends on P3. The hard one.*

**Phase 5 — `sendThirdPartyAnswer` + `sendResultsTo=thirdParty` origination (M).** Redirected-return path using `buildThirdPartyAnswer` and the existing `send_results_to_third_party` callee bookkeeping. *Independent of P3/P4; depends on P0.*

**Phase 6 — `sendJoin` (L4, L, optional).** Deferred; L4 is out of scope for most consumers.

### Smallest testable end-to-end slice

**P0 + P1 + P2, Zig↔Zig loopback.** Three in-process `Peer`s (A/B/C) over the loopback VatNetwork + in-memory transport pairs already used in tests. B `sendProvide`s C's cap naming A as recipient; A `sendAccept`s on the B↔C... no — A must Accept on the **A↔C** connection. So the slice needs A↔B and A↔C (and B↔C) peer pairs wired to one registry. Assertion: A ends with a working direct import of C's cap; B's vine gets released; B Finishes the Provide. No embargo, no promise resolution — a bare Provide+Accept. This is genuinely end-to-end over the wire and fully deterministic.

### Cross-impl reality check

Based on the pattern established for `resolve_disembargo` (`test_matrix.json`, supported-surface #4), expect **L3 origination to be Zig↔Zig-only initially**:

- **C++ (`capnp`/kj)** is the only reference impl with a mature, spec-current three-party handoff (it drove the 2023 redesign; its `VatNetwork`/`ThreePartyHandoff` is the reference). It is the realistic first cross-impl target — but only once a concrete (non-loopback) VatNetwork with matching token semantics exists, which is a large lift.
- **go-capnp, capnp-rpc (Rust), pycapnp** — none implement being the third party / accepting a real three-party handoff over the wire in a way that interops with an arbitrary VatNetwork; they are Level-1/2 in practice (they'd consume the vine via the proxy fallback, not perform pickup). Same situation the matrix already documents for `takeFromOtherQuestion`.

**Conclusion:** ship and test P0–P4 as **Zig↔Zig-only** with a loopback VatNetwork. A `three_party_handoff` e2e scenario should be added to `test_matrix.json` with **all four reference backends excluded** initially (documented as reference-library gaps, exactly like `resolve_disembargo`'s per-direction skips), pending a C++-compatible concrete VatNetwork.

### Top risks + where subtle bugs will hit

1. **Embargo ordering across two peers (Phase 4)** — the scout's warning lands here hardest. The spec's guarantee (`rpc.capnp:891-903`) requires foo() to reach VatC before the Disembargo, which requires correct interleaving across the A↔C, A↔B, B↔C connections. In a single-threaded loopback this is controllable; over real transports it is the classic disembargo race. **This is the single hardest sub-problem.**
2. **Vine refcount coupling** — the vine-release→Provide-Finish interaction is a new cross-message-type edge; getting it wrong leaks the vine export or Finishes the Provide too early (dropping the cap before Accept completes).
3. **Cross-peer coordination model** — the design must decide whether A/C peers cooperate in-process (loopback) or A truly dials C. The abstraction must not bake in the loopback assumption, or the concrete-transport follow-up rewrites it.
4. **Interaction with the already-fragile reflected-loopback resolve path** (#4 in Known limitations) when Phase 3 adds a third-vat branch to `peer/resolve.zig`.

### The single hardest sub-problem

**Embargo/disembargo ordering during a live-promise handoff (Phase 4).** It is the only piece that genuinely spans three connections with a hard ordering guarantee, cannot be validated by a single-peer unit test, and is precisely the "subtle bugs in disembargo ordering" the receive-side scout flagged. Everything else (message encoding, vine minting, question/callback plumbing) reuses mature, tested machinery.

---

### Critical Files for Implementation
- `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/peer/mod.zig` (add `sendProvide`/`sendAccept`/`sendJoin`/`sendThirdPartyAnswer`; state tables at `359-396`; question/callback pattern at `1238`, `1993`; `sendReturnProvidedTarget` at `2684`)
- `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/caps/outbound.zig` (`thirdPartyHosted` classification + emit branch at `115-139`, `259-271`)
- `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/caps/lifecycle.zig` (vine export mint/refcount + new `markThirdPartyHosted` handoff map; `52-178`)
- `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/peer/resolve.zig` (Phase 3: emit `thirdPartyHosted` Resolve on third-vat resolution)
- `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/peer/peer_embargo_accepts.zig` + `src/rpc/peer/disembargo.zig` (Phase 4: embargo-during-handoff — the hardest sub-problem)

New file to add: `/Users/nullstyle/prj/zig/capnp-zig/src/rpc/vat/network.zig` (the VatNetwork seam). Note: the wire builders (`protocol.zig:1092-1169`, `writeThirdPartyHosted` at `132`) and all receive-side orchestration already exist and need no changes.