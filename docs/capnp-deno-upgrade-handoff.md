# capnp-deno Upgrade Handoff

This note is for the next `capnp-deno` session that updates its dependency on
`capnp-zig`.

## Target

Pin `capnp-deno` to a `capnp-zig` commit at or after the L4 addressed Join
registry/connector pilot. In the implementation handoff for that sprint, use
the final pushed commit SHA as the exact dependency target.

The relevant baseline includes:

- `quic_zig` updated to 0.7.0 at upstream `28f1d960`.
- QUIC run loops calling `Connection.advance()` before datagram polling and
  again during active service.
- `rpc.transport.quic.ClientOptions.insecure_skip_verify`, defaulting to
  certificate verification and intended only for loopback/self-signed tests.
- Experimental L3 `sendProvide`, `sendAccept`,
  `resolvePromiseExportToThirdParty`, `setHandoffPickupHandler`,
  `sendThirdPartyAnswer`, `registerPendingThirdPartyAwait`, `VatNetwork`, and
  `LoopbackVatNetwork`.
- L3 forwarded parked promise-calls, including cap-bearing params/results via
  cross-peer proxy exports.
- L3 allocator/lifecycle hardening for loopback token registration,
  `sendProvide`, `sendAccept`, vine/provide coupling, teardown ordering, and
  cross-peer proxy cleanup.
- Experimental L2 persistence (`Peer.setPersistentExport`, `setRestorer`,
  `sendSave`, `sendRestore`) with rollback/lifecycle regressions for malformed
  payloads, Return send failures, callback failures after restored-cap retention,
  independent hook clearing, reconnect/resave, and OOM paths.
- Experimental L4 receive-side Join readiness and Zig-only JoinResult runtime
  with rollback/lifecycle regressions for matching parts, mismatches, duplicate
  parts, Finish-before-completion cleanup, Return send failures, pending
  direct-Accept lifetime, and OOM insertion rollback.
- Experimental `rpc.peer.JoinCoordinator` for the compact Zig JoinResult flow:
  it sends key parts, collects matching JoinResults through a `JoinNetwork`,
  sends direct Accept, retains/releases the accepted cap, and Finishes each
  JoinResult question on the peer where that part originated with per-question
  retry state; it also Finishes JoinResults after direct Accept exceptions. It
  rejects duplicate local part numbers before sending and can cancel after
  JoinResults arrive, after a direct Accept question is pending, or during deinit
  before freeing its callback context. This is still Zig-shape-only and not a
  Stable downstream API.
- Experimental L4 transparent proxy Join relay: Join requests targeting
  cross-peer proxy exports can forward to the proxy source peer, relay
  downstream JoinResult/exception Returns upstream, and preserve downstream
  lifetime until upstream Finish, including retry state if forwarding that
  downstream Finish fails. Regressions cover source unavailable, downstream send
  failure during Join forwarding, downstream results/exception Return relay
  failure, unexpected downstream Return cleanup, downstream Finish retry, owner
  teardown, source teardown before/after downstream Return, target mismatch
  through relay, Finish-before-Return, and OOM rollback. There is no public
  Stable `sendJoin` convenience or
  cross-implementation L4 claim.
- Experimental L4 addressed Join registry pilot: `AddressedJoinNetwork` carries
  opaque application addresses in provision tokens, resolves already-live
  registry entries, and can call an app-supplied connector for unknown addressed
  provisions. `just e2e-l4-zig` proves the registered-peer JoinResult→Accept
  path over real Zig↔Zig loopback TCP; focused regressions cover connector
  malformed-token/no-dial, shared-cache lease cleanup,
  network-teardown-before-release, and OOM-before-dial behavior. This is still
  not a production Join dialer or stable address format.
- L4 cross-implementation recon: C++ still has no callable generic Join hook for
  this harness, and the Go probe confirms generated Join/twoparty shapes but no
  runtime dispatch for `Message.join`.

## Integration Work

1. Update the `capnp-zig` package pin to the final pushed sprint SHA.
2. Refresh any generated Zig artifacts or bindings that import the runtime.
3. If `capnp-deno` exercises QUIC loopback tests with self-signed certificates,
   set `ClientOptions.insecure_skip_verify = true` only in those tests.
4. Keep production QUIC clients on the default certificate verification path.
5. Treat all L3 APIs as Experimental: pin exactly, keep wrappers narrow, and do
   not expose them as a stable `capnp-deno` contract yet.
6. Treat L2 persistence as Experimental too. If Deno exposes Save/Restore, keep
   wrappers narrow, copy sturdy-ref bytes out of callbacks, release restored
   imports when done, and use generated `Persistent.save` for `sealFor` until
   the high-level Zig convenience grows a sealed-save API.
7. Add downstream tests for the exact behavior `capnp-deno` needs:
   Provide/Accept handoff, auto-pickup through `VatNetwork`, embargoed pickup if
   promise pipelining is exposed, and forwarded cap-bearing params/results if the
   Deno layer exposes capability arguments or result caps.
8. Add downstream L2 tests if persistence is surfaced: save, reconnect, restore,
   resave, unknown ref, malformed/exception Return handling, and callback failure
   after restore.
9. Do not surface L4 as a stable downstream API yet. For exact-pin Zig-only
   experiments, wrap `JoinCoordinator` narrowly and keep the compact key/result
   format private to the experiment.

## Caveats

- L2 persistence remains Experimental. Sturdy-ref bytes and restore semantics are
  realm/vat-specific; do not expose them as a stable cross-runtime contract yet.
- L3 has a Zig↔C++ TCP proof, but not a full reference matrix. Do not expose it
  as a stable cross-implementation downstream contract yet.
- L4 Join remains Experimental and Zig-only in `capnp-zig`; transparent proxy
  relay and addressed registry/connector proofs are implemented only for
  capnp-zig peers and do not imply cross-implementation Join behavior.
- `LoopbackVatNetwork` is a test/in-process helper, not a production addressing
  policy.
- QUIC remains Experimental and opt-in behind `-Dquic=true`.
- The Stable API snapshot still covers the two-party core only; L2/L3/QUIC
  changes may move on 0.x minor bumps.
