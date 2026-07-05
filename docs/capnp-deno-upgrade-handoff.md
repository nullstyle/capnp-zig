# capnp-deno Upgrade Handoff

This note is for the next `capnp-deno` session that updates its dependency on
`capnp-zig`.

## Target

Pin `capnp-deno` to a `capnp-zig` commit at or after the L2 persistence
hardening sprint. In the implementation handoff for that sprint, use the final
pushed commit SHA as the exact dependency target.

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

## Caveats

- L2 persistence remains Experimental. Sturdy-ref bytes and restore semantics are
  realm/vat-specific; do not expose them as a stable cross-runtime contract yet.
- L3 remains Zig-to-Zig only. Do not promise cross-implementation L3 pickup yet.
- `LoopbackVatNetwork` is a test/in-process helper, not a production addressing
  policy.
- QUIC remains Experimental and opt-in behind `-Dquic=true`.
- The Stable API snapshot still covers the two-party core only; L2/L3/QUIC
  changes may move on 0.x minor bumps.
