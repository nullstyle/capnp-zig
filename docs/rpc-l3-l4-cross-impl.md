# RPC L3/L4 Cross-Implementation Status

Status: Experimental. Exact pins recommended.

This note records the first cross-implementation Level-3 result and the current
Level-4 blocker. It is intentionally narrower than the stable two-party
interop matrix.

## Positive L3 Result

`just e2e-l3-cpp` runs the `l3_l4_interop` scenario:

- Vat A and Vat B are capnp-zig peers.
- Vat C is a C++ reference host built in Docker from the vendored Cap'n Proto
  C++ 2.0 source.
- B bootstraps C++ `Number`.
- B returns an unresolved promise to A, then resolves it with
  `resolvePromiseExportToThirdParty`.
- A receives `thirdPartyHosted`, asks its `VatNetwork` for the A↔C++ peer,
  sends `Accept`, receives the direct cap, and invokes `Number.getNumber()`
  over the A↔C++ TCP connection.

The accepted cap is not invoked through the vine proxy. The TAP gate asserts
direct A↔C++ invocation and local drain of the vine/pickup transient state.

## C++ Notes

The Docker C++ backend builds vendored Cap'n Proto 2.0, whose generic
`VatNetwork` exposes Level-3 hooks (`introduceTo`, `connectToIntroduced`,
`awaitThirdParty`, `completeThirdParty`, and `generateEmbargoId`). The local
Homebrew 1.4 headers do not expose those hooks, so the local C++ build compiles
a guarded stub for this scenario; the real L3 proof is the Docker gate.

## Other Implementations

- Go: vendored go-capnp has a `Network3PH` interface with introduce/forward/
  await/complete hooks, but this sprint did not build a TCP e2e harness for it.
  Treat Go L3 as promising but unproven here.
- Rust: the current repo e2e adapter uses `twoparty::VatNetwork`; no ready L3
  TCP hook was integrated this sprint.
- Python: the current pycapnp e2e adapter exposes only the two-party path for
  this repo's scenarios; no L3/L4 hook was integrated this sprint.

## L4 Result

`just e2e-l3-cpp` includes a receive-shape check for the C++-convention
`JoinKeyPart` struct used by the probe schema:

- `joinId :UInt32`
- `partCount :UInt16`
- `partNum :UInt16`

That proves capnp-zig can consume the shape used by the cross-impl probe. It
does not prove L4 runtime interop. C++ generic `VatNetwork` still marks Level 4
as TODO, and capnp-zig still has no public `Peer.sendJoin`, direct
`JoinResult` connection flow, or multi-hop Join relay.
