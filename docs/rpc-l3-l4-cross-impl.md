# RPC L3/L4 Cross-Implementation Status

Status: Experimental. Exact pins recommended.

This note records the first cross-implementation Level-3 result and the current
Level-4 blocker. It is intentionally narrower than the stable two-party
interop matrix.

## L3 Result

`just e2e-l3-cpp` runs the `l3_l4_interop` matrix:

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

The same gate now also covers the major C++-interop failure contours:

- bad `ThirdPartyToContact` host data falls back to the vine proxy without
  attempting pickup;
- invalid/malformed semantic completion tokens and unknown completion nonces
  produce deterministic Accept exceptions;
- a C++ await-side rejection produces one pickup exception and drains the vine;
- a C++ disconnect after `Provide` does not prevent direct A↔C++ pickup;
- duplicate/late `Accept` is rejected without yielding a second cap;
- a hosted-cap exception after successful pickup is reported through the direct
  A↔C++ call path;
- every scenario checks local question/provide/vine/embargo state drains after
  releases.

The Zig runtime also supports using a completed, explicitly retained outbound
answer as the provided target. `sendProvideFromRetainedAnswer` and
`resolvePromiseExportToThirdPartyFromRetainedAnswer` transfer its Finish
lifetime into the vine/Provide coupling and preserve the promised-answer op path
for fallback forwarding and direct pickup. That path currently has focused
Zig↔Zig coverage only; it is not an additional scenario in the C++ matrix
described above.

The same narrow evidence boundary applies to automatic redirected results.
`ThirdPartyResultPolicy.vat_network` can resolve an inbound
`sendResultsTo.thirdParty` contact through an attached Zig `VatNetwork`, create
the recipient-side synthetic answer, remap capability-bearing results through
pinned proxies, replay calls pipelined before the result, and commit its target
Return before settling the source with `resultsSentElsewhere`; Finish follows
the normal answer lifecycle and may arrive early/reentrantly. Focused cases
also cover early recipient Finish and
source/target deinit reentered from synchronous delivery. The default remains
`.reject`, and `.application` retains the existing manual
`resultsSentElsewhere` contract. This automatic path is covered only by
Zig↔Zig regressions; the vendored C++ lane does not accept the inbound
redirected call, and no Go/Rust/Python claim is added. Its focused evidence
also covers ThirdPartyAnswer, pre-/post-delivery target-result, and
post-delivery source-marker send failure; allocation-index cleanup; distinct
network/source/target allocators; reentrant
deinit; and transport close without deinit on either side.

## L3 VatC Hosting Result

`just e2e-l3-vatc` reverses the roles: one vendored C++ process plays vats A
(recipient) and B (introducer) over real TCP against a two-peer Zig VatC. Both
sides emit TAP and must pass. Its nine scenarios are:

- `happy`, `embargo`, and `disconnect` for ordinary serving, accept-Disembargo
  ordering, Release, and abrupt-close lifecycle;
- `unknown-token`, `park-expiry`, and `park-adopt` for the order-independent
  rendezvous, failed-answer pipelining, timeout, and later-Provide adoption;
- `pipelined-provide` and `pipelined-provide-chain` for both live
  `receiverHosted` import-pin shapes (the accepted cap reaches C++'s own local
  service, not a host substitute);
- `park-fairness`, which gives A a one-entry park quota, requires its second
  unmatched Accept to be refused, proves sibling B can complete a legitimate
  reverse-direction handoff while the first park remains live, and then expires
  it from ordinary non-Accept traffic. The manual Zig frame pump reports EOF/reset/write
  failure through idempotent `HostPeer.notifyTransportClosed()`, and host-side
  assertions require holder reservations and gauges to drain.

This is containment evidence, not authentication: completion tokens remain
opaque application data. Go is still source-recon only, and the Rust/Python
adapters remain two-party, so no broader L3 hosting interop claim is made.

## C++ Notes

The Docker C++ backend builds vendored Cap'n Proto 2.0, whose generic
`VatNetwork` exposes Level-3 hooks (`introduceTo`, `connectToIntroduced`,
`awaitThirdParty`, `completeThirdParty`, and `generateEmbargoId`). The local
Homebrew 1.4 headers do not expose those hooks, so the local C++ build compiles
a guarded stub for this scenario; the real L3 proof is the Docker gate.

The C++ harness is intentionally test-only. It uses the high bits of the nonce
in the test schema's token structs to select failure-injection modes. That is
not a production VatNetwork addressing policy.

## Other Implementations

- Go: vendored go-capnp has a `Network3PH` interface with introduce/forward/
  await/complete hooks, but `just e2e-l3-go` now confirms the runtime is not
  ready for a TCP L3 proof: inbound `Accept`/`Provide`, `thirdPartyHosted`
  pickup with a network, `awaitFromThirdParty`, accept-context `Disembargo`, and
  same-network embargo/locality handling still hit `TODO: 3PH` guards or TODO
  comments in vendored `rpc.go`. The same probe now checks L4 source shape:
  generated Go `rpc.capnp` bindings expose `Message.join`, and the vendored
  twoparty schema carries `JoinKeyPart` / `JoinResult`, but the receive loop
  still has no `Message_Which_join` runtime dispatch. Treat Go L3/L4 as
  source-recon only until those paths are implemented upstream or locally
  vendored.
- Rust: the current repo e2e adapter uses `twoparty::VatNetwork`; no ready L3
  TCP hook was integrated this sprint.
- Python: the current pycapnp e2e adapter exposes only the two-party path for
  this repo's scenarios; no L3/L4 hook was integrated this sprint.

## L4 Result

`just e2e-l3-cpp` includes shape checks for the C++/twoparty-convention L4
structs used by the probe schema and a source-backed C++ runtime-surface probe.

`JoinKeyPart`:

- `joinId :UInt32`
- `partCount :UInt16`
- `partNum :UInt16`

`JoinResult`:

- `joinId :UInt32`
- `succeeded :Bool`
- `cap :Capability`

That proves capnp-zig can consume the shapes used by the cross-impl probe. The
source probe also asserts the current C++ runtime blocker remains exact:

- `rpc-prelude.h` exposes the generic Level-3 `VatNetworkBase` hooks used by
  the e2e lane;
- `rpc-prelude.h` exposes no callable generic L4 hook named `newJoiner`,
  `addJoinResult`, or `acceptConnectionFromJoiner`;
- `rpc.h` still leaves typed `VatNetwork` Level-4 support as a TODO-only
  section;
- `capability.h` still exposes only TODO comments for client/server Join
  extension points.

This does not prove cross-implementation L4 runtime interop. capnp-zig now has a
raw Experimental `Peer.sendJoinExperimental` helper, a Zig↔Zig JoinResult
runtime pilot behind `rpc.vat.join.JoinNetwork`, an Experimental
`JoinCoordinator` for the compact Zig JoinResult→Accept workflow, an
Experimental `AddressedJoinNetwork` registry/connector proof over real Zig↔Zig
TCP, and C++/Go source-surface recon. The Zig pilot can return compact Zig
JoinResult payloads, resolve them to an already-live direct peer or through an
application-supplied connector, send `Accept`, and invoke the accepted cap. It
can also relay Join requests through transparent cross-peer proxy exports to the
proxy source peer; the real `JoinCoordinator` now tracks the originating peer for
each part, rejects duplicate local part sends, Finishes each JoinResult on the
correct upstream connection after pickup, retries only unfinished JoinResult
Finishes after a partial send failure, Finishes JoinResults after terminal
direct Accept Returns, releases retained `Joined` leases when a later terminal
JoinResult failure makes the aggregate impossible, and can cancel a pending
direct Accept if its Return is lost or the coordinator is dropped. During
synchronous direct-Accept pickup it also suppresses the generic Accept
auto-Finish and sends that Finish after the callback unwinds, so a trailing
Finish OOM cannot restore coordinator-owned callback state after the cap has
been retained; if all immediate Finish attempts fail, later accepted-cap cleanup
retries the same host answer. Direct Accept peers also keep coordinator
back-links so peer-first teardown can Finish any unfinished Accept answer and
neutralize borrowed coordinator pointers. The transparent proxy relay
also keeps owner/source state when forwarding the downstream Finish fails, so a
retry can drain the downstream JoinResult lifetime, rejects non-import source
targets before allocating relay state, neutralizes the source question during
owner-peer teardown even if that best-effort downstream Finish send fails, and degrades
unexpected downstream Return tags to a local exception while draining relay
state. It still has no Stable `Peer.sendJoin`, no production Join addressing
policy or bundled dialer, no multi-hop relay beyond that transparent proxy case,
and no cross-implementation L4 runtime claim.

The current C++ blocker is source-backed and e2e-checked: vendored Cap'n Proto
C++ exposes the generic Level-3 `VatNetwork` hooks used by this lane, while
Level-4 Join is still described only in the commented network pseudo-interface
in `rpc.capnp` (`newJoiner`, `Joiner.addJoinResult`, `Joiner.connect`, and
`acceptConnectionFromJoiner`). No callable generic C++ `VatNetwork` Join hook is
available for the TCP e2e harness today.
