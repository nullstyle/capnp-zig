# Supported Surface (v0.10.0)

This is the authoritative statement of what `capnpc-zig` promises a consumer at
v0.9.0 — which modules are stable, what the RPC implementation conforms to, the
error contract, and the known limitations you are opting into. It is the single
source of truth; where other docs disagree, this file wins.

Pair it with [`stability.md`](stability.md) (per-module / per-platform maturity)
and [`CHANGELOG.md`](../CHANGELOG.md) (what changed).

## Semver at 0.x

The project is pre-1.0. Per semver, **0.x minor bumps may break the API** — and
they will, deliberately, for anything marked Experimental below. Pin an exact
version (`zig fetch --save …#v0.10.0`) and read the CHANGELOG before bumping.

- **Stable** (serialization + codegen + the two-party RPC core): the Stable
  surface is **frozen and CI-gated**. It is pinned by
  [`docs/api-snapshot.txt`](api-snapshot.txt) — the categorized Stable-only
  contract — and `zig build check-api` fails on any unreviewed drift. Breaking
  changes are avoided within 0.3.x and called out in the CHANGELOG when
  unavoidable.
- **Experimental** (retained outbound-answer lifetimes, L3/L4 three-party
  origination, reflected-cap resolve, QUIC, persistence vat-restore, events,
  `io_backend`, the demoted transport/ctor variants): may break at any 0.x
  minor bump. Functional and tested, but the API
  is not frozen; its surface evolves in
  [`docs/api-snapshot-experimental.txt`](api-snapshot-experimental.txt) (ungated).
  The QUIC-enabled surface is recorded separately in
  [`docs/api-snapshot-experimental-quic.txt`](api-snapshot-experimental-quic.txt),
  because `check-api` runs without `-Dquic=true` and otherwise sees only the
  disabled stub. `zig build -Dquic=true check-api-quic` maintains it and gates
  one real invariant: enabling QUIC must leave the FROZEN
  `api-snapshot.txt` byte-identical. Neither experimental snapshot is
  diff-checked in CI: their contents are not byte-stable across platforms
  (`std.Thread.Id` is `u64` on macOS and `u32` on Linux, so every thread-id
  field renders differently), which is precisely why only the Stable file can
  be a contract.

## Modules — which to import

The package exposes two library modules:

| Module | Contents | Import when |
|---|---|---|
| `capnpc-zig` | Full surface: serialization + codegen + RPC runtime + transport | You use RPC (`rpc.transport.tcp.connect`, generated interface clients/servers). |
| `capnpc-zig-core` | Serialization + codegen only; no RPC/transport in the module graph | You only read/write messages — keeps the RPC/transport code out of your build. |

Generated code imports the runtime as `@import("capnpc-zig")`; wire that name to
whichever module above you selected (see
[`build-integration.md`](build-integration.md)). This is the canonical rule —
earlier docs that mention only one name are being reconciled to point here.

## Stability by layer

| Layer | Tier |
|---|---|
| Wire format / packing (`message`) | **Stable** |
| Schema types + parsing (`schema`, `request`) | **Stable** |
| Code generation (`codegen`, the `capnpc-zig` plugin) | **Stable** |
| Reader convenience (`reader`) | **Stable** |
| RPC two-party core — frozen entry points (`rpc.wire.protocol` / `.framing`, `rpc.caps.table`, narrowed `Connection`, `ClientSession`, `ServerSession.accept`, the canonical two-party `Peer` surface + `CallError` / callback typedefs / `PeerLimits`, generated interface code) | **Stable** (frozen, CI-gated) |
| RPC retained outbound-answer lifetimes (`CallOptions` / generated `*WithOptions` / explicit Finish), L3 three-party origination, L4 Join runtime pilot/readiness, reflected-cap resolve (`resolvePromiseExportToImport`), `ServerSession`-as-a-type, `VatNetwork`, `JoinNetwork`, QUIC, persistence vat-restore, events, `io_backend`, demoted ctor/transport variants | **Experimental** |
| WASM host ABI (`src/wasm`) | **Experimental** |

The frozen Stable RPC surface is exactly the categorized set in
[`api-snapshot.txt`](api-snapshot.txt); the Experimental RPC surface is tracked
(ungated) in [`api-snapshot-experimental.txt`](api-snapshot-experimental.txt).
See [`stability.md`](stability.md) for the full matrix and per-platform status.

## Schema-language support

What the `capnpc-zig` code generator does with each Cap'n Proto schema-language
feature today. "Supported" means idiomatic typed Zig accessors; "partial" and
"unsupported" carry the caveats below.

| Feature | Status | Notes |
|---|---|---|
| Structs, groups, named/unnamed unions | supported | Union members (slot *and* group) guard on the discriminant and return `error.WrongUnionMember` for the wrong variant. `whichOrdinal()` exposes an unknown discriminant without weakening typed `which()`. |
| Enums | supported (exhaustive + forwarding) | Generated as exhaustive `enum(u16)`; typed getters reject an unknown ordinal with `error.InvalidEnumValue`, while the generated `enumOrdinals()` view reads/writes its logical `u16` value for forwarding. |
| Scalar (XOR) defaults, pointer defaults | supported | Applied on read for numeric/bool/enum and for text/data/struct/list pointer fields. Generated pointer-field `hasXxx()` methods report structural presence separately from the logical default. |
| Flat lists (all element sizes incl. inline-composite struct lists); lists of enum/text/data/interface | supported | Typed `*ListReader` / `*ListBuilder`; `initXxx(count)` on the builder. Enum lists also provide `getOrdinal()` / `setOrdinal()` and retain their `raw()` accessors. |
| Nested lists `List(List(T))` (including deeper nesting) | supported (additive typed view) | Existing raw `getXxx()` / `initXxx()` accessors remain `message.PointerListReader` / `PointerListBuilder`. In parallel, Readers and Builders expose `nestedLists()`: typed recursive `getXxx()` and `initXxx()` / `initXxxInSegment()` views cover scalars, Text, Data, enum, struct, interface/capability, and deeper lists in full and compact profiles; AnyPointer keeps the same raw pointer-list terminal as flat lists. Null inner pointers read as empty lists while `isNull()` preserves the absent/present distinction; every wrapper retains `raw()`. Unknown struct layouts fall back to raw struct-list access, and unresolved enum IDs use ordinal (`u16`) elements. |
| `AnyPointer`, `AnyStruct`, `AnyList`, bare `Capability` | supported (additive shape view) | Existing fields still expose their erased `AnyPointerReader` / `AnyPointerBuilder` API. The request model retains the schema sub-kind in parallel metadata, and structs/groups with constrained `AnyStruct`, `AnyList`, or bare `Capability` slots also expose union-guarded `pointerKinds()` in full and compact profiles. Reader getters return `StructReader`, `AnyListReader`, or `Capability`; Builder getters reopen existing near/single-far/double-far values without replacing them, while init/set methods deliberately replace them. Null lists cast as empty with `isNull()` preserved, malformed inline-composite tags and wire-distinguishable wrong-kind pointers fail, and a nonempty list cannot masquerade as AnyStruct. A layout-A double-far zero-offset struct tag remains indistinguishable from an empty inline-composite list. `AnyListReader` and all Builder shape wrappers retain `raw()`. Unconstrained `AnyPointer` intentionally stays erased. |
| Generics / parameterized types / brands | partial (executable metadata + finite concrete views) | The frozen `schema.Type` union is unchanged. Additive metadata retains parameters, nested named applications, annotation-use brands, interface superclass/method brands, AnyPointer sub-kinds, and `.bind`/`.inherit` scopes. An allocation-free 64-level resolver is shared by validation and generation, checks lexical scope, exact arity, indexes, depth, and cycles, and leaves valid unbound values erased. Scalar generic bindings and malformed graphs are `InvalidSchema`. For finite concrete branded data-struct fields, full and compact Reader/Builder `brands()` views support arbitrary-depth lists, generic struct applications as list terminals, enum/Text/Data/struct/interface terminals, nested branded structs, inherited lexical bindings, and cross-file imported applications/terminals. Existing erased accessors remain; generic RPC clients and implicit generic methods stay erased. |
| Annotations | supported (see caveat) | Parsed and emitted as `<Name>_annotations` / `_field_annotations` / … arrays plus `pub const` definition descriptors. File-level annotation *uses* are dropped. Parsed annotation-use brands are retained in `CodeGeneratorRequest`; generated annotation constants remain the legacy id/value projection. |
| Constants (incl. struct/list/enum consts) | supported | Emitted as `pub const`; pointer-typed consts expose a `get()` reader. |
| JSON / serde | descriptor only | `CAPNP_SCHEMA_MANIFEST_JSON` names the `capnp_<module>_<type>_to_json` / `_from_json` C-ABI symbols an *external* serde tool must supply; no `to_json` / `from_json` bodies are generated. |
| Canonicalization | supported (spec form + schema-aware form) | Two implementations for two jobs. **`canonical.canonicalize` / `canonicalizeFlat` / `isCanonical` (Experimental)** is the spec's actual canonical form: a **schema-free** walk of the raw pointer graph mirroring the reference implementation's `canonicalize()`/`isCanonical()` (rules cited to `layout.c++` in-source, differentially tested byte-for-byte against `capnp convert binary:canonical`). It preserves data for fields no local schema knows about and keeps upgraded lists as written, so it is the one **appropriate as a signing input**. Capabilities cannot be canonicalized (same as the C++ reference). **`schema_validation.canonicalizeMessage` / `canonicalizeMessageFlat` / `validateMessage` (Stable)** stays for **schema-aware equality**; additive `*WithBrand` forms apply a concrete root brand. Legacy forms use an empty root brand but honor concrete nested metadata. Schema-aware re-encoding drops unknown fields and re-encodes upgraded lists — fine for peers on the same schema, but not a signing input. The schema-free `canonical.*` API is unchanged. |
| Cross-file `import` / `using` | supported | Correct relative `@import` for referenced types; only referenced imports are emitted. A `using` alias produces no declaration (frontend-resolved). |

**Caveats worth pinning to memory:**

- **Typed enum access is deliberately exhaustive.** An older typed getter that
  meets a newly-added enumerant still gets `error.InvalidEnumValue`. Use the
  generated `reader.enumOrdinals().getXxx()` /
  `builder.enumOrdinals().setXxx(value)` view, or an enum list's
  `getOrdinal()` / `setOrdinal()`, when a proxy must preserve an unknown value.
  Schema-aware `validateMessage` remains strict and rejects such a value.
- **Pointer presence is structural, not semantic validation.** `hasXxx()` is
  generated for Text, Data, struct, list, AnyPointer, and interface slots. It
  returns false for a null or old-layout-missing slot (even when the field has a
  non-null schema default), and for an inactive/unknown union arm. A malformed
  nonzero pointer is present, so its getter may still fail validation.
- **Brand fidelity is additive, not whole-program generic specialization.**
  Existing field accessors keep the historically erased representation. Use
  `brands()` only when it is emitted for a finite concrete branded data-struct
  field. Supported views compose arbitrary-depth lists, enum/Text/Data/struct/
  interface terminals, generic struct applications as list terminals, nested
  branded structs, inherited lexical bindings, and cross-file imported
  applications/terminals. A valid unbound or recursively infinite application
  keeps erased/manual access; a scalar generic binding is invalid. Generic
  interfaces and implicit generic RPC methods do not gain specialized clients.
  Inspect `schema.TypeMetadata` when tooling needs the original expression even
  though a typed sidecar is unavailable.
- **Fidelity sidecars keep ordinary field safety rules.** `brands()` and
  `pointerKinds()` are generated for main structs and groups in full and compact
  profiles and return `error.WrongUnionMember` for an inactive union arm.
  Reader sidecars apply pointer defaults; Builder getters recursively
  materialize a schema pointer default before mutation when the physical slot
  is null, otherwise reopening the stored near/far pointer, while init/set
  methods replace the slot. A null constrained list is the usual empty Cap'n
  Proto list and a null struct materializes its logical empty value; a
  wire-distinguishable non-null wrong kind is rejected. Layout-A double-far
  empty struct/list tags are inherently ambiguous. Generated-name collisions
  with `Brands` / `PointerKinds` are rejected rather than producing ambiguous
  Zig.

- **Brand generation has a separate expansion budget.**
  `CodegenBudget.max_brand_specializations` defaults to 4096. Configure it with
  `max-codegen-brand-specializations=` on the plugin command line or
  `CAPNPC_ZIG_MAX_CODEGEN_BRAND_SPECIALIZATIONS`; exhaustion fails with
  `CodegenBudgetExceeded` before partial output is accepted.

## Error contract

The one frozen public error set is validation:

- `message.Message.MessageValidationError` — the complete, compiler-enforced set
  `Message.validate` / `validateCounted` can return
  (`src/serialization/message.zig`). A returned error outside it fails to build,
  so the set stays exact.

For RPC, the frozen client-facing set is:

- `rpc.peer.CallError` = `{ RemoteException, Disconnected, CallTimedOut,
  Canceled, UnexpectedReturn }` (`src/rpc/peer/errors.zig`) — returned by the
  generated `Response.unwrap()` / `BootstrapResponse.unwrap()`. This is the
  contract for consuming a call result. In 0.3.0 it is part of the **frozen**
  Stable RPC surface (pinned in `api-snapshot.txt`).

**Deliberately open:** the public user-callback typedefs (`CallBuildFn` /
`QuestionCallback` / `CallHandler` / `SaveHandler` / `RestoreHandler`) keep
`anyerror` by design — an application handler may fail any way it likes; the F1
freeze intentionally left these open. Consume call results through `unwrap()` —
that is the stable, `CallError`-typed path.

## RPC conformance

capnpc-zig implements **Cap'n Proto RPC Level 1** — the two-party core:
bootstrap, calls, returns, finish, promise pipelining, and promise
resolution/embargo. In 0.3.0 this core is **Stable and frozen**: its public
entry points are the categorized Stable set in [`api-snapshot.txt`](api-snapshot.txt),
CI-gated by `zig build check-api`. It is proven bidirectionally against the C++,
Go, Python, and Rust reference implementations in the cross-implementation e2e
matrix (returned-capability invocation, typed pipelining with E-order, and
capability release).

**Performance + soak evidence.** The two-party core carries committed regression
evidence: `bench-rpc` measures round-trip latency (p50/p99) + calls/sec against
a committed baseline (`bench-check`), and the RPC soak harness reports latency
percentiles plus a memory-growth curve asserted flat at ≥100 concurrent peers.
On Windows the soak cannot report success without positive session, call,
chaos-close, and applicable deadline-cancellation counters; the old successful
no-op is gone.
In CI the *pipelined throughput* case is enforced; the *sequential latency*
cases are advisory (a shared runner cannot measure a serialized round-trip
reliably — see [`stability.md`](stability.md)), and gate on a quiet machine via
`bench-check -- --enforce-advisory`.
Answer-lifecycle regressions also cover synchronous transports where `Finish`
re-enters during a results — or Bootstrap — `Return` send: parked
promised-answer calls replay before the recorded answer is immediately cleaned
up (Bootstrap answers commit through the same reserve → send →
commit-or-cleanup discipline, so recording can no longer fail after the frame
is on the wire). The same commit-then-cleanup applies when the `Finish`
preceded the late `Return` entirely, so calls pipelined on a cancelled answer
always receive their own Returns. A late `Return` for a call cancelled by an
early `Finish` honors the Finish's `releaseResultCaps` flag instead of leaking
the results descriptors' wire references, reusing a question id whose
early-Finish tombstone is still undischarged is rejected as
`DuplicateQuestionId`, and nested resolved-answer reservations (a reentrant
transport delivering a new Call/Bootstrap mid-send) are counted so an outer
post-send commit cannot underflow the answer map's reserved capacity.

**Param-capability release.** capnpc-zig settles the capabilities a caller
passes in a Call's params with explicit `Release` frames — from the
post-dispatch auto-release for params a handler ignored, and from the
application's own `releaseImport` for ones it kept (what the generated param
accessors set up when they retain a cap and hand back an owning `Client`).
rpc.capnp forbids pairing those frames with `Return.releaseParamCaps = true`
("the sender must not send separate `Release` messages for them"), so the peer
stamps `releaseParamCaps = false` on every Return answering a call whose params
carried a `senderHosted`/`senderPromise`/`thirdPartyHosted` descriptor — the
same thing the C++ reference does on every Return it sends. Answers that took no
param references (Bootstrap/Provide/Accept/Join, calls with cap-free params, the
`sendResultsTo = thirdParty` refusal issued before any import is noted) keep the
schema default `true`, where the flag is a no-op. Applications do not set this
flag; a handler's only obligation is to retain a param cap it intends to keep.

The reflected-capability resolve/embargo handshake — a promise capability
resolved to a *caller-hosted* cap (`Peer.resolvePromiseExportToImport`), driving
the `senderLoopback`/`receiverLoopback` `Disembargo` — is exercised end to end by
the `resolve_disembargo` e2e scenario. As of 0.3.0 (W1) capnp-zig relays the
reflected call as a plain `sendResultsTo=caller` call and translates the results
straight back onto the caller's pipelined question, a shape **all four** reference
clients consume: the go-capnp and capnp-rpc client directions are de-SKIPped and
now pass. The scenario also exercises **server-invokes-a-client-cap** (cap in call
params) and **disconnect-mid-call** cross-impl (E2). The one remaining `SKIP` is
`zig-client → python-server`: pycapnp cannot host the reflecting server. The
matrix is asymmetric only because of that single reference-library gap, not
capnp-zig behavior — see Known limitations #4.

Note this cross-implementation matrix runs **per push on the Linux CI tier**
(the `e2e-zig` job), alongside the Zig↔Zig self-interop e2e
(`zig build e2e-self`). It does not run on the macOS or Windows tiers, because
hosted runners there cannot run Linux containers.

Beyond Level 1 (all **Experimental**, outside the frozen contract):

- **Retained outbound-answer lifetimes:** the default `sendCall*` and generated
  `callXxx` methods still use automatic result lifetime and send `Finish` after
  dispatching the terminal `Return`. Their additive `*WithOptions` forms accept
  `rpc.peer.CallOptions`; `.result_lifetime = .retained` keeps the remote answer
  open after the callback until `Peer.finishRetainedQuestion(question_id,
  release_result_caps)` succeeds or ownership is transferred to a Level-3
  handoff. Generated `Client`, `PipelinedClient`, and
  `callXxxPipelinedWithOptions` entry points expose the policy; streaming
  fire-and-forget calls remain automatic. A retained callback is delivered at
  most once. If parsing or the callback reports an error after the Return is
  visible, the peer reports it through `on_error` and still returns the question
  id so the caller can Finish the retained answer. Finishing before Return,
  finishing twice, or manually finishing a
  transferred answer returns a state-specific error, and a failed Finish send
  leaves the lifetime retryable. `PeerLimits.max_retained_questions` defaults
  to 1024 and applies before a Call is emitted; `PeerStats` separates
  caller-owned and transferred retained answers, while the redacted
  `retained_questions` resource reports pressure and rejection. This API is
  Experimental even though the existing automatic call surface remains
  unchanged.
- **Level 2 (persistence):** Save/Restore SturdyRef hooks are present
  (`rpc.peer` persistence surface) and documented in
  [`rpc-persistence.md`](rpc-persistence.md). Current mainline evidence covers
  reconnect/resave, malformed params/results, Return send failures, callback
  failure after restored-cap retention, independent hook clearing, and allocator
  rollback for the persistence entry points. Experimental; the vat-level restore
  interface and realm conventions may still change.
- **Level 3 (three-party handoff):** a minimal **origination** slice has landed
  as of 0.3.0 (Experimental): `Peer.sendProvide` / `sendAccept`,
  `resolvePromiseExportToThirdParty`, `sendThirdPartyAnswer`,
  `registerPendingThirdPartyAwait`, a recipient auto-pickup seam
  (`setHandoffPickupHandler`), the `rpc.vat.network.VatNetwork` addressing seam
  (+ `LoopbackVatNetwork`), and `thirdPartyHosted` descriptor emission — in
  addition to the inbound `Provide` / `Accept` / `Join` / `ThirdPartyAnswer`
  handling that was already present. This arc is **lightly soaked and
  Experimental**. Main now has one positive cross-implementation proof:
  `just e2e-l3-cpp` runs a Zig↔C++ Level-3 handoff over real TCP against the
  C++ reference stack built from vendored Cap'n Proto 2.0, with Vat A accepting
  and invoking the hosted cap directly on its A↔C++ connection. This is
  C++-first evidence, not a full reference matrix: `just e2e-l3-go` now checks
  that vendored go-capnp exposes `Network3PH` hook names but still leaves the
  required runtime paths (`Accept`/`Provide`, `thirdPartyHosted` pickup with a
  network, `awaitFromThirdParty`, accept-context `Disembargo`, same-network
  locality) behind `TODO: 3PH` guards, and the current Rust/Python e2e adapters
  expose only the two-party path. The former gap — the introducer not
  forwarding the *original* parked pipelined calls on a promise to the host (they
  hit the Level-1/2 rejecting vine) — is now
  **RESOLVED**: when the handed-off promise resolves, the introducer forwards
  each parked pipelined call cross-peer to the capability host (targeting the
  provided cap on the introducer↔host connection) and relays the host's result
  back to complete the caller's original pipelined question, preserving e-order
  (spec `rpc.capnp:898` and the handoff ordering section: parked calls reach the
  host in send order and strictly before any post-pickup direct call) and the
  vine refcount/liveness coupling. On main, this forwarding also remaps
  cap-bearing params/results by minting cross-peer proxy exports, so caller-hosted
  param caps and host-returned result caps can be invoked through the relay. The
  current mainline hardening evidence also covers loopback token duplicate/unknown
  paths, `sendProvide` / `sendAccept` allocator rollback, vine/provide teardown
  ordering, embargoed pickup ordering, auto-pickup callback-failure cleanup,
  auto-pickup internal Accept Finish retry after synchronous host-answer commit,
  and cross-peer proxy cleanup (the reentrant-Finish promised-answer drain the
  pickup path relies on is covered by the two-party answer-lifecycle suite). The C++
  interop lane is now a small failure matrix rather than only a happy-path proof:
  bad contact data falls back to the vine proxy, invalid/unknown completion tokens
  and await-side C++ rejection produce deterministic pickup exceptions, a C++
  disconnect after `Provide` still permits direct pickup, duplicate/late `Accept`
  is rejected without a second cap, hosted-cap exceptions report over the direct
  A↔C++ path, and every case asserts local Provide/Accept/vine/embargo state
  drains. A completed retained answer can now be the provided target:
  `sendProvideFromRetainedAnswer` and
  `resolvePromiseExportToThirdPartyFromRetainedAnswer` take a promised-answer
  op path, transfer the source lifetime into the vine/Provide coupling, preserve
  that exact target for vine fallback and direct pickup, and Finish the Provide
  and source answer when the coupling ends. Failure before the Provide commit
  rolls caller ownership back. After that commit the protocol must conservatively
  treat the Provide as delivered: a later combined Resolve failure consumes the
  source into cleanup, and failed protocol-owned Finish remains queued for
  `checkDeadlines()` maintenance. This retained-target path currently has Zig↔Zig regression
  coverage; it is not an additional C++ interop claim. Do not depend on the L3
  surface for production interop without exact pins.

  Inbound redirected results now have a second opt-in policy alongside that
  origination surface. `ThirdPartyResultPolicy.vat_network` uses the
  `VatNetwork` already attached to the callee-facing peer to resolve
  `ThirdPartyToContact`, announce a synthetic callee-range answer with
  `ThirdPartyAnswer`, and deliver the handler's ordinary results/exception
  Return on the introduced peer. Capability-bearing results are remapped
  through pinned cross-peer proxies, and that answer participates in the normal
  unresolved/resolved, pipelining, terminal Return, and Finish lifecycle. The
  target Return commits before the original caller receives
  `resultsSentElsewhere`; Finish follows normally or may arrive early. The policy is
  borrowed-network, single-thread-affine, Experimental, and Zig↔Zig-only.
  `.reject` remains the default and `.application` retains the existing manual
  routing contract. No production dialer, authentication/identity policy, or
  L4 integration is implied. Current focused evidence covers capability
  remap/proxy Release, pipeline-before-result, early Finish and queued-child
  drain, missing-network refusal, pre-/post-delivery send boundaries,
  allocation-index cleanup, distinct network/source/target allocators,
  reentrant source/target deinit, transport close without deinit, and route
  drain. An ambiguous ThirdPartyAnswer write error is transport-terminal because
  that frame has no acknowledgement.

- **Level 3 — HOSTING across multiple connections (VatC role):** as of this
  change a vat whose `Provide` and `Accept` arrive on **different peers** can
  serve the handoff (Experimental). The pieces: a vat-wide
  `rpc.vat.provisions.ProvisionIndex` (refcounted, connection-independent
  provision objects; matched handoffs remain independent of later index
  teardown, while parked holder records are centrally detached on adoption,
  Finish, expiry, transport close, and peer/index teardown), `Peer.attachProvisionIndex`
  / `detachProvisionIndex`, the `rpc.peer.Vat` facade (owns the index + the
  accept-embargo CSPRNG; `enroll` one peer per connection), a Release-immune
  `handoff_ref_count` export ref class pinning provided/proxied targets, the
  spec-form accept-`Disembargo` host arm (promisedAnswer target naming the
  Provide question; per-provision embargo slots with find-or-create for the
  Disembargo-before-Accept race), Accept-before-Provide **parking** with
  adoption, owner-side re-resolution for stored promised targets, the
  introducer's forwarded Disembargo rewritten to the spec form and ordered
  after the parked-call replay, and entropy-backed 16-byte embargo ids
  (fail-closed seeding). **Known limitations, honestly:**
  1. **LIFTED (was: fail closed).** `receiverHosted` provide targets (the host
     provided a capability it *imports* from the introducer) are now **served
     cross-peer** via deferred-Release import pinning — exactly the design's
     L9/L17 mechanism: the import entry is retained under a `handoff_pin_count`
     lease taken at Provide registration (stored `.local{receiverHosted}`
     targets) or at serve time (stored `.promised` targets re-resolving to an
     import), every outbound wire Release for a pinned import is WITHHELD into
     a `deferred_release` tally (a `receiverHosted` descriptor grants no
     transferable wire reference, so the withheld count is the only thing
     keeping the introducer-side export alive across the [Provide, serve)
     window), and the last unpin emits the accumulated count as one exact
     Release — granted == released across the whole handoff, just later.
     Embargoed accepts of such targets complete at Disembargo time through the
     same serve. Residual honest constraints: a stored-`.promised` target whose
     re-resolved import has since died still fails closed
     (`CrossPeerProvisionTargetUnavailable` — the lift serves live targets, it
     does not resurrect dead ones). An introducer-side service that RETAINS
     the provided cap it received as a call param no longer has to declare
     `releaseParamCaps = false` by hand: the peer stamps it on every Return
     answering a call whose params granted import refs, so the introducer keeps
     the export the handoff needs. Retaining the cap (`InboundCapTable.retain*`,
     which the generated param accessors already do) is the whole application
     obligation.
  2. The vat (index/`Vat` + all enrolled peers) is **single-threaded**;
     `WorkerPool`-hosted multi-peer vats remain unsupported. WorkerPool peers
     do receive per-worker OS-seeded CSPRNG entropy and fail closed if seeding
     is unavailable; entropy is no longer the limitation. The constraint is
     shared vat state: `Vat.enroll` gives every enrolled peer the same index
     and `&self.rng`, neither synchronized across worker threads.
  3. **Cross-implementation hosting is PROVEN against the C++ reference, and
     only against it.** `just e2e-l3-vatc` runs the vendored Cap'n Proto 2.0
     reference as vats A (recipient) and B (introducer) over real TCP against
     a capnp-zig two-peer VatC host: C++ emits the Provide, the
     `thirdPartyHosted` resolve, the Accept, and the spec-form forwarded
     accept-Disembargo; the Zig host registers the provision on one peer,
     serves the Accept cross-peer from the sibling, releases the embargoed
     Accept on the Disembargo, and drains leak-free. Nine scenarios (happy,
     embargo, unknown-token, disconnect, park-expiry, park-adopt,
     park-fairness, pipelined-provide, pipelined-provide-chain) assert on both sides — the last two are the
     `receiverHosted` lift proven against the reference: C++ introduces a
     still-pipelined cap that re-resolves to C++'s own local capability, and
     the accepted cap must reach it (both stored forms, site 1 and site 2).
     `park-expiry` covers the failed-answer directions of the broken-pipeline
     rule: the parked-accept TTL evicts an Accept with an exception, and both a
     call pipelined on it *before* it failed (drained from the pending answer)
     and one arriving *after* the Return (answered from the recorded exception)
     must come back with a copy of that same exception rather than hanging.
     `park-adopt` proves the other half of the order-independent rendezvous
     (rpc.h:483-492): an Accept naming a provision that does not exist YET
     parks, and is ADOPTED and served when the Provide naming that token
     arrives. Together with `unknown-token` (remains parked throughout its
     live observation window) that covers both
     directions of "the two calls can happen in any order".
     `park-fairness` gives one recipient a one-entry test quota, proves its
     second unmatched Accept is refused while a sibling completes a real
     reverse-direction handoff with the first park still live, then drives
     expiry from ordinary traffic and
     verifies close/teardown refunds the reservations.
     **Still unproven:** every other implementation (go-capnp's 3PH is `TODO`,
     Rust/Python adapters are two-party only), and — even for C++ —
     redirected returns / `ThirdPartyAnswer` (absent from the vendored C++).
     First contact found and fixed one genuine host defect —
     reflected-loopback question ids collided with the remote's inbound answer
     ids — which no Zig↔Zig test had exposed.
  4. **LIFTED (was: no public open-answer origination).** A promisedAnswer
     **provided target** whose answer cap is settled still resolves to a
     concrete stored target at Provide time (serves cross-peer); the
     stored-`.promised` form (promise-valued answer caps) serves via owner-side
     ops re-resolution to a fixed chain depth of 4, and fails closed beyond it
     or when the answer vanished. For an answer that must remain open, originate
     the call with `.result_lifetime = .retained`, wait for its terminal Return,
     then pass the question id plus promised-answer ops to
     `sendProvideFromRetainedAnswer` or the combined
     `resolvePromiseExportToThirdPartyFromRetainedAnswer`. A successful setup
     transfers Finish ownership to the coupling; the application can no longer
     Finish that question directly.
  5. Parked accepts (Accept-before-Provide) remain **unauthenticated**: the
     recipient token is arbitrary bytes and needs no prior `Provide`, bootstrap,
     or handshake. Admission is therefore containment, not identity. In
     addition to the vat-wide ceilings, each peer defaults to 64 parks and
     16 KiB of attributable bytes (normalized token plus embargo bytes, charged
     per Accept even for a shared token). Transport close detaches that peer's
     parked and embargo-queued holder records; active provider-owned provisions
     deliberately remain valid. The high-level `Vat` defaults to a 30-second
     TTL, requires a custom clock or value-stored `Options.io`, checks the O(1)
     next deadline at every inbound-frame boundary and from deadline
     maintenance, and exposes an explicit best-effort sweep. The raw
     `ProvisionIndex` keeps `park_ttl_ms = null` for compatibility. Explicit
     null is also the `Vat` opt-out. `ProvisionIndex.stats()` / `Vat.stats()`,
     per-peer park gauges, redacted pressure/rejection resources, and
     `TimeoutKind.parked_accept` provide Experimental operability without
     exposing token or frame contents.
- **Level 4 (Join):** a guarded Zig↔Zig runtime pilot is present and
  documented in [`rpc-l4-join-readiness.md`](rpc-l4-join-readiness.md).
  `Peer.sendJoinExperimental` can originate raw Join parts, and inbound `Join`
  can collect local JoinKeyPart structs, compare resolved targets, return
  provided caps on the legacy raw-helper path, or, with an Experimental
  `rpc.vat.join.JoinNetwork` attached, return compact Zig JoinResult payloads
  that the joiner resolves into a direct `Accept` and callable cap.
  `JoinCoordinator` is the first Experimental Zig-shape helper above the raw
  sender: it sends compact key parts, collects matching JoinResults, sends
  direct Accept, retains/releases the accepted cap, and Finishes JoinResult
  questions on their originating peers after pickup; it tracks Finish state per
  JoinResult question so partial send failures retry only unfinished questions.
  During synchronous direct-Accept pickup it suppresses the internal Accept
  auto-Finish, keeps return-error restoration disabled from question creation,
  sends the Accept Finish only after the callback unwinds, and records the
  Accept answer until a later cleanup path successfully drains it. The direct
  Accept peer also keeps a teardown back-link to neutralize coordinator-owned
  accepted caps and unfinished Accept answers if that peer deinits first.
  It rejects duplicate local part numbers before sending, treats malformed or
  exception JoinResult Returns as terminal failed results that still Finish the
  affected question, treats mismatched successful JoinResults as terminal without
  retaining `Joined` leases while preserving retryable Finish state, and can
  cancel after JoinResults arrive, including a pending direct Accept whose Return
  is lost. If the direct Accept returns an exception, malformed result, or other
  terminal Return, the coordinator still Finishes JoinResult lifetimes and drops
  retained `Joined` inputs because the host-side provision has already been
  consumed.
  Dropping a coordinator best-effort cancels pending Join and Accept questions
  before freeing its callback context. Transparent cross-peer proxy exports can
  relay Join requests to their source peer, relay downstream JoinResult/exception
  Returns upstream, and keep the downstream JoinResult alive until the upstream
  Finish; if forwarding that downstream Finish fails, relay state is kept for a
  later retry.
  `AddressedJoinNetwork` adds an Experimental registry proof where
  application-supplied opaque addresses are carried in provision tokens,
  resolved through already-live registry entries, or resolved through an
  app-supplied connector for unknown addressed provisions. `JoinNetwork`
  returned buffers are now caller-allocator owned: `hostJoinResult()` and
  `connectJoined()` take the allocator used for returned provision/result
  buffers, while network registry and connector lease internals remain owned by
  the network.
  Receive-side state is now bounded by
  `PeerLimits.max_join_parts_per_join = 64` and the aggregate
  `max_pending_join_records = 4096`, which charges buckets, parts, relays,
  canonical hosted provisions, result answers, and direct Accepts. Each
  completed handoff has one origin-owned `HostedJoin`; result records and a
  distinct Accept host borrow it, and provision bytes are charged once at the
  owner. `PeerStats` and events expose only redacted Join record/part/byte
  counts and inbound answer IDs.
  Raw peers keep Join expiry opt-in. TCP connect/serve and WorkerPool default to
  a 30-second lease, with explicit null as the opt-out. First-part deadlines do
  not extend; relays and hosted Accepts get fresh local-clock deadlines. The
  cached sweep runs at frame/deadline/Accept boundaries and is also available
  as `sweepExpiredJoins()`. `attachJoinNetwork()` and `detachJoinNetwork()`
  reject replacement while dependent state exists; identical reattachment is
  a no-op.
  Regressions cover Finish/send-failure/OOM paths, pending direct-Accept
  rollback, coordinator duplicate-send rejection, post-JoinResult and
  post-Accept-send cancel cleanup, drop-time pending Join/Accept cancellation,
  partial-Finish retry without replaying successful Finishes, terminal
  direct-Accept JoinResult cleanup, malformed/exception JoinResult terminal
  cleanup including mixed retained-result cleanup,
  mismatched successful JoinResult cleanup, synchronous direct-Accept Finish OOM
  retry plus later release-time/transfer drain including `releaseAccepted()`
  partial failure retry and `takeAccepted()` transfer while Finish remains
  retryable, direct Accept peer teardown neutralization, sendPart OOM rollback,
  addressed unknown/stale/duplicate provision handling,
  connector malformed-token/no-dial,
  network-teardown-before-release, and OOM-before-dial handling, retained result
  import release, mismatch/cancel cleanup, callback failure after retention,
  JoinResult fallback-exception send-failure rollback,
  distinct Join-host/Accept-host allocator ownership for promised targets,
  proxy relay success through the real coordinator, source unavailable,
  unsupported source-target rejection, downstream Join send failure, downstream
  results/exception Return relay failure, unexpected downstream Return cleanup,
  downstream Finish retry, owner teardown including downstream Finish send
  failure, source teardown before and after downstream Return, target mismatch
  through relay, and relay setup OOM rollback. `just e2e-l4-zig` runs a
  nine-case Zig↔Zig TCP gate that first proves short-TTL/small-quota attacker
  cleanup and then completes the addressed JoinResult→Accept→call path. There
  is no Stable `Peer.sendJoin`, no production Join addressing policy or bundled
  dialer, no multi-hop relay beyond transparent proxy relay, and no
  cross-implementation L4 interop claim. The C++ L3 e2e lane includes shape
  probes plus a source-backed
  runtime-surface probe; it currently
  confirms that the C++ reference stack exposes no callable generic
  `VatNetwork` Join hook for this TCP harness.
  `just e2e-l3-go` confirms Go has generated Join/twoparty shapes but no
  runtime dispatch for `Message.join`. Experimental; exact-pin only.

## Known limitations (v0.10.0)

Each of these is a defined, non-corrupting behavior — safe to tag with, listed so
you know exactly what you are relying on. None is a leak/UAF/hang against a
cooperating peer.

### Active

- **The frozen snapshot pins signatures, fields and error sets — but not
  `anytype`.** `docs/api-snapshot.txt` records struct fields with their default
  values, union variants, enum ordinals, and concrete error sets (expanded from
  Zig's inferred sets). A *generic* signature is the residual hole: Zig cannot
  resolve an inferred error set for a function whose parameters include
  `anytype` until it is instantiated, so the renderer emits an opaque
  self-referential marker that is identical for every set, and `api-closure`
  skips the signature entirely.

  This was nine Stable signatures; **seven are now closed and two remain**.
  Five are genuinely tightened to a concrete set:
  `codegen.ArrayListWriter.print` (explicit `error{CodegenBudgetExceeded,
  OutOfMemory}` — it keeps `args: anytype`, but an explicit return set renders
  concretely anyway), and `message.MessageBuilder.writeTo` / `writePackedTo`
  plus `rpc.wire.protocol.CapDescriptor.writeReceiverAnswer` /
  `writeThirdPartyHostedNull`, which took concrete parameter types
  (`*std.Io.Writer` and `message.StructBuilder` respectively) and so also
  cleared their `api-closure` skip.

  Two more lost the opaque marker without gaining a tighter pin, and the
  snapshot now says so honestly by rendering `anyerror!void`:
  `rpc.wire.protocol.CapDescriptor.writeThirdPartyHosted` (it calls
  `message.cloneAnyPointer`, which is declared `anyerror!void` because it
  recurses across a type-erased boundary whose helpers are `@ptrCast` to
  `anyerror` signatures) and
  `rpc.caps.table.payload_remap.clonePayloadWithRemappedCaps` (its
  `map_inbound_cap` parameter is an `anyerror`-typed callback). Both resolve to
  `anyerror` at *every* instantiation, so there is nothing to tighten.

  **The generic-parameter holes are otherwise closed.**
  `reader.Reader.readMessage` and `reader.Reader.readPackedMessage` — the last
  two `anytype` signatures, formerly duck-typed over any sequential stream
  reader — took a concrete `*std.Io.Reader` (breaking; `SliceReader`, whose
  only purpose was feeding them, was removed — use
  `std.Io.Reader.fixed(bytes)`). Their error sets now render concretely and an
  error rename turns `check-api` red, verified in both directions. What
  remains is the honest pair above: a signature that resolves to `anyerror` at
  every instantiation cannot drift-detect either, but there is no tighter
  truth to pin.

- **The frozen surface IS now closed under its own signatures**, gated by
  `zig build api-closure` on all three CI tiers. Its first run reported 14
  violations; resolving them promoted the types a consumer cannot avoid —
  `ConnectOptions`, `ServeOptions`, `Export`, and a narrowed `Listener`. Before
  that, `ServerSession.accept` required a `*Listener` no Stable API could
  construct, so the frozen server entry point was unusable on its own terms.

  Two limits on what the check proves. **Closure is not constructibility**: it
  verifies that a type named in a Stable signature is itself frozen, not that a
  Stable *constructor* for it exists — `Listener.init` / `close` / `getAddress`
  needed separate promotion for the entry point to actually be reachable, and a
  future promotion could satisfy the gate while still leaving a type
  unconstructible. And a method that takes or returns its own enclosing type is
  exempt by design, which is what keeps `ServerSession` frozen at `.accept` plus
  its lifecycle without freezing the struct wholesale.


- **Inbound `Call.sendResultsTo = thirdParty` is refused by default.** The
  default `.reject` policy answers with one exception `Return` before dispatch
  instead of accepting a call whose results might be dropped. Both reference
  implementations refuse too (go-capnp echoes `Unimplemented`; the C++ stack
  aborts the connection), so this remains the conservative interop behavior.

  Two Experimental opt-ins exist. `.application` preserves the original manual
  contract: the application delivers the result itself and calls
  `Peer.sendReturnResultsSentElsewhere`; because the peer never sees that
  result, pipelined calls on the redirected answer receive their own exception.
  `.vat_network` instead resolves the contact through an attached
  `VatNetwork`, creates a normal synthetic answer on the introduced peer, and
  automatically forwards results/exceptions and remaps capability descriptors
  there. The synthetic answer supports pipelining and Finish. Its evidence is
  Zig↔Zig only, the supplied network and peers are borrowed and
  single-thread-affine, and the runtime still supplies no production dialer or
  identity/authentication policy. A proxy topology must opt each relevant
  callee-facing capnp-zig hop into the intended policy. The focused automatic
  suite covers send/OOM rollback, reentrant teardown, and transport close on
  both route endpoints.

The forwarded-return intermediary case that shipped as the one remaining active
v0.3.0 limitation is resolved as of v0.6.0. Every limitation listed above is
either a Level-3 surface or a serialization compatibility gap; the frozen
two-party RPC surface has no active limitation. Historical resolved items are listed
below so release-to-release behavior changes stay auditable.

### Resolved since v0.9.0

- **Recursive nested lists now have additive typed Reader/Builder views.** A
  struct or group containing `List(List(T))` exposes `nestedLists()` in both
  codegen profiles. The view recurses to arbitrary list depth, supports the
  same terminal element families as direct generated lists, preserves enum
  ordinal forwarding, pointer defaults, union guards, explicit null inner
  elements, and segment-targeted initialization, and retains `raw()` at every
  recursive level. The pre-existing raw field getters and initializers are
  unchanged, so code using `PointerListReader` / `PointerListBuilder` continues
  to compile.

- **The list-upgrade rule is complete in both directions, `List(Void)`
  included.** On a struct's own fields *and* one level down: a list of any
  element size except one bit decodes as a struct list (so a peer that evolved
  `List(UInt32)` into `List(SomeStruct)` reads old data), and a
  correctly-encoded struct list decodes back as `List(UInt8/16/32/64)`,
  `List(Text)` or a pointer list (so a binary still on the old schema reads
  what the evolved peer writes). The inverse direction is served by one shared
  resolver, so `PointerListReader.getU32List` and friends (nested
  `List(List(UInt32))`) and `AnyPointerReader.getPointerList` (type-erased
  access) accept exactly what `StructReader.read*List` accepts. Element
  preconditions follow the C++ reference on every surface: a primitive list
  needs a non-empty data section, a pointer list needs at least one pointer, a
  struct list is never readable as `List(Bool)`, and Text and Data still
  require byte elements. `U8ListReader.slice` fails with
  `error.InvalidPointer` on such a list — its bytes are one struct apart, so
  no contiguous slice holds them. `readVoidList` closes the last arm: a void
  element needs zero data bits and zero pointers, so — exactly as the C++
  reference (`layout.c++`, both element-size checks are vacuous for
  `ElementSize::VOID`) — every well-formed list satisfies a `List(Void)` read
  at its actual element count, an inline-composite list at its TAG count.
  (Before v0.9.0 it rejected everything but a plain void list with
  `error.InvalidPointer`; an earlier revision of this file misdescribed that
  as misreporting the word count.)

### Resolved since v0.3.0

- **Forwarded-return `takeFromOtherQuestion` / `resultsSentElsewhere` — RESOLVED.**
  When acting as a forwarding intermediary, capnp-zig now translates nested
  `takeFromOtherQuestion` IDs through the forwarded-question map in the
  caller-translation and propagation modes, and preserves `resultsSentElsewhere`
  markers instead of degrading to a clean exception
  (`src/rpc/peer/forward/peer_forwarded_return_logic.zig`). Reachable only in
  proxy topologies; standard two-party client/server calls were already
  unaffected.

### Resolved since v0.2.0

- **Echoed `Unimplemented(Disembargo)` (was #1) — RESOLVED (W2).** An echoed
  `Unimplemented(Disembargo)` was previously silently dropped, leaving the target
  import flagged `embargoed` with a retained `pending_embargoes` entry. It is now
  treated as the protocol violation it is: the peer sends an `Abort` ("peer echoed
  Disembargo as Unimplemented") and returns `error.EchoedDisembargoUnimplemented`,
  which tears the connection down (`src/rpc/peer/bootstrap.zig`,
  `src/rpc/peer/errors.zig`). No stuck embargo state can accumulate.

- **`hasKnownDisembargoTarget` over-accepts (was #2) — RESOLVED (W3).** The
  `importedCap` arm previously accepted an export *or* import id; it now validates
  against the export table only, the exact id space the spec names on the
  `senderLoopback` path (`src/rpc/peer/mod.zig`, `hasKnownDisembargoTarget`).

- **Reflected-loopback return interop (was #4) — RESOLVED (W1).** capnp-zig
  previously returned the caller's original question with an eager
  `takeFromOtherQuestion` redirect (forwarding the relayed call with
  `sendResultsTo=caller`); kj-capnp (C++) and pycapnp consumed it, but go-capnp
  could not parse `takeFromOtherQuestion` and capnp-rpc (Rust) stalled on it. It
  now relays the reflected call as a plain `sendResultsTo=caller` call and
  translates the real results straight back onto the caller's pipelined question
  as a plain `.results` return (internal `.translate_to_caller` forward mode,
  `src/rpc/peer/forward/peer_forward_orchestration.zig`) — a shape **all four**
  reference clients consume. The `resolve_disembargo` go/rust client skips were
  removed; the only remaining skip is `zig-client → python-server` (pycapnp cannot
  host the reflecting server). A former side effect — a *direct* (non-deferred)
  generated handler on a reflected-to cap not running its body on the
  `resultsSentElsewhere` path — no longer applies on the reflected loopback path,
  since results now flow as plain `.results`.
