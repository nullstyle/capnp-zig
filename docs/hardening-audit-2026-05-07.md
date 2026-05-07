# Hardening Audit - 2026-05-07

This document records the fresh May 7, 2026 hardening audit for capnp-zig.
It applies the external HTTP/3/QUIC hardening guide at
`/Users/nullstyle/prj/ai-workspace/hardening-guide.md` to capnp-zig's current
Cap'n Proto serialization, RPC, QUIC, WASM, and codegen surfaces.

The guide's main transferable principles are:

- Treat every frame, schema, pointer, capability ID, and host ABI pointer as hostile input.
- Validate all protocol state before mutating peer state, maps, queues, or builders.
- Enforce explicit count and byte budgets before allocation, copying, buffering, or iteration.
- Avoid `panic`, `unreachable`, unchecked arithmetic, and raw pointer parsing on input-facing paths.
- Use production-safe error disclosure and logging defaults.
- Add fuzzing, OOM/failing-allocator coverage, ReleaseSafe tests, and unsafe-pattern gates.

## First Tranche

These items are the recommended first implementation tranche because they cover
the most exposed RPC and transport paths:

- [x] Validate RPC frames before exposing `DecodedMessage`.
- [x] Reject malformed non-null protocol pointers instead of collapsing them to null.
- [x] Validate inbound `receiverHosted` capability IDs against live exports.
- [x] Cap promised-answer transform length during parse/resolve.
- [ ] Add peer and host bridge count/byte budgets for pending input-driven state.
- [x] Reject duplicate active inbound question IDs.
- [x] Reject over-release before mutating export tables.
- [ ] Fix failure atomicity in Return, third-party handoff, resolve/embargo, and return-routing paths.
- [ ] Enforce framer and send-queue byte budgets before append/copy allocation.
- [ ] Expose QUIC production hardening controls from `ServerOptions`.
- [ ] Add focused regression tests and the first hardening gates.

Progress in this tranche:

- RPC frames now use validated message decoding, and message validation accepts well-formed capability pointers while rejecting invalid capability pointer encodings.
- Optional RPC struct-list fields now distinguish true null/missing fields from malformed non-null pointers in promised-answer transforms and payload cap tables.
- `receiverHosted` cap-table descriptors now require a live local export ID.
- Promised-answer transforms are capped at decode and resolve time.
- Peer Release over-counts now return `error.ReleaseCountExceeded` before mutating export state.
- Peers now track active inbound question IDs and reject duplicate calls while the first call is still unresolved.
- TCP and QUIC outbound queues now check configured queue bounds before copying frames into owned queue allocations.
- The cumulative RPC level3 and serialization message suites pass with the new regressions.

## Findings

### RPC Protocol And Capability Parsing

**High: RPC ingress skips message validation**

- Location: `src/rpc/level0/protocol.zig:284`
- Trigger: hostile frame with valid top-level tag but excessive, cyclic, or deep payload pointers.
- Impact: no RPC-aware traversal/nesting validation before handlers mutate peer/capability state.
- Fix: add cap-pointer-aware validation before exposing `DecodedMessage`.
- Tests: traversal-limit and nesting-limit frames must be rejected before `asCall` or cap-table handling.

**High: inbound `receiverHosted` IDs are trusted without export lookup**

- Location: `src/rpc/level0/cap_table.zig:307`
- Trigger: peer sends a cap table entry naming an arbitrary local export ID.
- Impact: forged or stale local capability reference can enter `ResolvedCap.exported`.
- Fix: require live export or promise-export membership before accepting the ID.
- Tests: unknown, released, and over-released `receiverHosted` IDs.

**Medium: malformed list pointers become absent**

- Location: `src/rpc/level0/protocol.zig:637`, `src/rpc/level0/protocol.zig:752`
- Trigger: non-null malformed promised-answer transform or payload cap-table pointer.
- Impact: malformed protocol data is interpreted as "no transform" or "no caps".
- Fix: only treat true null as absent; propagate malformed pointer errors.
- Tests: wrong-type and out-of-bounds list pointers in transforms and payload cap tables.

**Medium: promised-answer transforms have no resolve-time cap**

- Location: `src/rpc/common/promise_pipeline.zig:67`
- Trigger: peer supplies a huge promised-answer transform list.
- Impact: CPU amplification up to frame-size limits.
- Fix: enforce the same transform count limit during direct parse/resolve as clone paths.
- Tests: a transform list above the configured cap must fail with a protocol error.

**Low: protocol builders still use `catch unreachable`**

- Location: `src/rpc/level0/protocol.zig:92` and similar generated builder wrappers.
- Trigger: generated setter contract changes or invalid builder state on encode paths.
- Impact: protocol-adjacent panic/undefined assumption remains in hardened surface.
- Fix: propagate typed errors from helpers or narrow and document an audited allowlist.
- Tests: add an unsafe-pattern gate for network/protocol paths.

### Peer State Machine

**High: over-release deletes live exports**

- Location: `src/rpc/level3/peer/peer_cap_lifecycle.zig:111`
- Trigger: peer sends `Release(id, count)` where `count > ref_count`.
- Impact: remote can prematurely remove local exports, promise exports, and queued calls.
- Fix: reject over-release as a protocol error before mutating export tables.
- Tests: count overflow and over-release regression coverage.

**High: duplicate inbound caller questions can run twice**

- Location: `src/rpc/level3/peer.zig:2024`
- Trigger: duplicate `Call.question_id` while the first normal caller call is in flight.
- Impact: duplicate handler execution and conflicting returns.
- Fix: track all active inbound answer IDs until terminal Return/Finish handling.
- Tests: duplicate normal calls must be rejected before invoking the target twice.

**High: Return removes question before validation/dispatch succeeds**

- Location: `src/rpc/level3/peer/return/peer_return_orchestration.zig:167`
- Trigger: `Return` for a live question followed by OOM, invalid cap table, or callback failure.
- Impact: question state is lost and shutdown/finish semantics can be wrong.
- Fix: validate inbound caps first and remove only after commit, or restore on failure.
- Tests: injected OOM and malformed cap-table failures keep state consistent.

**High: third-party handoff drops pending state before adoption commits**

- Location: `src/rpc/level3/peer/third_party/peer_third_party_pending.zig:13`
- Trigger: matching `ThirdPartyAnswer` or `awaitFromThirdParty`, then duplicate adopted answer ID or OOM.
- Impact: pending await/answer is removed and the callback path is orphaned.
- Fix: prevalidate and reserve capacity, then remove pending state only after adoption succeeds.
- Tests: duplicate and OOM adoption failures preserve pending state.

**Medium: resolve/embargo state leaks on partial failure**

- Location: `src/rpc/level3/peer/peer_control.zig:327`
- Trigger: `Resolve` to exported/promised cap followed by disembargo send or resolved-import store failure.
- Impact: stale pending embargoes, blocked imports, or embargo ID exhaustion.
- Fix: use staged state and rollback on failure.
- Tests: injected send/OOM failures leave no pending embargo leak.

**Medium: resolved import overwrite is not atomic**

- Location: `src/rpc/level3/peer/peer_cap_lifecycle.zig:139`
- Trigger: duplicate or late `Resolve` plus OOM during map update.
- Impact: old resolution can be removed before the new one is stored.
- Fix: reject duplicate resolve or use reserve/update with rollback.
- Tests: duplicate resolve and OOM regression coverage.

**Medium: return routing is cleared before send succeeds**

- Location: `src/rpc/level3/peer/return/peer_return_dispatch.zig:217`
- Trigger: answering `sendResultsTo.yourself` or `thirdParty`, then send fails.
- Impact: retry can send the wrong return shape or lose third-party routing.
- Fix: clear routing after send success, or restore on failure.
- Tests: send failure preserves routing state.

**Medium: embargoed Accept can overwrite question mapping**

- Location: `src/rpc/level3/peer/peer_embargo_accepts.zig:20`
- Trigger: duplicate embargoed `Accept.question_id`.
- Impact: old list entries become orphaned or duplicate returns are emitted.
- Fix: reject duplicate accept answer IDs before appending or inserting.
- Tests: duplicate accepts fail before state mutation.

**Medium: peer pending state lacks explicit resource budgets**

- Location: `src/rpc/level3/peer.zig:1355`,
  `src/rpc/level3/peer/third_party/peer_third_party_returns.zig:29`
- Trigger: floods of unresolved promised calls, third-party returns, provides, joins, or large payloads.
- Impact: unbounded memory and CPU growth.
- Fix: add `PeerLimits` for pending counts, pending bytes, captured payload bytes, joins, embargo buckets, and abort reason length.
- Tests: budget exhaustion must return controlled protocol/load errors without unbounded allocation.

**Low: remote error text is copied/logged and local error names are sent back**

- Location: `src/rpc/level3/peer/peer_control.zig:63`, `src/rpc/level3/peer.zig:1678`
- Trigger: large or hostile `Abort.reason`, malformed messages, or local error propagation.
- Impact: memory/log injection and implementation fingerprinting.
- Fix: cap and sanitize stored reasons; use generic external errors in production.
- Tests: oversized abort reasons are capped and malformed peer errors do not expose internal error names.

### Serialization And Schema Parsing

**High: zero-word inline-composite lists bypass resource budgets**

- Location: `src/serialization/message.zig:1090`,
  `src/serialization/request_reader.zig:107`,
  `src/serialization/message/clone_any_pointer.zig:115`,
  `src/serialization/schema_validation.zig:755`
- Trigger: tiny inline-composite list with huge `element_count`, `data_words = 0`, `pointer_words = 0`, `word_count = 0`.
- Impact: validation consumes zero traversal words, then parsing can allocate from attacker count or clone/canonicalize can loop over huge element counts.
- Fix: add element-count budgets independent of word budgets; reject zero-width struct lists in schema-specific contexts where they cannot be valid.
- Tests: huge zero-width struct lists through parse, clone, and canonicalization.

**Medium: malformed schema pointers and values normalize to defaults**

- Location: `src/serialization/request_reader.zig:101`,
  `src/serialization/request_reader.zig:630`,
  `src/serialization/request_reader.zig:609`
- Trigger: non-null wrong-type list pointer, out-of-bounds data default pointer, or invalid `Value.which`.
- Impact: malformed compiler requests can be accepted as empty lists, empty data, `.void`, or missing defaults.
- Fix: distinguish null from malformed before defaulting.
- Tests: wrong-type nodes, malformed default data, and invalid annotation values.

**Medium: schema/request text parsing bypasses strict Text validation**

- Location: `src/serialization/request_reader.zig:661`,
  `src/serialization/request_reader.zig:625`,
  `src/serialization/message.zig:1632`,
  `src/serialization/schema_validation.zig:396`
- Trigger: Text pointer missing trailing NUL or containing invalid UTF-8 in schema names/defaults.
- Impact: malformed Cap'n Proto Text is accepted by codegen/schema paths despite strict APIs existing.
- Fix: use strict text readers in request and schema-constrained paths.
- Tests: invalid UTF-8 and missing-NUL display names, field names, and default text.

**Low/Medium: unpacked `Message.init` does not enforce total framed word budget**

- Location: `src/serialization/message.zig:500`, `src/serialization/message.zig:870`
- Trigger: valid small root plus very large unreferenced segment in a borrowed-slice message.
- Impact: direct message init can accept oversized messages if traversal stays small.
- Fix: accumulate segment sizes during init and reject over a total message limit.
- Tests: oversized unreferenced segment is rejected.

**Low: parser warnings expose detailed malformed-input internals**

- Location: `src/serialization/message.zig:461`,
  `src/serialization/message.zig:626`,
  `src/serialization/message.zig:973`
- Trigger: malformed segment headers, far pointers, or bounds failures.
- Impact: log amplification and fingerprinting risk.
- Fix: keep parse/validation silent by default; gate detailed diagnostics behind opt-in debug tracing.

### Transport And QUIC

**High: QUIC hardening controls are not exposed**

- Location: `src/rpc/level2/quic_transport.zig:446`
- Trigger: public UDP server config only passes cert/key/ALPN/transport params/max connections to nullq.
- Impact: capnp-zig cannot configure Retry, listener/source rate limits, token keys, per-connection memory caps, or logging rate limits.
- Fix: add `ServerOptions` fields or a production preset and thread them into `nullq.Server.Config`.
- Tests: config propagation and production preset tests.

**High: host bridge queues inbound calls without limits**

- Location: `src/rpc/integration/host_peer.zig:225`
- Trigger: peer sends many bridged calls while the host does not drain `host_calls`.
- Impact: each call copies the full frame and grows host queues without count or byte budgets.
- Fix: add inbound host-call count and byte limits enforced before allocation.
- Tests: host-call flood and duplicate question ID coverage.

**Medium: send queue budgets are checked after allocating**

- Location: `src/rpc/level2/transport.zig:227`,
  `src/rpc/level2/quic_transport.zig:579`
- Trigger: queue is already full, then a large frame is sent.
- Impact: large allocation/copy happens before the budget rejection.
- Fix: preflight/reserve queue capacity before copying.
- Tests: failing-allocator tests prove budget rejection does not allocate.

**Medium: TCP queue undercounts writer-owned bytes**

- Location: `src/rpc/level2/transport.zig:111`
- Trigger: writer takes a large batch and blocks while producers enqueue another full queue.
- Impact: resident outbound bytes can exceed `max_queued_bytes`.
- Fix: track in-flight writer bytes until freed or release queue budget only after write/free.
- Tests: blocked-writer budget test.

**Medium: TCP terminal frame errors invoke callbacks before closing**

- Location: `src/rpc/level2/connection.zig:330`
- Trigger: malformed frame invokes `on_error`, then shutdown happens after `handleRead` returns.
- Impact: callbacks see the transport open and can enqueue writes on a corrupted stream.
- Fix: mark closing/shutdown before invoking terminal error callbacks.
- Tests: malformed-frame callback sees closing state.

**Medium: inbound OOM is retried indefinitely**

- Location: `src/rpc/level2/connection.zig:331`, `src/rpc/level2/quic_transport.zig:724`
- Trigger: peer sends a max-legal frame and allocator fails while copying it out.
- Impact: buffered peer data remains and the connection can retry/log OOM repeatedly.
- Fix: close with excessive-load/connection error after peer-driven OOM or use a small retry budget.
- Tests: failing allocator on receive path.

**Medium: close idempotency has fd races**

- Location: `src/rpc/level2/runtime.zig:99`, `src/rpc/level2/transport.zig:281`
- Trigger: concurrent close, shutdown, and deinit paths.
- Impact: load-then-store guards can double-close or shutdown a reused fd.
- Fix: compare-exchange guard or locked fd lifecycle enum.
- Tests: concurrent close/deinit stress test.

**Medium: panic-based cleanup guard is callback-reachable**

- Location: `src/rpc/level2/connection.zig:149`, `src/rpc/level2/quic_transport.zig:514`
- Trigger: malformed input invokes `on_error`; callback tries to deinit owner.
- Impact: process panic instead of typed lifecycle error or deferred cleanup.
- Fix: provide non-panicking deferred close/deinit path.
- Tests: callback cleanup attempts do not panic.

**Medium: StreamState has no in-flight budget and unchecked increment**

- Location: `src/rpc/level2/stream_state.zig:15`
- Trigger: generated streaming client issues unbounded fire-and-forget calls.
- Impact: `u32` overflow can panic/wrap and drain semantics become unreliable.
- Fix: make `noteCallSent` return an error and enforce `max_in_flight`.
- Tests: excessive in-flight calls return a typed error.

**Low: host bridge forwards detailed exception text**

- Location: `src/rpc/integration/host_peer.zig:147`
- Trigger: host responds with internal error strings or prebuilt Return frames.
- Impact: peers can receive implementation-specific diagnostics.
- Fix: add production error policy with generic default reasons, length caps, and debug opt-in.

### WASM, Codegen, And Build Policy

**High: WASM host ABI raw pointers can trap or segfault**

- Location: `src/wasm/capnp_host_abi.zig:236`,
  `src/wasm/capnp_host_abi.zig:247`,
  `src/wasm/capnp_host_abi.zig:421`
- Trigger: host passes nonzero invalid input or output pointers.
- Impact: `@ptrFromInt` slices/stores can trap the WASM instance or segfault native tests.
- Fix: validate IO pointers against tracked ABI allocations and replace `catch unreachable`.
- Tests: invalid pointer, edge pointer, and wrong-length ABI tests.

**High: WASM example serde uses unvalidated frames and unbounded JSON**

- Location: `src/wasm/capnp_host_abi.zig:1149`, `src/wasm/capnp_host_abi.zig:1216`
- Trigger: malformed or oversized frame/JSON passed to example conversion APIs.
- Impact: malformed pointer graphs or large fields can consume CPU/memory before controlled ABI error.
- Fix: validate frames, cap traversal/nesting/frame/text/JSON lengths.
- Tests: deeply nested frames and huge strings.

**High: codegen can expand large request input into much larger output**

- Location: `src/main.zig:238`,
  `src/capnpc-zig/generator.zig:162`,
  `src/capnpc-zig/struct_gen.zig:1065`
- Trigger: large but under-64MiB `CodeGeneratorRequest` with many nodes, long names, annotations, or defaults.
- Impact: generated output can exhaust memory.
- Fix: add `CodegenBudget` and a budgeted writer for nodes, imports, fields, name bytes, default bytes, manifest bytes, and output bytes.
- Tests: large defaults and many declarations.

**Medium: distinct schema names can normalize to the same Zig symbol**

- Location: `src/capnpc-zig/types.zig:39`, `src/capnpc-zig/generator.zig:386`
- Trigger: separator-only names, `$`/`_` variants, or duplicate nested simple names.
- Impact: duplicate declarations or ambiguous generated APIs.
- Fix: track generated identifiers per scope and fail with `error.DuplicateGeneratedName`.
- Tests: field, method, enum, type, and nested-type collisions.

**Medium: ReleaseFast is allowed for public/install/WASM/RPC examples**

- Location: `build.zig:26`, `build.zig:85`, `build.zig:112`
- Trigger: `zig build -Doptimize=ReleaseFast`.
- Impact: runtime safety can be disabled around untrusted parsing, pointer casts, arithmetic, and `unreachable` paths.
- Fix: default public/install/WASM/RPC targets to ReleaseSafe or require explicit unsafe opt-in.
- Tests: CI build-policy tests.

**Medium: output path symlink escape**

- Location: `src/main.zig:283`
- Trigger: output path is lexically safe but an existing parent component is a symlink outside the output tree.
- Impact: codegen can write generated `.zig` files outside the intended output root.
- Fix: create/open output through a no-follow component walk under the output root.
- Tests: symlinked parent escape.

**Medium: WASM/HostPeer defaults are unlimited**

- Location: `src/wasm/capnp_host_abi.zig:112`,
  `src/wasm/capnp_host_abi.zig:179`,
  `src/wasm/capnp_host_abi.zig:311`,
  `src/rpc/integration/host_peer.zig:11`
- Trigger: repeated allocation, peer creation, or outbound generation before callers set limits.
- Impact: no max peers, outstanding allocation bytes, count, or host peer limits by default.
- Fix: conservative defaults with explicit opt-up.
- Tests: loops until caps return controlled errors.

**Low/Medium: external errors leak specific details**

- Location: `src/capnpc-zig/generator.zig:821`,
  `src/capnpc-zig/generator.zig:852`,
  `src/wasm/capnp_host_abi.zig:522`
- Trigger: remote/generated RPC errors or WASM ABI errors.
- Impact: hosts/peers receive implementation-specific messages.
- Fix: stable coarse external codes/messages, detailed diagnostics behind debug telemetry.

**Low: kvstore example defaults are public and chatty**

- Location: `examples/kvstore/server.zig:821`,
  `examples/kvstore/server.zig:1006`,
  `examples/kvstore/server.zig:1363`
- Trigger: running the KV example with default options.
- Impact: binds to all interfaces, logs user keys/prefixes/internal paths/state, and lacks app-level quotas.
- Fix: bind localhost and quiet/redacted logs by default; cap keys, values, ops, watches, and pending notifications.

### Test And Release Gates

**Critical: no real fuzz target suite**

- Current state: deterministic fuzz-like tests are wired as normal unit tests.
- Fix: add fuzz/smoke targets for message decode, packed decode, RPC framer, RPC protocol, QUIC length framer, and peer state.

**High: no network-input panic-ban gate**

- Current state: no CI/Justfile check blocks new `catch unreachable`, `.?`, `@panic`, or unchecked assert patterns in network-input paths.
- Fix: add an allowlisted hardening scan command for `src/serialization`, `src/rpc`, and `src/wasm`.

**High: resource-budget and OOM tests are selective**

- Current state: good coverage exists in places, but not across message init, packed expansion, request parsing/codegen input, protocol decode, connection read, or QUIC receive.
- Fix: add `test-resource-budgets` and `test-oom` build steps.

**High: error disclosure policy is not tested**

- Current state: docs/tests assert detailed abort and exception reasons in some places.
- Fix: add production-mode tests for generic abort/exception reasons and log redaction.

**Medium: ReleaseSafe policy is partial**

- Current state: `just release` uses ReleaseSafe, but tests/e2e generally use default optimize.
- Fix: add ReleaseSafe test/e2e commands and CI jobs.

**Medium: interop security gate is missing**

- Current state: e2e interop is mostly functional/happy-path.
- Fix: add malformed/resource e2e cases with raw frame clients.

**Medium: no public-advisory regression matrix**

- Fix: add `docs/security-regression-matrix.md` and map relevant Cap'n Proto/RPC/QUIC vulnerability classes to tests.

**Medium: no disclosure scan gate**

- Fix: add scans for banners, build IDs, source paths, stack traces, and verbose close reasons.
