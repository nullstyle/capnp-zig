# Production Parity Checklist

Updated: 2026-02-06
Owner: `capnp-zig` / `capnpc-zig`

## Goal
Ship a production-ready Zig Cap'n Proto library and compiler plugin with spec-level parity against the reference implementation, plus enforced cross-language interop.

## Current Status
- Wire-format/message core is in strong shape and heavily tested.
- RPC core (bootstrap/call/return/finish/release/resolve/disembargo) is implemented and tested.
- Unsupported/unknown inbound RPC messages now return `Unimplemented` instead of failing locally.
- Baseline `provide`/`accept`/`join` lifecycle support is in place (store provision, accept by recipient key, clear on `Finish`) and now has dedicated runtime tests.
- Core parity checklist items are complete in-repo, including interop hard-gating and codegen parity paths.
- Remaining work is ongoing production hardening (broader external e2e matrix, soak/perf tuning, API polish).

## Definition Of Done
- No known spec-required feature is marked unsupported in primary runtime/codegen paths.
- `zig build test`, `zig build test-rpc`, and `zig build test-interop-rpc` pass in CI without skips.
- Interop passes against Go and Cap'n Proto CLI for representative schemas and RPC flows.
- Public APIs and failure modes are documented and stable.

## P0: RPC Spec Completeness
- [x] Implement `sendResultsTo.yourself` semantics end-to-end.
- [x] Implement `sendResultsTo.thirdParty` and `Return.acceptFromThirdParty` flow.
- [x] Implement `CapDescriptor.thirdPartyHosted` and `CapDescriptor.receiverAnswer` end-to-end.
- [x] Replace `Unimplemented` fallback for `provide`, `accept`, `join`, and `thirdPartyAnswer` with explicit handlers.
- [x] Implement baseline `provide`/`accept`/`join` lifecycle semantics (provision storage, accept lookup, finish cleanup).
- [x] Implement baseline level-3 `Accept.embargo` + `Disembargo.context.accept` queue/release semantics.
- [x] Enforce full embargo ordering semantics for pipelined calls targeting embargoed `Accept` results.
- [x] Implement full level-3 `thirdPartyAnswer` adoption flow.
- [x] Implement full level-4 join-key aggregation/verification semantics.
- [x] Add protocol conformance tests for every `MessageTag` and every `sendResultsTo` mode.

## P0: Interop As A Hard Gate
- [x] Make `tests/interop_rpc_test.zig` non-skippable in CI by provisioning `capnp`, Go, and `CAPNP_GO_STD`.
- [x] Add CI job that runs `zig build test-interop-rpc --summary all`.
- [x] Add interop cases covering promise resolution races, third-party handoff compatibility, and cancellation/finish edge cases.
- [x] Add C++ reference-server backend path in interop CI (`CAPNP_INTEROP_BACKEND=cpp`) for core bootstrap/call/exception/follow-up flows.

## P1: Codegen Parity
- [x] Replace current TODO/stub type mappings in `src/capnpc-zig/types.zig` and `src/capnpc-zig/generator.zig`.
- [x] Generate real accessors/builders for text/data/complex pointer types.
- [x] Generate robust enum/list/struct/interface handling with defaults and annotations.
- [x] Expand codegen tests for nested interfaces, complex constants, and edge schemas.

## P1: Correctness & Hardening
- [x] Add malformed-frame/fuzz tests for RPC/message decoding.
- [x] Add resource-limit tests (segment count, nesting, traversal limits) to prevent DoS regressions.
- [x] Add stress tests for promise/disembargo ordering and forwarded-tail cleanup.

## P2: Production Cleanup
- [x] Document API contracts and error taxonomy.
- [x] Add benchmark baselines and regression thresholds (packed/unpacked + RPC ping-pong).
- [x] Review memory lifecycle and failure cleanup paths under high concurrency.

## Immediate Execution Order
1. (Done) `sendResultsTo.thirdParty` + `acceptFromThirdParty` support.
2. (Done) `thirdPartyHosted`/`receiverAnswer` capability descriptor support.
3. (Done) Replace generic `Unimplemented` fallback for `provide`/`accept`/`join`/`thirdPartyAnswer`.
4. (Done) Full level-4 semantics for `join` (join-key aggregation/verification).
5. (Done) CI-enforced interop (remove skips).

## Next Hardening Focus
- Expand containerized cross-language e2e matrix beyond current Go path (C++ reference server/client flows).
- Add long-haul/chaos RPC tests (disconnect, half-close, delayed resolve/disembargo ordering under load).
- Lock API stability commitments and migration notes for public runtime/codegen entry points.

## Progress Log
- 2026-02-06: Fixed top-level RPC message tag ordinals to match `rpc.capnp`.
- 2026-02-06: Added `Abort` and `Unimplemented` parsing/building and peer handling.
- 2026-02-06: Added promised-answer sender-promise queue/replay behavior for unresolved promise exports.
- 2026-02-06: Added outbound `Unimplemented` replies for unsupported/unknown inbound RPC messages.
- 2026-02-06: Implemented `sendResultsTo.yourself` runtime behavior for local exports and forwarded calls; added coverage for forwarded imported-cap path and local return conversion to `resultsSentElsewhere`.
- 2026-02-06: Implemented baseline `sendResultsTo.thirdParty`/`acceptFromThirdParty` plumbing: local successful returns now map to `acceptFromThirdParty`, forwarded third-party calls are no longer rejected, and third-party destination pointers are cloned when forwarding.
- 2026-02-06: Completed `sendResultsTo.thirdParty`/`acceptFromThirdParty` payload propagation: third-party pointers are retained and replayed across local returns and forwarded flows, mapped state is cleaned up on `Finish`, and protocol/runtime tests now assert payload round-tripping.
- 2026-02-06: Completed `CapDescriptor.thirdPartyHosted` + `receiverAnswer` support in protocol/cap-table/peer forwarding paths, including third-party vine fallback and promised-cap forwarding as `receiverAnswer`.
- 2026-02-06: Added explicit handlers for `provide`, `accept`, `join`, and `thirdPartyAnswer` (exception/abort semantics) and protocol decode/build coverage for all four message types.
- 2026-02-06: Implemented baseline `provide`/`accept`/`join` runtime semantics and wired dedicated `tests/rpc_peer_test.zig` into `zig build test-rpc` to validate provision success, duplicate recipient abort, finish cleanup, join return, and current `thirdPartyAnswer` abort behavior.
- 2026-02-06: Implemented embargoed `Accept` deferral/release via `Disembargo.context.accept` plus finish-cancel cleanup, added protocol builder coverage for disembargo-accept, and expanded RPC peer tests for embargo release/cancel paths.
- 2026-02-06: Added pipelined-call ordering coverage for embargoed `Accept` answers, proving calls targeting the pending promised answer stay blocked until `Disembargo.context.accept` and execute only after the `Accept` return is emitted.
- 2026-02-06: Implemented full level-3 `thirdPartyAnswer` adoption semantics, including completion-key correlation, callee answer-id adoption, early-return buffering/replay, and callback answer-id translation back to the original question; added runtime tests for both await-first and answer-first races.
- 2026-02-06: Implemented level-4 join-key aggregation/verification using two-party `JoinKeyPart` semantics (`joinId`/`partCount`/`partNum`), including deferred multi-part completion, mismatch detection, and finish-cancel cleanup; added runtime tests for multi-part success and mismatch.
- 2026-02-06: Added protocol conformance coverage for all `MessageTag` discriminants and all `sendResultsTo` modes (`caller`, `yourself`, `third_party`).
- 2026-02-06: Replaced stub codegen type mappings in `src/capnpc-zig/types.zig`/`src/capnpc-zig/generator.zig` with concrete runtime reader types, added `List(Void)` reader/builder generation, and generated pointer-field helper builders (`clear*`, `set*Capability`, `set*Text`, `set*Data`, `set*Null`) with coverage in `tests/codegen_test.zig`.
- 2026-02-06: Fixed generated struct setter default encoding to match Cap'n Proto xor semantics for bool/numeric/float/enum fields, including fallback `u16` handling for unresolved enum types; added regression assertions in `tests/codegen_defaults_test.zig` and `tests/codegen_test.zig`.
- 2026-02-06: Tightened interface field generation to use `message.Capability` readers and `readCapability()` accessors instead of raw any-pointer reads, with updated codegen mapping/tests to keep generated APIs capability-typed.
- 2026-02-06: Fixed unresolved-struct field getter fallback to emit `readStruct()`/`message.StructReader` rather than generating `UnsupportedType` code paths; added regression coverage in `tests/codegen_test.zig`.
- 2026-02-06: Added typed enum-list wrappers in generated struct APIs (`EnumListReader`/`EnumListBuilder`) so enum list fields expose enum get/set operations instead of raw `u16`; added codegen assertions for wrapper emission and usage.
- 2026-02-06: Added compile-and-run coverage for generated enum-list code via `tests/codegen_generated_runtime_test.zig` (generates Zig from schema, compiles with `zig test`, and executes runtime read/write assertions), and fixed emitted enum-list getter control flow accordingly.
- 2026-02-06: Added compile-and-run coverage for generated defaulted primitive/enum setters using `tests/test_schemas/default_setter_runtime.capnp`, validating round-trip correctness for default and non-default values (bool/int/float/enum) through generated builders/readers.
- 2026-02-06: Added raw wire-data assertions to generated default-setter runtime coverage, proving default-valued writes encode as all-zero scalar storage and non-default writes produce non-zero encoded bytes.
- 2026-02-06: Added typed struct-list wrappers in generated struct APIs (`StructListReader`/`StructListBuilder`) and validated output via dedicated codegen assertions.
- 2026-02-06: Added typed pointer-list wrappers for `List(Data)` and `List(Interface)` (`DataListReader`/`DataListBuilder`, `CapabilityListReader`/`CapabilityListBuilder`) so generated list APIs expose concrete get/set operations instead of raw pointer-list plumbing.
- 2026-02-06: Added compile-and-run coverage for generated list wrappers via `tests/test_schemas/list_wrappers_runtime.capnp`, including runtime assertions for struct-list values, data list payloads, and capability list IDs.
- 2026-02-06: Fixed interface codegen to compile for zero-method interfaces by explicitly consuming `server`/`caps` in generated `onCall`, preventing unused-symbol build failures in runtime harnesses.
- 2026-02-06: Added nested-interface codegen coverage using `tests/test_schemas/nested_interfaces.capnp`, asserting recursive generation of outer/inner interface APIs and method scaffolding.
- 2026-02-06: Added compile-and-run coverage for complex constants from `tests/test_schemas/defaults.capnp`, validating primitive/data/enum constants plus pointer constants (`magicList.get()`, `magicInner.get()`).
- 2026-02-06: Added edge-schema compile-and-run coverage via `tests/test_schemas/edge_codegen.capnp` (zero-method interface + empty text/list constants), validating generated API correctness for minimal-interface schemas.
- 2026-02-06: Hardened stream/frame decode arithmetic in `src/rpc/framing.zig` and message framing decode in `src/message.zig` to use checked math, returning decode errors on malformed overflowed headers instead of risking arithmetic traps.
- 2026-02-06: Added malformed/fuzz decode coverage for framing, message, and RPC protocol decode paths (`tests/rpc_framing_test.zig`, `tests/message_test.zig`, `tests/rpc_protocol_test.zig`) with deterministic random-input loops and malformed-header assertions.
- 2026-02-06: Fixed `protocol.DecodedMessage.init` to clean up temporary decoded messages on invalid tags, eliminating a leak surfaced by new malformed-frame fuzz coverage.
- 2026-02-06: Added segment-count resource limits in message decode/validate (`Message.max_segment_count`, `ValidationOptions.segment_count_limit`) and added explicit segment-limit regression tests in `tests/message_test.zig`.
- 2026-02-06: Added traversal and nesting boundary-condition tests in `tests/message_test.zig` to lock in limit semantics (`exact-limit passes`, `below-limit fails`) for DoS hardening.
- 2026-02-06: Added looped RPC stress coverage in `src/rpc/peer.zig` for embargoed `Accept` + promised-call release ordering and repeated forwarded-tail finish/return races, with explicit state-cleanup assertions for pending embargo/promise and tail-forward maps.
- 2026-02-06: Hardened `tests/interop_rpc_test.zig` prerequisites to require `capnp`/Go/`CAPNP_GO_STD` (with vendored `vendor/ext/go-capnp/std` fallback) instead of skipping, and added interop cases for loopPromise race ordering, third-party `sendResultsTo` compatibility (`Unimplemented` fallback from current Go runtime), exception-path follow-up call health, and early-`Finish` follow-up-call resilience.
- 2026-02-06: Added CI hard gate workflow `.github/workflows/interop-rpc.yml` that provisions `capnp` + Go, sets `CAPNP_GO_STD`, and runs `zig build test-interop-rpc --summary all`.
- 2026-02-06: Added machine-readable benchmark output (`--json`) for ping-pong and packed/unpacked benches, added `tools/bench_check.zig`, and wired `zig build bench-check` with committed baselines in `bench/baselines.json`.
- 2026-02-06: Tightened benchmark regression threshold from 45% to 30% and added CI hard gate workflow `.github/workflows/bench-regression.yml` to enforce `zig build bench-check` on PRs and `main`.
- 2026-02-06: Added API/lifecycle/error-contract documentation in `docs/api_contracts.md` covering ownership, thread-affinity, and caller error policy by category.
- 2026-02-06: Added additional high-load RPC cleanup coverage in `src/rpc/peer.zig` for repeated third-party await/answer races and for deinit cleanup of unresolved embargoed-accept + promised-call queues.
- 2026-02-06: Added runtime reliability coverage for `Connection.handleRead` fragmented/coalesced/error/malformed-frame behavior and for `Transport.signalClose` idempotency plus write-completion callback/untracking.
- 2026-02-06: Added interop backend selection (`go`/`cpp`) to `tests/interop_rpc_test.zig`, introduced a C++ reference `Arith` server harness (`tests/interop_rpc/cpp/arith_server.cpp`), and updated `.github/workflows/interop-rpc.yml` to run a backend matrix with `libcapnp-dev` installed.
