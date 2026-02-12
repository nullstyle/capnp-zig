# Implementation Plan

Updated: 2026-02-11

## Phase 1: Schema Parsing and Code Generation Entrypoint -- Done

Parsed `CodeGeneratorRequest` messages from the Cap'n Proto compiler plugin
protocol (stdin wire format) and wired the result into the code generation
entrypoint. This established the pipeline from `.capnp` schema files through
the `capnp` compiler into Zig-native schema structures.

**What was built:**
- Wire-format message layer: segment management, pointer encoding/decoding,
  struct/list/text/data serialization, packing, far pointers.
- `CodeGeneratorRequest` parser that reads nodes, fields, types, and basic
  values from the binary plugin protocol.
- Code generation driver that emits idiomatic Zig Reader/Builder types from
  schema nodes.

**Key files:**
- `src/serialization/message.zig` + `src/serialization/message/*` -- core wire format (~2000 LOC)
- `src/serialization/schema.zig` -- schema type definitions (Node, Field, Type, Value)
- `src/serialization/request_reader.zig` -- CodeGeneratorRequest parsing
- `src/capnpc-zig/generator.zig` -- codegen driver
- `src/capnpc-zig/struct_gen.zig` -- struct field accessor generation
- `src/capnpc-zig/types.zig` -- Cap'n Proto to Zig type mapping
- `src/main.zig` -- compiler plugin entrypoint (stdin -> stdout)

---

## Phase 2: Constants, Defaults, and Annotations -- Done

Extended code generation to handle the full breadth of Cap'n Proto schema
features: typed constant values, pointer defaults, annotation uses, and
default-value XOR encoding for all primitive/enum/float/bool fields.

**What was built:**
- Typed value codegen for all Cap'n Proto value kinds (void, bool, integers,
  floats, text, data, enum, struct, list, anyPointer).
- Pointer default generation for struct, list, text, and data fields.
- Annotation support in generated output.
- Default setter XOR semantics matching the Cap'n Proto spec.

**Key tests:**
- `tests/serialization/codegen_test.zig` -- codegen output assertions
- `tests/serialization/codegen_defaults_test.zig` -- default value encoding
- `tests/serialization/codegen_annotations_test.zig` -- annotation handling
- `tests/serialization/codegen_union_group_test.zig` -- union/group generation
- `tests/golden/` -- golden file tests for expected codegen output

---

## Phase 3: Official Test Corpus and Interop Harness -- Done

Integrated the official Cap'n Proto test fixtures and built a cross-language
interop harness to validate wire-format compatibility against reference
implementations.

**What was built:**
- Vendored `capnp_test` official test fixtures as a git submodule.
- Python-generated binary fixtures for single-segment, multi-segment (far
  pointer), and packed encoding.
- Round-trip interop tests: encode in Zig, decode in Python/Go and vice versa.
- Go Cap'n Proto reference (`go-capnp`) as a git submodule for e2e validation.

**Key files and tests:**
- `vendor/ext/capnp_test/` -- official Cap'n Proto test fixtures (submodule)
- `vendor/ext/go-capnp/` -- Go reference implementation (submodule)
- `tests/interop/` -- binary fixtures and fixture generation scripts
- `tests/serialization/interop_test.zig`, `interop_roundtrip_test.zig`
- `tests/serialization/capnp_testdata_test.zig`, `capnp_test_vendor_test.zig`

---

## Phase 4: Schema Validation and Canonicalization -- Done

Added schema-driven validation and canonicalization APIs to verify schema
graphs and produce canonical forms for comparison and hashing.

**What was built:**
- Schema graph validation (structural integrity, type consistency, reference
  resolution).
- Canonicalization for deterministic schema comparison.
- Segment-count and traversal/nesting resource limits for DoS hardening.

**Key files:**
- `src/serialization/schema_validation.zig` -- validation and canonicalization
- `tests/serialization/schema_validation_test.zig`
- `tests/serialization/message_test.zig` -- resource limit and boundary tests

---

## Phase 5: Benchmarks -- Done

Extended benchmark coverage to measure serialization performance across
different message shapes and encoding modes, with regression detection.

**What was built:**
- Packed and unpacked encoding benchmarks.
- RPC ping-pong latency benchmark with configurable iterations and payload.
- Machine-readable JSON output for CI integration.
- Allocation-aware reporting (alloc count and bytes per iteration).
- Regression checker (`tools/bench_check.zig`) with committed baselines and
  a 30% regression threshold enforced in `just ci`.

**Key files:**
- `bench/ping_pong.zig` -- RPC round-trip latency bench
- `bench/packed_unpacked.zig` -- serialization throughput bench
- `bench/alloc_counter.zig` -- allocation tracking allocator
- `bench/baselines.json` -- committed regression baselines

---

## Phase 6: RPC Runtime and Codegen -- Done

Implemented the full Cap'n Proto RPC protocol over TCP using libxev, including
capability lifecycle, promise pipelining, three-party handoff, streaming, and
interface codegen. The runtime is organized into four levels mirroring the
Cap'n Proto spec layers.

### Sub-milestones

**6a. Wire protocol and framing** -- Done
- RPC message type definitions and parsers for all `MessageTag` discriminants.
- Segment-framed message reassembly from byte streams with checked-math
  overflow protection.
- Cap table encoding/decoding with reference counting.

Key files:
- `src/rpc/level0/protocol.zig` -- RPC wire message types
- `src/rpc/level0/framing.zig` -- message framing state machine
- `src/rpc/level0/cap_table.zig` -- export/import capability tracking

**6b. Promise pipeline and promised answers** -- Done
- Promised-answer transforms and queued pipelined-call replay.
- Deep-copy utility for promised-answer op slices.
- Return send helpers for centralized clear/send/free behavior.

Key files:
- `src/rpc/level1/promise_pipeline.zig` -- promise state and transforms
- `src/rpc/level1/peer_promises.zig` -- promise tracking per peer
- `src/rpc/level1/promised_answer_copy.zig` -- op-slice deep copy
- `src/rpc/level1/peer_return_send_helpers.zig` -- return send utilities

**6c. Transport, connection, and runtime** -- Done
- libxev-based async TCP transport (macOS kqueue, Linux io_uring).
- Connection state machine: framing, parsing, dispatch, write scheduling.
- Runtime/listener wrapping the libxev event loop.
- Streaming flow control (`StreamState`, `StreamClient`).
- Worker pool for optional off-loop computation.
- Host-neutral frame pump for WASM environments.

Key files:
- `src/rpc/level2/transport_xev.zig` -- async TCP I/O via libxev
- `src/rpc/level2/connection.zig` -- per-connection state machine
- `src/rpc/level2/runtime.zig` -- event loop and listener
- `src/rpc/level2/stream_state.zig` -- streaming flow control
- `src/rpc/level2/worker_pool.zig` -- optional worker threads
- `src/rpc/level2/host_peer.zig` -- WASM-compatible frame pump

**6d. Peer and full RPC protocol** -- Done
- Inbound/outbound call orchestration with question/answer tables.
- Return handling, exception propagation, and forwarded-call management.
- Capability export, import, release, and reference counting lifecycle.
- `sendResultsTo.yourself` and `sendResultsTo.thirdParty` semantics.
- Bootstrap capability advertisement and resolution.
- Embargoed `Accept` deferral/release via `Disembargo.context.accept`.
- Full level-3 `thirdPartyAnswer` adoption with completion-key correlation.
- Level-4 join-key aggregation/verification.
- Provide/accept/join lifecycle (provision storage, accept lookup, finish
  cleanup).
- Graceful shutdown with drain semantics.

Key files:
- `src/rpc/level3/peer.zig` -- main peer type (public API)
- `src/rpc/level3/peer/peer_dispatch.zig` -- inbound message dispatch
- `src/rpc/level3/peer/call/` -- call target resolution, sending, orchestration
- `src/rpc/level3/peer/return/` -- return frames, dispatch, orchestration
- `src/rpc/level3/peer/forward/` -- forwarded-call management
- `src/rpc/level3/peer/third_party/` -- three-party capability transfer
- `src/rpc/level3/peer/provide/` -- provide/accept/join orchestration
- `src/rpc/level3/payload_remap.zig` -- capability descriptor remapping

**6e. Interface codegen** -- Done
- Generated Server VTables, Client types, and PipelinedClient types per
  interface.
- Typed capability parameter helpers (`setXxxServer`, `setXxxClient`) on
  builders.
- Typed capability resolution (`resolveXxx`) on readers returning typed
  Clients.
- Typed List(Interface) per-element resolution.
- Deferred handler support (`DeferredHandler` + `ReturnSender`).
- Streaming method detection and `StreamClient` / `StreamHandler` generation.
- Interface inheritance (`extends`) with ancestor VTable fields and
  `(interface_id, method_id)` dispatch.

Key tests:
- `tests/serialization/codegen_rpc_nested_test.zig`
- `tests/serialization/codegen_streaming_test.zig`
- `tests/serialization/codegen_generated_runtime_test.zig`

**6f. RPC testing and interop** -- Done
- Unit tests for framing, protocol, cap table encoding, and promise transforms.
- Loopback peer tests for bootstrap, call/return, embargo, three-party handoff,
  join, and cleanup.
- Malformed-frame and deterministic fuzz tests for framing and protocol decode.
- Stress tests for embargo ordering, forwarded-tail races, and high-load
  cleanup.
- Cross-language e2e harness (`tests/e2e/`) with Go, C++, Python, and Rust
  backends, orchestrated by `tools/e2e_runner.zig`.

Key test files:
- `tests/rpc/level0/` -- framing, protocol, cap table tests
- `tests/rpc/level1/` -- promised answer transform, return send helper tests
- `tests/rpc/level2/` -- host peer, transport state/callback, cleanup, worker pool tests
- `tests/rpc/level3/` -- full peer tests, control tests
- `tests/e2e/` -- cross-language e2e harness (Go, C++, Python, Rust)

---

## Phase 7: Production Hardening -- In Progress

With core parity complete, focus has shifted to production readiness. This is
not a single deliverable but ongoing work across several fronts.

### Done
- API contract and error taxonomy documentation (`docs/api_contracts.md`).
- Module stability matrix with semver guidance (`docs/stability.md`).
- Benchmark regression thresholds enforced in CI.
- Allocation-aware benchmark reporting.
- MessageBuilder write-path optimizations (pre-reserved root, contiguous text
  writes, pre-sized output).
- WASM host ABI draft (`src/wasm/capnp_host_abi.zig`, `docs/wasm_host_abi.md`).
- Getting-started guides for serialization and RPC (`docs/getting-started-*.md`).

### Remaining
- Expand containerized cross-language e2e matrix beyond current Go path (C++
  reference server/client flows as a CI hard gate).
- Add long-haul/chaos RPC tests (disconnect, half-close, delayed
  resolve/disembargo ordering under load).
- Lock API stability commitments and migration notes for public runtime/codegen
  entry points as they mature from Experimental to Stable.
- Continue API ergonomics and docs cleanup as features stabilize.

Detailed checklist: `docs/production_parity_checklist.md`

---

## Known Future Work

- **TLS / authentication**: Not in scope for initial release; the runtime
  assumes a trusted transport.
- **Packed RPC streams**: Standard (unpacked) framing only for now.
- **HTTP/WebSocket bridges**: Not planned for initial release.
- **Multi-transport multiplexing**: Single transport per connection currently.
- **Zig package publication**: Tracked in `docs/zig_package_publication.md`.
- **WASM host ABI finalization**: Draft ABI exists; needs real-world host
  validation (Deno, Node, Rust wasmtime, etc.).
