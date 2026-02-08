# Stability Matrix

This document describes the stability level of each module in capnpc-zig and
provides guidance for downstream consumers.

## Stability Levels

| Level | Meaning |
|---|---|
| **Stable** | API is settled. Breaking changes follow semver (major bump). Bug fixes and additive changes only within a minor version. |
| **Experimental** | Functional but the API may change across any release. Use at your own risk; pin to an exact version. |
| **Internal** | Implementation detail. Not exported through `src/lib.zig`. May change or be removed without notice. |

## Module Status

### Stable

| Module | Path | Notes |
|---|---|---|
| Wire Format | `src/message.zig`, `src/message/*` | Core serialization: segments, pointers, structs, lists, text, data, packed encoding. Thoroughly tested and interop-validated. |
| Schema Types | `src/schema.zig` | In-memory schema representation (Node, Field, Type, Value). Mirrors the upstream `schema.capnp` definitions. |
| Schema Parsing | `src/request_reader.zig` | Parses `CodeGeneratorRequest` from Cap'n Proto wire format. |
| Schema Validation | `src/schema_validation.zig` | Validates and canonicalizes schema graphs. |
| Code Generation | `src/capnpc-zig/generator.zig`, `src/capnpc-zig/struct_gen.zig`, `src/capnpc-zig/types.zig` | Generates idiomatic Zig Reader/Builder types from `.capnp` schemas. |
| Reader Convenience | `src/reader.zig` | Segment-aware message reader with packed support. |

### Experimental

| Module | Path | Notes |
|---|---|---|
| RPC Runtime | `src/rpc/runtime.zig` | Event loop wrapper over libxev. API will evolve. |
| RPC Connection | `src/rpc/connection.zig` | Transport + framer combination. Under active development. |
| RPC Peer | `src/rpc/peer.zig` | Full RPC peer with question/answer tables, capability lifecycle, and bootstrap. Core design is stabilizing but the public API may still change. |
| RPC Protocol | `src/rpc/protocol.zig` | Wire readers/builders for RPC messages (Call, Return, Resolve, etc.). |
| RPC Capability Table | `src/rpc/cap_table.zig` | Export/import tracking for capabilities. |
| RPC Framing | `src/rpc/framing.zig` | Segment-framed message reassembly from byte streams. |
| RPC Transport (xev) | `src/rpc/transport_xev.zig` | Async TCP I/O via libxev. |
| RPC Host Peer | `src/rpc/host_peer.zig` | Host-neutral detached frame-pump for wasm environments. |
| RPC Payload Remap | `src/rpc/payload_remap.zig` | Capability descriptor remapping for outbound messages. |

### Internal

These modules are not exported through `src/lib.zig` and should not be imported
directly by consumers. They are subject to change without notice.

| Module | Path | Notes |
|---|---|---|
| Peer dispatch | `src/rpc/peer/peer_dispatch.zig` | Inbound message dispatch logic. |
| Peer control | `src/rpc/peer/peer_control.zig` | Peer lifecycle control. |
| Peer cleanup | `src/rpc/peer/peer_cleanup.zig` | Resource cleanup on peer teardown. |
| Peer transport callbacks | `src/rpc/peer/peer_transport_callbacks.zig` | Transport event wiring. |
| Peer transport state | `src/rpc/peer/peer_transport_state.zig` | Transport-level state tracking. |
| Peer call targets | `src/rpc/peer/call/peer_call_targets.zig` | Call target resolution. |
| Peer call sender | `src/rpc/peer/call/peer_call_sender.zig` | Outbound call construction. |
| Peer call orchestration | `src/rpc/peer/call/peer_call_orchestration.zig` | Call lifecycle orchestration. |
| Peer promises | `src/rpc/peer/peer_promises.zig` | Promise pipeline tracking. |
| Peer inbound release | `src/rpc/peer/peer_inbound_release.zig` | Inbound release message handling. |
| Peer embargo accepts | `src/rpc/peer/peer_embargo_accepts.zig` | Embargo/accept flow. |
| Peer cap lifecycle | `src/rpc/peer/peer_cap_lifecycle.zig` | Capability reference counting. |
| Peer outbound control | `src/rpc/peer/peer_outbound_control.zig` | Outbound message control. |
| Peer return frames | `src/rpc/peer/return/peer_return_frames.zig` | Return message framing. |
| Peer return orchestration | `src/rpc/peer/return/peer_return_orchestration.zig` | Return lifecycle. |
| Peer return dispatch | `src/rpc/peer/return/peer_return_dispatch.zig` | Return dispatch logic. |
| Peer return send helpers | `src/rpc/peer/return/peer_return_send_helpers.zig` | Return send utilities. |
| Peer forward orchestration | `src/rpc/peer/forward/peer_forward_orchestration.zig` | Forwarded-call management. |
| Peer forward return callbacks | `src/rpc/peer/forward/peer_forward_return_callbacks.zig` | Forwarded return handling. |
| Peer forwarded return logic | `src/rpc/peer/forward/peer_forwarded_return_logic.zig` | Forwarded return processing. |
| Peer provide/join | `src/rpc/peer/provide/peer_join_state.zig`, `peer_provides_state.zig`, `peer_provide_join_orchestration.zig` | Three-party handoff (provide/accept/join). |
| Peer third-party | `src/rpc/peer/third_party/peer_third_party_adoption.zig`, `peer_third_party_pending.zig`, `peer_third_party_returns.zig` | Third-party capability transfer. |
| Promised answer copy | `src/rpc/promised_answer_copy.zig` | Deep-copy utility for promised answers. |
| RPC mod (core) | `src/rpc/mod_core.zig` | Core RPC re-exports (subset without xev). |
| List readers impl | `src/message/list_readers.zig` | List reader type definitions (re-exported by `message.zig`). |
| List builders impl | `src/message/list_builders.zig` | List builder type definitions (re-exported by `message.zig`). |
| Any pointer impl | `src/message/any_pointer_reader.zig`, `src/message/any_pointer_builder.zig` | AnyPointer impl (re-exported by `message.zig`). |
| Struct builder impl | `src/message/struct_builder.zig` | StructBuilder impl (re-exported by `message.zig`). |
| Clone any pointer | `src/message/clone_any_pointer.zig` | Deep-copy impl (re-exported by `message.zig`). |

## Semver Guidance

capnpc-zig follows [Semantic Versioning 2.0.0](https://semver.org/).

- The current version is **0.1.0**, indicating early development. All public
  APIs may change between 0.x releases.
- Within the 0.x series, **minor** bumps may include breaking changes to
  experimental modules. Stable modules will remain compatible within a minor
  version where possible, with breaking changes clearly documented in the
  changelog.
- Once the project reaches **1.0.0**, the stability levels above will be
  enforced strictly:
  - Breaking changes to **Stable** modules require a major version bump.
  - **Experimental** modules may break in minor releases but will be documented.
  - **Internal** modules carry no compatibility guarantees.

## Recommendations for Consumers

1. **Pin your dependency** to an exact version or commit hash until 1.0.0.
2. **Only import through `src/lib.zig`** (i.e., `@import("capnpc-zig")`).
   Direct imports of internal modules may break at any time.
3. **Expect RPC API churn.** The RPC runtime is under active development.
   If you depend on it, watch the changelog closely.
4. **Wire format and codegen are safe to depend on.** These layers are
   well-tested, interop-validated, and unlikely to see breaking changes.
