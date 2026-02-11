# RPC Runtime Design (libxev)

## Goals
- Full Cap'n Proto RPC protocol compliance (bootstrap, calls, returns, pipelining, capability transfer).
- Production-ready performance: low overhead, backpressure-aware, minimal allocations.
- Integration with the existing `src/serialization/message.zig` wire-format layer and codegen.
- Event-driven, cross-platform IO via libxev.

## Non-Goals (Initial Phase)
- TLS or authentication (assume a trusted transport).
- Multi-transport multiplexing in a single connection.
- HTTP/WebSocket bridges.

## Architecture Overview
The runtime is organized into a small set of components, with strict ownership and lifetime rules:

- `rpc/Runtime` (`src/rpc/level2/runtime.zig`): owns the libxev loop (or attaches to an existing loop), manages connections, and provides a minimal executor to schedule user callbacks without blocking the IO loop.
- `rpc/Connection` (`src/rpc/level2/connection.zig`): per-transport state machine for framing, parsing, dispatch, and write scheduling.
- `rpc/Transport` (`src/rpc/level2/transport_xev.zig`): libxev TCP transport, handling async read/write and exposing buffers to `Connection`.
- `rpc/Protocol` (`src/rpc/level0/protocol.zig`): Cap'n Proto RPC wire message definitions and parsing helpers.
- `rpc/CapTable` (`src/rpc/level0/cap_table.zig`): export/import capability tracking with reference counting and lifetime management.
- `rpc/Peer` (`src/rpc/level3/peer.zig` + `src/rpc/level3/peer/*`): inbound/outbound call orchestration, return handling, and lifecycle dispatch.
- `rpc/Promise Pipeline` (`src/rpc/level1/promise_pipeline.zig`, `src/rpc/level1/peer_promises.zig`): promised-answer transforms and queued pipelined-call replay.

All runtime types are single-threaded unless explicitly documented. The event loop thread owns connections and transport IO.

## libxev Integration
We use libxev’s proactor model:
- `xev.Loop` is the main reactor.
- `xev.TCP` provides async `accept`, `read`, `write`, and `close`.
- `xev.Completion` objects are embedded in connection state and must remain stable while in-flight.

Connection IO pipeline:
1. `Transport` submits a read into a fixed buffer.
2. On completion, `Connection` consumes bytes into a framing parser.
3. Complete frames are parsed into RPC messages (Cap’n Proto message framing).
4. Parsed messages are dispatched to handlers.
5. Outbound messages are serialized and queued; `Transport` submits writes using libxev’s queued write support.

## Framing and Parsing
- RPC messages are Cap’n Proto messages with standard segment framing (and optional packing in future).
- The `Connection` maintains a framing state machine: header parse -> segment sizes -> payload.
- Parsed messages are validated using `schema_validation.zig` where schema information is known (e.g. generated stubs).
- Malformed frames or protocol violations abort the connection.

## Capability Model
- Each connection maintains:
  - `exports`: server-side capabilities this peer can invoke.
  - `imports`: client-side capabilities received from the peer.
- Capabilities are represented by IDs and refcounts. The runtime sends `Release` when the refcount reaches zero.
- `AnyPointer` capability pointers are treated as interface pointers in schema validation and canonicalization.

## Call Flow
Inbound call:
1. `Call` message parsed with target capability ID and method.
2. `Peer` dispatch logic locates server implementation and invokes it.
3. Results are serialized into a `Return` message.
4. Exceptions map to `Return` with an error payload.

Outbound call:
1. Generated client stubs/peer helpers allocate a `QuestionId` and build params.
2. `Connection` sends `Call` and tracks outstanding question state.
3. `Return` resolves the promise and releases temporary capabilities.

## Concurrency and Scheduling
- User handlers run on a lightweight executor to avoid blocking IO. The initial implementation uses:
  - a single-threaded queue processed between loop ticks, or
  - optional worker threads that post results back to the loop.
- All connection state mutations occur on the loop thread.

## Error Handling
- Protocol errors close the connection and fail all in-flight questions.
- Transport errors propagate to `Connection` and trigger cleanup.
- Application errors are serialized as RPC exceptions.

## Current Module Layout
- `src/rpc/level2/runtime.zig`
- `src/rpc/level2/connection.zig`
- `src/rpc/level2/transport_xev.zig`
- `src/rpc/level0/protocol.zig`
- `src/rpc/level0/cap_table.zig`
- `src/rpc/level0/framing.zig`
- `src/rpc/level1/promise_pipeline.zig`
- `src/rpc/level1/peer_promises.zig`
- `src/rpc/level3/peer.zig`

## Test Plan
- Unit tests for framing and state machines.
- Loopback tests with in-process client/server.
- Interop tests against reference backends in the canonical `tests/e2e` harness.

## Open Questions
- Exact mapping of Cap’n Proto RPC protocol types to generated Zig types.
- How to expose higher-level pipelined call ergonomics in generated client stubs.
- Whether to support packed RPC streams in the first iteration.
