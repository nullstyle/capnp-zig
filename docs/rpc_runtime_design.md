# RPC Runtime Design

## Goals
- Full Cap'n Proto RPC protocol compliance (bootstrap, calls, returns, pipelining, capability transfer).
- Production-ready performance: low overhead, backpressure-aware, minimal allocations.
- Integration with the existing `src/serialization/message.zig` wire-format layer and codegen.
- Concurrent read/write I/O over `std.Io` with dedicated writer threads. The runtime is polymorphic over the concrete `std.Io` backend so that `std.Io.Threaded` (Zig 0.16) and the eventual `std.Io.Evented` are both supported without protocol changes — see `src/io_backend.zig`.

## Non-Goals (Initial Phase)
- TLS or authentication for the TCP transport (assume a trusted transport).
- Multi-transport multiplexing in a single connection.
- HTTP/WebSocket bridges.

## Architecture Overview
The runtime is organized into a small set of components, with strict ownership and lifetime rules:

- `rpc/Runtime` (`src/rpc/transport/tcp/runtime.zig`): listener and socket helpers for creating TCP connections.
- `rpc/Connection` (`src/rpc/transport/tcp/connection.zig`): per-transport state machine for framing, parsing, dispatch, and write scheduling.
- `rpc/Transport` (`src/rpc/transport/tcp/stream_transport.zig`): concurrent read/write transport, handling blocking I/O and exposing buffers to `Connection`.
- `rpc/quic.Connection` (`src/rpc/transport/quic/connection.zig`): optional nullq-backed QUIC vat session using ALPN `capnp-rpc/1` and baseline bidirectional stream 0.
- `rpc/Protocol` (`src/rpc/wire/protocol.zig`): Cap'n Proto RPC wire message definitions and parsing helpers.
- `rpc/CapTable` (`src/rpc/caps/table.zig`): export/import capability tracking with reference counting and lifetime management.
- `rpc/Peer` (`src/rpc/peer/mod.zig` + `src/rpc/peer/*`): public peer facade, state limits, inbound/outbound call orchestration, return handling, and lifecycle dispatch.
- `rpc/Promise Pipeline` (`src/rpc/promises/pipeline.zig`, `src/rpc/promises/peer_promises.zig`): promised-answer transforms and queued pipelined-call replay.

All runtime types are single-threaded unless explicitly documented. Each connection uses a dedicated writer thread for outbound I/O and blocking reads on the main connection thread.

## Transport
Each connection uses a `Transport` with concurrent read/write I/O:
1. The read side performs blocking reads into a fixed buffer on the connection thread.
2. `Connection` consumes bytes into a framing parser.
3. Complete frames are parsed into RPC messages (Cap’n Proto message framing).
4. Parsed messages are dispatched to handlers.
5. Outbound messages are serialized and enqueued; a dedicated writer thread drains the write queue.

## QUIC Transport
QUIC is opt-in at the build-module boundary. Default builds keep nullq/BoringSSL
out of serialization and TCP-only applications; `rpc.quic` is a disabled facade
that exposes only nullq-free framing helpers and a clear compile-time error for
transport construction. Build with `-Dquic=true` to select the QUIC-enabled
library root, import nullq, and expose the native transport implementation.

The first QUIC transport is intentionally conservative and native to QUIC:

- ALPN is `capnp-rpc/1`.
- One QUIC connection represents one authenticated vat-to-vat RPC session.
- Client-initiated bidirectional stream 0 carries the baseline Cap'n Proto RPC message stream.
- Each RPC message is length-delimited with a 32-bit little-endian byte length, followed by the existing Cap'n Proto RPC message bytes.

This gives the existing peer/protocol layers the same complete-message callback shape as TCP while letting QUIC handle handshake, loss recovery, stream flow control, and connection migration. When QUIC is enabled, the implementation is exposed as `rpc.quic.Connection` and uses the same `Peer.attachConnection` path as the TCP `rpc.connection.Connection`.

The next QUIC-native mode should keep the same ALPN and vat session model, but split traffic across streams: a control stream for bootstrap, exports/imports, disconnects, and routing metadata; data streams for large payloads or streaming method patterns; and DATAGRAM frames only for non-critical sideband telemetry.

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
- `src/rpc/transport/tcp/runtime.zig`
- `src/rpc/transport/tcp/connection.zig`
- `src/rpc/transport/tcp/stream_transport.zig`
- `src/rpc/wire/protocol.zig`
- `src/rpc/caps/table.zig`
- `src/rpc/wire/framing.zig`
- `src/rpc/promises/pipeline.zig`
- `src/rpc/promises/peer_promises.zig`
- `src/rpc/peer/mod.zig`
- `src/rpc/peer/state.zig`
- `src/rpc/peer/errors.zig`
- `src/rpc/peer/transport.zig`
- `src/rpc/peer/dispatch.zig`
- `src/rpc/peer/bootstrap.zig`
- `src/rpc/peer/finish.zig`
- `src/rpc/peer/resolve.zig`
- `src/rpc/peer/disembargo.zig`

## Test Plan
- Unit tests for framing and state machines.
- Loopback tests with in-process client/server.
- Interop tests against reference backends in the canonical `tests/e2e` harness.

## Open Questions
- Exact mapping of Cap’n Proto RPC protocol types to generated Zig types.
- How to expose higher-level pipelined call ergonomics in generated client stubs.
- Whether to support packed RPC streams in the first iteration.
