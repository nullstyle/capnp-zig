# Architecture

This document describes the layered architecture of capnp-zig, a pure Zig
implementation of Cap'n Proto serialization, code generation, and RPC.

## Layer Diagram

```text
+-----------------------------------------------------------------------+
| Layer 4: RPC Runtime                                  src/rpc/        |
|                                                                       |
|   wire/            Framing and rpc.capnp message readers/builders     |
|   caps/            Capability tables, descriptors, and payload remap   |
|   promises/        Promised-answer and pipelined-call support          |
|   events.zig       Redacted transport-general observer events          |
|   transport/tcp/   TCP listener, connection, and stream transport      |
|   transport/quic/  Optional QUIC baseline/native transports           |
|   peer/            Call routing, bootstrap, lifecycle orchestration    |
|   integration/     HostPeer and WorkerPool host-facing adapters        |
+-----------------------------------------------------------------------+
        | uses generated rpc.capnp bindings and serialization messages
        v
+-----------------------------------------------------------------------+
| Layer 3: Code Generation                         src/capnpc-zig/      |
|                                                                       |
|   generator.zig      Driver: schema nodes -> Zig source               |
|   struct_gen.zig     Field accessor generation                        |
|   brand_fidelity.zig Shared finite-brand eligibility and accounting   |
|   types.zig          Cap'n Proto type -> Zig type mapping             |
+-----------------------------------------------------------------------+
        | reads schema nodes; emits code that imports the runtime
        v
+-----------------------------------------------------------------------+
| Layer 2: Schema                                  src/serialization/    |
|                                                                       |
|   schema.zig              Type definitions                            |
|   request_reader.zig      CodeGeneratorRequest parsing                |
|   type_resolver.zig       Bounded allocation-free brand resolution    |
|   schema_validation.zig   Schema graph validation and canonicalization |
+-----------------------------------------------------------------------+
        | schema types reference wire-format element sizes and IDs
        v
+-----------------------------------------------------------------------+
| Layer 1: Wire Format                             src/serialization/    |
|                                                                       |
|   message.zig             Segment management and pointer encoding      |
|   message/*               Struct/list/text/data readers and builders   |
|   reader.zig              Segment-aware reader convenience helpers     |
+-----------------------------------------------------------------------+
```

Default builds use only Zig std and the vendored test fixtures. QUIC builds are
opt-in with `-Dquic=true`, which resolves `quic-zig` and its BoringSSL support
through `build.zig.zon`. The manifest pins the `quic` package at tag `v0.12.0`;
that archive pins boringssl-zig commit `292c70a`.

## Key Types By Layer

### Layer 1: Wire Format

| Type | Role |
|---|---|
| `Message` | Immutable view over segment-framed bytes; zero-copy reads. |
| `MessageBuilder` | Allocates segments and builds messages in wire format. |
| `StructReader` | Reads struct data/pointer sections from a `Message`. |
| `StructBuilder` | Writes struct fields into a `MessageBuilder`. |
| `*ListReader` / `*ListBuilder` | Typed list accessors. |
| `AnyPointerReader` / `AnyPointerBuilder` | Untyped pointer access. |

### Layer 2: Schema

| Type | Role |
|---|---|
| `schema.Node` | A schema graph node. |
| `schema.Field` | A struct field descriptor. |
| `schema.Type` | Cap'n Proto type union. |
| `schema.Value` | Default or constant values. |
| `schema.RequestedFile` | A file entry from a `CodeGeneratorRequest`. |
| `schema_validation.*WithBrand` | Stable additive validation/canonicalization entry points for a concrete root brand. |

### Layer 3: Code Generation

| Type | Role |
|---|---|
| `Generator` | Main driver from schema nodes to `.zig` source. |
| `StructGenerator` | Generates reader and builder types for a struct. |
| `TypeGenerator` | Maps Cap'n Proto types to Zig type expressions. |
| `CodegenBudget` | Bounds hostile schema expansion, including at most 4096 brand specializations by default. |

### Layer 4: RPC Runtime

The public RPC surface is intentionally grouped by domain:

| Module | Role |
|---|---|
| `rpc.wire` | RPC message framing and generated `rpc.capnp` protocol accessors. |
| `rpc.caps` | Capability descriptors, import/export tables, and payload remapping. |
| `rpc.promises` | Promised-answer pipeline state and transform traversal. |
| `rpc.events` | Redacted observer events shared by transports and peer dispatch. |
| `rpc.transport.tcp` | TCP runtime, listener, connection, and stream transport. |
| `rpc.transport.quic` | Optional QUIC connection, server fanout, baseline/native modes. |
| `rpc.peer` | Full RPC peer facade: questions, answers, bootstrap, calls, returns. |
| `rpc.integration` | `HostPeer` and `WorkerPool` adapters for host applications. |
| `rpc.generated` | Generated bindings for Cap'n Proto's standard RPC schemas. |
| `rpc.testing` | Test-only white-box helpers. |

Generated RPC code imports through these domain modules. Deprecated top-level
aliases such as `rpc.protocol`, `rpc.connection`, `rpc.cap_table`, and
`rpc._internal` are intentionally absent.

## Data Flows

### Serialization Write Path

```text
Application code
      |
      v
MessageBuilder.allocateStruct()
      |
      v
StructBuilder.write*()
      |
      v
MessageBuilder.toBytes()
```

### Deserialization Read Path

```text
Wire bytes
      |
      v
Message.init(bytes)
      |
      v
Message.getRootStruct()
      |
      v
StructReader.read*()
```

### Code Generation

```text
capnp compile --output=capnpc-zig
      |
      v
stdin CodeGeneratorRequest
      |
      v
request_reader.parseCodeGeneratorRequest()
      |
      v
Generator.generateFile()
      |
      v
stdout .zig source files
```

### RPC Message Exchange

```text
Application                          Remote peer
    |                                     |
    |-- Peer.sendCall() ----------------->|
    |                                     |
    |<----------- Return / Resolve -------|
    |                                     |
    |<----------- inbound Call -----------|
    |-- handler Return ------------------>|
```

TCP and QUIC transports both deliver complete standard RPC frames to
`rpc.peer.Peer`. QUIC baseline mode carries those frames over bidirectional
stream 0. QUIC native mode preserves the same frame callback contract while
routing large frames through one-shot unidirectional data streams.

On Windows, QUIC's socket wait is a single cancellable `io.concurrent` UDP
receive. An `Io.Condition` returns the owner thread to timer, wake, close, or
completion handling without moving QUIC state or callbacks to the worker.
Fanout sessions live at stable heap addresses for the lifetime of any attached
`Peer` transport.

## Public API Surface

`src/lib.zig` exports:

```zig
pub const message = @import("serialization/message.zig");
pub const schema = @import("serialization/schema.zig");
pub const reader = @import("serialization/reader.zig");
pub const codegen = @import("capnpc-zig/generator.zig");
pub const request = @import("serialization/request_reader.zig");
pub const schema_validation = @import("serialization/schema_validation.zig");
pub const rpc = @import("rpc/mod.zig");
pub const io_backend = @import("io_backend.zig");
```

Default imports expose serialization, codegen, TCP RPC, and a disabled
`rpc.transport.quic` facade with dependency-free framing helpers. Passing
`-Dquic=true` selects the QUIC-enabled library root and exposes the
quic-zig-backed `rpc.transport.quic` transport.

## External Dependencies

| Dependency | Used by | Purpose |
|---|---|---|
| Zig std | Library/runtime | Serialization, codegen, TCP RPC, and `std.Io` backend selection. |
| `quic-zig` / BoringSSL | Optional QUIC builds | QUIC transport backend. |
| `vendor/ext/go-capnp` | Tests / e2e | Go Cap'n Proto reference for interop testing. |
| `vendor/ext/capnp_test` | Tests | Official Cap'n Proto test fixtures. |
