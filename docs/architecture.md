# Architecture

This document describes the layered architecture of capnpc-zig, a pure Zig
implementation of Cap'n Proto serialization, code generation, and RPC.

## Layer Diagram

```
+-----------------------------------------------------------------------+
|                                                                       |
|  Layer 4: RPC Runtime  (EXPERIMENTAL)              src/rpc/           |
|                                                                       |
|    runtime.zig          Event loop (xev-backed)                       |
|    connection.zig       Connection state machine                      |
|    peer.zig             Call routing, bootstrap, capability lifecycle  |
|    protocol.zig         RPC message types (Call, Return, Resolve, ..) |
|    cap_table.zig        Capability export/import tables               |
|    framing.zig          Segment-framed message reassembly             |
|    transport_xev.zig    Async TCP I/O via libxev                      |
|    host_peer.zig        Host-neutral peer transport (wasm-compatible) |
|    payload_remap.zig    Capability descriptor remapping               |
|                                                                       |
+-----------------------------------------------------------------------+
        |  uses wire format for message encoding/decoding
        v
+-----------------------------------------------------------------------+
|                                                                       |
|  Layer 3: Code Generation                      src/capnpc-zig/        |
|                                                                       |
|    generator.zig        Driver: schema nodes -> Zig source            |
|    struct_gen.zig       Field accessor generation (Reader + Builder)  |
|    types.zig            Cap'n Proto type -> Zig type mapping          |
|                                                                       |
+-----------------------------------------------------------------------+
        |  reads schema nodes; emits code that imports the wire format
        v
+-----------------------------------------------------------------------+
|                                                                       |
|  Layer 2: Schema                                                      |
|                                                                       |
|    schema.zig              Type definitions (Node, Field, Type, Value)|
|    request_reader.zig      Parse CodeGeneratorRequest from stdin      |
|    schema_validation.zig   Validate & canonicalize schema graphs      |
|                                                                       |
+-----------------------------------------------------------------------+
        |  schema types reference wire-format element sizes and IDs
        v
+-----------------------------------------------------------------------+
|                                                                       |
|  Layer 1: Wire Format                          src/message.zig        |
|                                                 src/message/*          |
|                                                                       |
|    message.zig              Segment management, pointer encoding,     |
|                             packing/unpacking, Message & MessageBuilder|
|    message/struct_builder.zig   StructBuilder (write fields)          |
|    message/list_builders.zig    Typed list builders                   |
|    message/list_readers.zig     Typed list readers                    |
|    message/any_pointer_reader.zig   AnyPointer read support           |
|    message/any_pointer_builder.zig  AnyPointer write support          |
|    message/clone_any_pointer.zig    Deep-copy pointers across msgs    |
|                                                                       |
+-----------------------------------------------------------------------+

External dependency: libxev (TCP event loop, used by Layer 4 only)
```

## Key Types by Layer

### Layer 1 -- Wire Format

| Type | Role |
|---|---|
| `Message` | Immutable view over segment-framed bytes; zero-copy reads |
| `MessageBuilder` | Allocates segments and builds messages in wire format |
| `StructReader` | Reads struct data/pointer sections from a `Message` |
| `StructBuilder` | Writes struct fields into a `MessageBuilder` |
| `*ListReader` | Typed list readers (U8, U16, Text, Bool, Struct, ...) |
| `*ListBuilder` | Typed list builders |
| `AnyPointerReader` / `AnyPointerBuilder` | Untyped pointer access |

### Layer 2 -- Schema

| Type | Role |
|---|---|
| `schema.Node` | A schema graph node (file, struct, enum, interface, const, annotation) |
| `schema.Field` | A struct field descriptor (slot or group) |
| `schema.Type` | Cap'n Proto type union (primitives, list, struct, enum, interface, any_pointer) |
| `schema.Value` | Default / constant values |
| `schema.RequestedFile` | A file entry from a CodeGeneratorRequest |

### Layer 3 -- Code Generation

| Type | Role |
|---|---|
| `Generator` | Main driver: takes schema nodes, produces `.zig` source files |
| `StructGenerator` | Generates Reader and Builder types for a single struct |
| `TypeGenerator` | Maps Cap'n Proto types to Zig type expressions |

### Layer 4 -- RPC Runtime

| Type | Role |
|---|---|
| `Runtime` | Owns the xev event loop |
| `Listener` | Accepts inbound TCP connections |
| `Connection` | Combines transport + framer for a single link |
| `Peer` | Full RPC peer: question/answer tables, call routing, bootstrap |
| `HostPeer` | Detached frame-pump wrapper for host-neutral (wasm) environments |
| `cap_table.ExportCap` / `ImportCap` | Capability references |
| `protocol.*` | Wire readers/builders for RPC messages (Call, Return, Resolve, ...) |

## Data Flows

### Serialization (write path)

```
Application code
      |
      v
MessageBuilder.allocateStruct()   -- reserve space in segments
      |
      v
StructBuilder.write*()            -- write field values into data section
      |                              write pointers (text, list, nested struct)
      v
MessageBuilder.toBytes()          -- emit segment-framed wire bytes
```

### Deserialization (read path)

```
Wire bytes (from file, network, stdin)
      |
      v
Message.init(bytes)               -- parse segment table, validate bounds
      |
      v
Message.getRootStruct()           -- return StructReader for root pointer
      |
      v
StructReader.read*()              -- zero-copy field access into wire bytes
```

### Code generation (compiler plugin)

```
capnp compile --output=capnpc-zig
      |
      v
stdin (CodeGeneratorRequest, Cap'n Proto wire format)
      |
      v
request_reader.parseCodeGeneratorRequest()   -- Layer 2: decode schema
      |
      v
Generator.generateFile()                     -- Layer 3: walk nodes
      |
      v
StructGenerator.generate()                   -- emit Reader/Builder types
      |
      v
stdout (.zig source files)
```

### RPC message exchange

```
Application                          Remote peer
    |                                     |
    |-- Peer.sendCall() ----------------->|
    |   (builds Call msg, assigns         |
    |    question ID, remaps caps)        |
    |                                     |
    |<----------- Return / Resolve -------|
    |   (Peer dispatches to               |
    |    QuestionCallback)                |
    |                                     |
    |<----------- inbound Call -----------|
    |   (Peer invokes CallHandler         |
    |    for exported capability)          |
```

## Public API Surface (`src/lib.zig`)

```zig
pub const message            = @import("message.zig");           // Layer 1
pub const schema             = @import("schema.zig");            // Layer 2
pub const reader             = @import("reader.zig");            // Layer 1 convenience
pub const codegen            = @import("capnpc-zig/generator.zig"); // Layer 3
pub const request            = @import("request_reader.zig");    // Layer 2
pub const schema_validation  = @import("schema_validation.zig"); // Layer 2
pub const rpc                = @import("rpc/mod.zig");           // Layer 4
```

## External Dependencies

| Dependency | Used by | Purpose |
|---|---|---|
| libxev | Layer 4 (RPC) | Cross-platform async I/O event loop |
| vendor/ext/go-capnp | Tests / e2e | Go Cap'n Proto reference for interop testing |
| vendor/ext/capnp_test | Tests | Official Cap'n Proto test fixtures |
