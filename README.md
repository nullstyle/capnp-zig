# capnpc-zig

A pure Zig implementation of [Cap'n Proto](https://capnproto.org/) -- a serialization framework and RPC system. Includes a compiler plugin (`capnpc-zig`), a message serialization library, and an RPC runtime built on `std.Io` with a concurrent read/write transport. Targets Zig 0.17-dev.

> **Status (v0.3.0):** serialization, codegen, the `capnpc-zig` plugin, and the
> **two-party RPC core** are **Stable** on a **frozen, CI-gated** public surface
> (`docs/api-snapshot.txt`). The L3 three-party arc, the reflected-cap resolver,
> QUIC, persistence vat-restore, events, and the demoted transport/ctor variants
> remain **Experimental** and may change at any 0.x minor bump. Pre-1.0 — pin an
> exact version. See [`docs/supported-surface.md`](docs/supported-surface.md) for
> the full contract and [`docs/stability.md`](docs/stability.md) for the
> per-module matrix.

## Features

- **Pure Zig Implementation**: No C++ dependencies, targets Zig 0.17-dev
- **Full Serialization Support**: Complete Cap'n Proto wire format including packed encoding and far pointers
- **Zero-Copy Deserialization**: Readers work directly with message bytes
- **Builder Pattern**: Ergonomic API for constructing messages
- **Schema-Driven Code Generation**: Generates idiomatic Zig Reader/Builder types from `.capnp` schemas
- **RPC Runtime**: Cap'n Proto RPC over TCP with capability-based messaging
- **Comprehensive Tests**: Extensive message/codegen/RPC/interop coverage
- **Type Safe**: Leverages Zig's compile-time type system

## Installation

### As a dependency

Fetch a tagged release into your `build.zig.zon` (`zig fetch --save` records the
`.hash`):

```bash
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.3.0
```

Then import `capnpc-zig` (full: serialization + codegen + RPC) or
`capnpc-zig-core` (serialization + codegen only) — see
[docs/build-integration.md](docs/build-integration.md) for the complete
`build.zig` wiring, including running `capnp compile` during your build.

### Prerequisites

- Zig 0.17-dev on `PATH` (minimum declared in `build.zig.zon`; helper tools remain in `mise.toml`)
- Cap'n Proto compiler (`capnp`) - optional, for schema compilation
- `mise` (recommended, for environment management)
- `just` (recommended, for task automation)
- Docker (optional, for local GitHub Actions runs via `act`)

### Building from Source

```bash
# Using just (recommended)
just build

# Or using zig directly
zig build

# Run tests
just test
# or
zig build test --summary all
```

## Toolchain Support

This branch is built and tested with Zig `0.17.0-dev.813+2153f8143` (current
master). Zig 0.16 is no longer a supported target for this branch; downstream
consumers should use a compatible 0.17-dev snapshot until Zig 0.17 stabilizes.
The local `mise.toml` intentionally does not manage Zig because current
development is using a master/zvm-style toolchain rather than a resolvable mise
release tarball; CI installs the same pinned snapshot via `mlugg/setup-zig`.

Linux, macOS, and Windows are all first-class targets and development
operating systems, gated per push in CI. The per-layer platform matrix
(including the few upstream-blocked features) lives in
[docs/stability.md](docs/stability.md#platform-support).

## Usage

### As a Library

Add `capnpc-zig` to your project and use the message serialization API directly:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a message builder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // Allocate a struct with 1 data word and 2 pointer words
    var struct_builder = try builder.allocateStruct(1, 2);

    // Write primitive fields
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);

    // Write text fields
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    // Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize
    var msg = try message.Message.init(allocator, bytes);
    defer msg.deinit();

    const root = try msg.getRootStruct();

    // Read fields
    const value1 = root.readU32(0); // 42
    const value2 = root.readU32(4); // 100
    const text1 = try root.readText(0); // "Hello"
    const text2 = try root.readText(1); // "World"
}
```

### Generated Code Example

For a Cap'n Proto schema like:

```capnp
@0x9eb32e19f86ee174;

struct Person {
  name @0 :Text;
  age @1 :UInt32;
  email @2 :Text;
}
```

The generated Zig code provides:

```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create a Person
    var msg_builder = capnpc.message.MessageBuilder.init(allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setName("Alice");
    person_builder.setAge(30);
    try person_builder.setEmail("alice@example.com");

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize
    var msg = try capnpc.message.Message.init(allocator, bytes);
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    // Access fields
    const name = try person_reader.getName();
    const age = person_reader.getAge();
    const email = try person_reader.getEmail();
}
```

For a canonical `build.zig` codegen + generated-module wiring example, see `docs/build-integration.md`.

## Architecture

The implementation follows a four-layer design, each building on the previous:

### Layer 1: Wire Format

`src/serialization/message.zig` + `src/serialization/message/`

Core Cap'n Proto binary format: segment management, pointer encoding/decoding, struct/list/text/data serialization, packed encoding, and far pointers. Key types: `MessageBuilder`, `Message`, `StructBuilder`, `StructReader`.

### Layer 2: Schema

`src/serialization/schema.zig`, `src/serialization/request_reader.zig`, `src/serialization/schema_validation.zig`

Schema type definitions (Node, Field, Type, Value), `CodeGeneratorRequest` parsing from stdin, and schema validation/canonicalization.

### Layer 3: Code Generation

`src/capnpc-zig/`

Generates idiomatic Zig Reader/Builder types from Cap'n Proto schemas. `generator.zig` is the main driver; `struct_gen.zig` generates field accessors; `types.zig` maps Cap'n Proto types to Zig types.

### Layer 4: RPC Runtime

`src/rpc/`

Cap'n Proto RPC over TCP and optional QUIC using `std.Io` with a concurrent
read/write transport. Organized by domain:

- **Wire** (`src/rpc/wire/`): Message framing and typed RPC wire message readers/builders.
- **Capabilities** (`src/rpc/caps/`): Capability tables, capability pointers, lifecycle helpers, and payload remapping.
- **Promises** (`src/rpc/promises/`): Promised-answer transforms, queued pipelined-call replay, return routing, and return-send helpers.
- **Transport** (`src/rpc/transport/`): Peer-facing binding contracts plus TCP and optional QUIC transport backends.
- **Peer** (`src/rpc/peer/`): Inbound/outbound call orchestration, return handling, capability lifecycle, embargo handling, third-party handoff, and forwarding logic.
- **Integration** (`src/rpc/integration/`): Host-facing adapters such as `HostPeer` and `WorkerPool`.

### Key Data Flows

**Code generation**: stdin (CodeGeneratorRequest) -> `request_reader.parseCodeGeneratorRequest()` -> `Generator.generateFile()` -> `StructGenerator.generate()` -> stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` -> `StructBuilder.write*()` -> `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` -> `Message.getRootStruct()` -> `StructReader.read*()` (zero-copy, reads directly from wire bytes)

**RPC call flow**: Client builds `Call` message -> `Peer` serializes and queues write -> `Transport` sends via write thread -> remote `Connection` frames and parses -> `Peer` dispatches to server implementation -> `Return` message sent back

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `rpc`, `io_backend`

## Project Structure

```
capnpc-zig/
├── src/
│   ├── main.zig                        # Compiler plugin entry point
│   ├── lib.zig                         # Library exports
│   ├── serialization/
│   │   ├── message.zig                 # Wire format: segments, pointers, packing
│   │   ├── message/                    # Sub-modules: struct/list builders & readers,
│   │   │                               #   any-pointer, clone helpers
│   │   ├── schema.zig                  # Schema type definitions (Node, Field, Type, Value)
│   │   ├── reader.zig                  # Convenience re-exports for generated readers
│   │   ├── request_reader.zig          # CodeGeneratorRequest parser
│   │   └── schema_validation.zig       # Schema validation and canonicalization
│   ├── capnpc-zig/
│   │   ├── generator.zig              # Code generation driver
│   │   ├── struct_gen.zig             # Struct field accessor generation
│   │   └── types.zig                  # Cap'n Proto -> Zig type mapping
│   ├── rpc/
│   │   ├── mod.zig                    # RPC public module
│   │   ├── capnp/
│   │   │   └── rpc.capnp             # Canonical RPC schema copy
│   │   ├── wire/                      # Framing and protocol defs
│   │   ├── caps/                      # Cap tables, descriptors, lifecycle helpers
│   │   ├── promises/                  # Promise pipeline and return routing
│   │   ├── transport/                 # Binding, stream state, TCP/QUIC backends
│   │   │   ├── tcp/
│   │   │   └── quic/
│   │   ├── peer/                      # Dispatch, call/return/forward/provide
│   │   │   ├── call/                  #   orchestration, capability lifecycle,
│   │   │   ├── return/                #   embargo, third-party handoff
│   │   │   ├── forward/
│   │   │   ├── provide/
│   │   │   └── third_party/
│   │   └── integration/               # HostPeer and WorkerPool adapters
│   └── wasm/                          # Experimental WASM host ABI
├── tests/
│   ├── serialization/                 # Message, codegen, interop, schema tests
│   ├── rpc/                           # RPC tests organized by domain
│   ├── golden/                        # Golden codegen output (do not format)
│   ├── interop/                       # Cross-language interop fixtures
│   ├── e2e/                           # End-to-end test harness
│   ├── capnp_testdata/                # Official Cap'n Proto test fixtures
│   └── test_schemas/                  # .capnp schemas used by tests
├── docs/                              # Design docs and guides
├── vendor/ext/                        # Vendored submodules (go-capnp, capnp_test)
├── build.zig                          # Zig build configuration
├── build.zig.zon                      # Zig package manifest
├── Justfile                           # Task automation
└── mise.toml                          # Environment configuration
```

## RPC Runtime

The RPC runtime implements the Cap'n Proto RPC protocol over domain-shaped TCP
and optional QUIC transport modules, using `std.Io` with a concurrent read/write
transport layer.

**Status**: Wire format, codegen, interop, and the RPC runtime are complete;
production hardening is ongoing. See `docs/stability.md` for the per-module
stability matrix and `CHANGELOG.md` for what is changing now.
Canonical RPC schema source-of-truth copy: `src/rpc/capnp/rpc.capnp`.
For the public-surface alias cleanup, see
[`docs/rpc-migration-guide.md`](docs/rpc-migration-guide.md).

### Design Highlights

- **Concurrent I/O**: Each connection uses a dedicated writer thread and blocking reads. All runtime types are single-threaded unless explicitly documented.
- **Capability-based security**: Each connection maintains export and import tables tracking capabilities by ID with reference counting. The runtime sends `Release` when a refcount reaches zero.
- **Promise pipelining**: Calls can be pipelined on promised answers before results arrive, reducing round trips.
- **Structured peer orchestration**: The `Peer` type handles the full lifecycle -- call dispatch, return handling, embargo management, capability forwarding, and third-party handoff.
- **Backend-agnostic I/O**: Every socket op flows through `std.Io`, so the runtime is polymorphic over the concrete backend. `std.Io.Threaded` and `std.Io.Evented` are selected through the same helper when Zig exposes them for the target.

### Switchable Io Backend

The RPC runtime accepts a `std.Io` value at every entry point (`rpc.transport.tcp.Listener.init`, `rpc.transport.tcp.Connection.init`, `rpc.transport.tcp.Transport.init`). To centralise backend selection, the library exports `capnpc.io_backend`:

```zig
const capnpc = @import("capnpc-zig");

pub fn main(init: std.process.Init) !void {
    var backend = try capnpc.io_backend.Backend.init(.process_init, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();
    // pass `io` to rpc.transport.tcp.Listener / rpc.transport.tcp.Connection / etc.
}
```

`Backend.init` accepts:

- `.process_init` -- reuse the `std.Io` provided by `std.process.Init`.
- `.threaded` -- explicitly construct a fresh `std.Io.Threaded` (useful for sizing your own thread pool or running multiple isolated I/O instances).
- `.evented` -- construct and own `std.Io.Evented` where Zig exposes it; returns `error.EventedBackendUnsupported` only when `std.Io.Evented == void` for the target.

Bundled RPC executables (`example-rpc`, `e2e-zig-server`, `e2e-zig-client`) read the selection from a build option:

```bash
zig build example-rpc -Dio-backend=process_init   # default
zig build example-rpc -Dio-backend=threaded       # explicit Threaded
zig build example-rpc -Dio-backend=evented        # explicit Evented where supported
```

Use `just check-evented` (or `zig build -Dio-backend=evented check`) as the
supported no-link compile gate for the Evented selector on targets where Zig
exposes `std.Io.Evented`.

Evented socket behavior depends on Zig's platform backend implementation; the
selector itself now lives behind `src/io_backend.zig`.

### Running the RPC Example

```bash
zig build example-rpc
```

### QUIC Transport

The QUIC RPC transport is optional and excluded from normal builds. The
quic-zig dependency is listed in the package manifest for opt-in builds, but
`build.zig` resolves it only with `-Dquic=true`. Enable it when you need
`capnpc.rpc.transport.quic`:

```bash
zig build -Dquic=true check --summary all
zig build -Dquic=true test-rpc-quic --summary all
```

QUIC defaults to baseline mode, which carries the same ordered RPC frame stream
as TCP over a QUIC connection. Native mode is an explicit opt-in on both peers
for QUIC-specific control/data stream routing. For mode selection, helper
module boundaries, and production hardening defaults, see
`docs/quic-transport.md`.

### RPC Benchmarks

```bash
zig build bench-ping-pong -- --iters 10000 --payload 1024
```

## API Reference

### Message Module

#### `MessageBuilder`

Creates Cap'n Proto messages.

- `init(allocator: Allocator) MessageBuilder` - Create a new message builder
- `deinit()` - Free all resources
- `allocateStruct(data_words: u16, pointer_words: u16) !StructBuilder` - Allocate a struct
- `toBytes() ![]const u8` - Serialize to Cap'n Proto wire format

#### `Message`

Reads Cap'n Proto messages.

- `init(allocator: Allocator, data: []const u8) !Message` - Parse a message
- `deinit()` - Free resources
- `getRootStruct() !StructReader` - Get the root struct

#### `StructBuilder`

Builds struct data.

- `writeU8/U16/U32/U64(offset: usize, value: T)` - Write integer fields
- `writeBool(byte_offset: usize, bit_offset: u3, value: bool)` - Write boolean fields
- `writeText(pointer_index: usize, text: []const u8) !void` - Write text fields

#### `StructReader`

Reads struct data.

- `readU8/U16/U32/U64(offset: usize) T` - Read integer fields
- `readBool(byte_offset: usize, bit_offset: u3) bool` - Read boolean fields
- `readText(pointer_index: usize) ![]const u8` - Read text fields

## Testing

The project includes comprehensive tests:

```bash
# Run all tests
just test

# Run broad test groups
zig build test-serialization # Serialization-focused suites
zig build test-rpc           # All RPC suites

# Run RPC suites by domain
zig build test-rpc-wire       # Framing/protocol
zig build test-rpc-caps       # Capability tables
zig build test-rpc-promises   # Promises/pipelining
zig build test-rpc-transport  # TCP/raw-frame transport
zig build test-rpc-peer       # Peer semantics
zig build test-rpc-integration # HostPeer/WorkerPool integration
zig build -Dquic=true test-rpc-quic # Optional QUIC transport

# Run specific focused suites
zig build test-message       # Message tests
zig build test-codegen       # Codegen tests
zig build docs-smoke         # Docs/examples public API smoke checks
zig build test-docs-snippets # Compile documentation snippet fixtures
zig build -Dquic=true test-docs-snippets-quic # Optional QUIC docs snippets
just release-preflight      # Complete local release preflight
just e2e                    # Cross-language interop harness
```

### Run GitHub Actions Locally

Use [`act`](https://github.com/nektos/act) to run `.github/workflows/ci.yml` on your machine.

```bash
# Install toolchain declared in mise config (includes act)
mise install

# List available CI jobs
just act-list

# Run CI workflow locally (default event: pull_request)
just act-ci

# Run a single job
just act-ci-job test

# Optional: run benchmark gate locally
just act-bench
```

Notes:
- The repo `.actrc` maps all matrix runner labels to a Linux container image for local execution.
- The default local container architecture is `linux/arm64` (override per command with `--container-architecture linux/amd64` if needed).
- The repo `.actrc` and `just act-*` tasks pin matrix to `os:ubuntu-latest` for stable local runs.
- Benchmark regression checks are excluded from `just act-ci` by default; run `just act-bench` when you explicitly want that signal.
- Ensure Docker is running before invoking `act`.

### Test Coverage

- Message wire-format encode/decode, pointer resolution, limits, and malformed/fuzz inputs
- Codegen generation/compile/runtime behavior across schema features, including schema-evolution compatibility checks
- RPC protocol, framing, cap-table encoding, peer runtime semantics, and transport failure-path behavior
- Interop validation against reference stacks via the e2e harness

## Performance

The implementation prioritizes:

- **Zero-copy reads**: Readers work directly on message bytes
- **Minimal allocations**: Only allocate for owned data (text, lists)
- **Compile-time safety**: Leverage Zig's type system
- **Inline-friendly**: Small functions suitable for inlining

### Benchmarks

```bash
zig build bench-packed       # Packed encoding benchmark
zig build bench-unpacked     # Unpacked encoding benchmark
zig build bench-ping-pong -- --iters 10000 --payload 1024  # RPC ping-pong
```

## Development

### Available Commands

```bash
# Build
just build

# Run tests
just test

# Run RPC ping-pong example
just example

# Format code
just fmt

# Clean build artifacts
just clean

# Check for compilation errors
just check

# Check Evented Io backend selection where the target supports std.Io.Evented
just check-evented

# Check docs/examples for stale public API names and missing build recipes
zig build docs-smoke

# Compile documentation snippet fixtures
zig build test-docs-snippets

# Generate API docs into zig-out/docs
just docs
```

### Adding New Features

1. Write tests first in `tests/`
2. Implement feature in `src/`
3. Run `just test` to verify
4. Format with `just fmt`

## Implementation Status

Implemented today:
- Full Cap'n Proto message wire format (including packed/unpacked and far pointers)
- Schema-driven code generation via `capnpc-zig`
- RPC protocol/runtime surface with dedicated RPC test suites
- Schema-evolution runtime coverage and expanded transport failure-path tests
- Local benchmark and interop gates (`zig build bench-check`, `just e2e`)

Runtime design and API stability notes live in:
- `docs/release-notes-2026-05-10.md`
- `docs/rpc_runtime_design.md`
- `docs/quic-transport.md`
- `docs/stability.md`

## Dependencies

- **go-capnp** (`vendor/ext/go-capnp/`) -- Go Cap'n Proto reference (git submodule), used by the e2e Go backend
- **capnp_test** (`vendor/ext/capnp_test/`) -- Official Cap'n Proto test fixtures (git submodule)

## Contributing

Contributions are welcome! Please ensure:

- Code is formatted with `zig fmt`
- All tests pass (`zig build test`)
- Docs/examples gates pass when docs or public API examples change
  (`zig build docs-smoke` and `zig build test-docs-snippets`)
- New features include tests
- Documentation is updated

## License

MIT License

## Acknowledgments

- Cap'n Proto project for the excellent serialization format
- Zig community for the amazing language and tooling
- Existing Cap'n Proto implementations for reference

## Support

For issues, questions, or contributions, please open an issue or pull request.
