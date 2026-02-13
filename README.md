# capnpc-zig

**WARNING: This code was extensively vibed;  It's only for me for now, use at your own risk**

A pure Zig implementation of [Cap'n Proto](https://capnproto.org/) -- a serialization framework and RPC system. Includes a compiler plugin (`capnpc-zig`), a message serialization library, and an RPC runtime built on libxev. Written entirely in Zig 0.15.2.

## Features

- **Pure Zig Implementation**: No C++ dependencies, written entirely in Zig 0.15.2
- **Full Serialization Support**: Complete Cap'n Proto wire format including packed encoding and far pointers
- **Zero-Copy Deserialization**: Readers work directly with message bytes
- **Builder Pattern**: Ergonomic API for constructing messages
- **Schema-Driven Code Generation**: Generates idiomatic Zig Reader/Builder types from `.capnp` schemas
- **RPC Runtime**: Cap'n Proto RPC over TCP using libxev, with capability-based messaging
- **Comprehensive Tests**: Extensive message/codegen/RPC/interop coverage
- **Type Safe**: Leverages Zig's compile-time type system

## Installation

### Prerequisites

- Zig 0.15.2
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

Cap'n Proto RPC over TCP using libxev. Organized by Cap'n Proto RPC levels:

- **Level 0** (`src/rpc/level0/`): Protocol primitives -- message framing, RPC wire message definitions, and capability export/import table with reference counting.
- **Level 1** (`src/rpc/level1/`): Promise pipelining -- promised-answer transforms, queued pipelined-call replay, and return-send helpers.
- **Level 2** (`src/rpc/level2/`): Runtime plumbing -- the libxev event loop and TCP transport, per-connection state machine, host peer management, stream state, and worker pool.
- **Level 3** (`src/rpc/level3/`): Full peer semantics -- inbound/outbound call orchestration, return handling, capability lifecycle, embargo handling, third-party handoff, and forwarding logic.

### Key Data Flows

**Code generation**: stdin (CodeGeneratorRequest) -> `request_reader.parseCodeGeneratorRequest()` -> `Generator.generateFile()` -> `StructGenerator.generate()` -> stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` -> `StructBuilder.write*()` -> `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` -> `Message.getRootStruct()` -> `StructReader.read*()` (zero-copy, reads directly from wire bytes)

**RPC call flow**: Client builds `Call` message -> `Peer` serializes and queues write -> `Transport` sends via libxev -> remote `Connection` frames and parses -> `Peer` dispatches to server implementation -> `Return` message sent back

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `rpc`, `xev`

A secondary `lib_core.zig` provides the same exports without the libxev transport surface, for environments that do not need the async I/O runtime.

## Project Structure

```
capnpc-zig/
├── src/
│   ├── main.zig                        # Compiler plugin entry point
│   ├── lib.zig                         # Full library exports (with xev)
│   ├── lib_core.zig                    # Core library exports (without xev)
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
│   │   ├── mod.zig                    # RPC public module (full)
│   │   ├── mod_core.zig               # RPC public module (no xev)
│   │   ├── capnp/
│   │   │   └── rpc.capnp             # Canonical RPC schema copy
│   │   ├── level0/                    # Framing, protocol defs, cap table
│   │   ├── level1/                    # Promise pipeline, pipelined-call replay
│   │   ├── level2/                    # Runtime, connection, transport, worker pool
│   │   └── level3/                    # Peer dispatch, call/return/forward/provide
│   │       └── peer/                  #   orchestration, capability lifecycle,
│   │           ├── call/              #   embargo, third-party handoff
│   │           ├── return/
│   │           ├── forward/
│   │           ├── provide/
│   │           └── third_party/
│   └── wasm/                          # Experimental WASM host ABI
├── tests/
│   ├── serialization/                 # Message, codegen, interop, schema tests
│   ├── rpc/                           # RPC tests organized by level (0-3)
│   ├── golden/                        # Golden codegen output (do not format)
│   ├── interop/                       # Cross-language interop fixtures
│   ├── e2e/                           # End-to-end test harness
│   ├── capnp_testdata/                # Official Cap'n Proto test fixtures
│   └── test_schemas/                  # .capnp schemas used by tests
├── docs/                              # Design docs and guides
├── vendor/ext/                        # Vendored submodules (go-capnp, capnp_test)
├── build.zig                          # Zig build configuration
├── build.zig.zon                      # Zig package manifest (libxev dep)
├── Justfile                           # Task automation
└── .mise.toml                         # Environment configuration
```

## RPC Runtime

The RPC runtime implements the Cap'n Proto RPC protocol over TCP, using [libxev](https://github.com/kprotty/libxev) as the async I/O backend. It is organized following the Cap'n Proto RPC specification levels.

**Status**: Phase 6 (RPC runtime + codegen) is complete. Phase 7 (production hardening) is in progress. See `PLAN.md` and `docs/rpc_runtime_design.md` for details.
Canonical RPC schema source-of-truth copy: `src/rpc/capnp/rpc.capnp` (integration plan: `docs/rpc-capnp-integration-plan.md`).

### Design Highlights

- **Event-driven I/O**: Built on libxev's proactor model. The event loop thread owns connections and transport I/O. All runtime types are single-threaded unless explicitly documented.
- **Capability-based security**: Each connection maintains export and import tables tracking capabilities by ID with reference counting. The runtime sends `Release` when a refcount reaches zero.
- **Promise pipelining**: Calls can be pipelined on promised answers before results arrive, reducing round trips.
- **Structured peer orchestration**: The `Peer` type handles the full lifecycle -- call dispatch, return handling, embargo management, capability forwarding, and third-party handoff.

### Running the RPC Example

```bash
zig build example-rpc
```

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

# Run RPC suites by Cap'n Proto level (cumulative)
zig build test-rpc-level0    # Framing/protocol/cap-table
zig build test-rpc-level1    # Promises/pipelining
zig build test-rpc-level2    # Runtime plumbing
zig build test-rpc-level3    # Advanced peer semantics (level 3+)

# Run specific focused suites
zig build test-message       # Message tests
zig build test-codegen       # Codegen tests
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

# Format code
just fmt

# Clean build artifacts
just clean

# Check for compilation errors
just check

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

Roadmap and parity tracking live in:
- `ROADMAP.md`
- `docs/production_parity_checklist.md`

## Dependencies

- **libxev** -- Event loop library, fetched via `build.zig.zon` URL+hash dependency (used by RPC runtime)
- **go-capnp** (`vendor/ext/go-capnp/`) -- Go Cap'n Proto reference (git submodule), used by the e2e Go backend
- **capnp_test** (`vendor/ext/capnp_test/`) -- Official Cap'n Proto test fixtures (git submodule)

## Contributing

Contributions are welcome! Please ensure:

- Code is formatted with `zig fmt`
- All tests pass (`zig build test`)
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
