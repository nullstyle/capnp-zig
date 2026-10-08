# capnpc-zig

A pure Zig implementation of [Cap'n Proto](https://capnproto.org/) -- a serialization framework and RPC system. Includes a compiler plugin (`capnpc-zig`), a message serialization library, and an RPC runtime built on `std.Io` with a concurrent read/write transport. Targets tagged Zig 0.17.

> **Status (v0.22.0):** serialization, codegen, the `capnpc-zig` plugin, and the
> **two-party RPC core** are **Stable** on a **frozen, CI-gated** public surface
> (`docs/api-snapshot.txt`). The L3/L4 three-party arc, the reflected-cap resolver,
> QUIC, persistence vat-restore, events, binary schema reflection, and the demoted transport/ctor variants
> remain **Experimental** and may change at any 0.x minor bump. Pre-1.0 — pin an
> exact version. See [`docs/supported-surface.md`](docs/supported-surface.md) for
> the full contract and [`docs/stability.md`](docs/stability.md) for the
> per-module matrix.

## Features

- **Pure Zig Implementation**: No C++ dependencies, targets tagged Zig 0.17
- **Full Serialization Support**: Complete Cap'n Proto wire format including packed encoding and far pointers
- **Zero-Copy Deserialization**: Readers work directly with message bytes
- **Builder Pattern**: Ergonomic API for constructing messages
- **Schema-Driven Code Generation**: Generates idiomatic Zig Reader/Builder types from `.capnp` schemas
- **Executable Type Fidelity**: Brand-aware schema validation/canonicalization
  and finite typed generic views without removing erased APIs
- **Binary Schema Reflection (Experimental)**: Embedded schema nodes, type and field lookup, and dynamic readers/builders with generic bindings and schema evolution
- **RPC Runtime**: Cap'n Proto RPC over TCP with capability-based messaging
- **Optional QUIC RPC**: Baseline and native modes, including real `Peer` fanout, close-isolation coverage, and an embedded (foreign `quic.app.Driver` host) session seat for ALPN-routed multi-protocol listeners
- **Comprehensive Tests**: Extensive message/codegen/RPC/interop coverage
- **Type Safe**: Leverages Zig's compile-time type system

## Installation

### As a dependency

Fetch a tagged release into your `build.zig.zon` (`zig fetch --save` records the
`.hash`):

```bash
zig fetch --save git+https://github.com/nullstyle/capnp-zig.git#v0.22.0
```

Then import `capnpc-zig` (full: serialization + codegen + RPC) or
`capnpc-zig-core` (serialization + codegen only) — see
[docs/build-integration.md](docs/build-integration.md) for the complete
`build.zig` wiring, including generating code during your build with the plugin
from the same pinned package (`dep.artifact("capnpc-zig")`, never a PATH
binary). That codegen recipe needs v0.19.0 or later.

Upgrading from v0.21.x? [docs/upgrading-to-0.22.0.md](docs/upgrading-to-0.22.0.md)
lists the coordinated set and who should take the QUIC fixes. From v0.20.x,
read [docs/upgrading-to-0.21.0.md](docs/upgrading-to-0.21.0.md) first (its
Breaking changes and migrations); from v0.19.x, also
[docs/upgrading-to-0.20.0.md](docs/upgrading-to-0.20.0.md), which says who
must take the AF_UNIX security fix.

### Prerequisites

- Tagged Zig 0.17 on `PATH` (`mise install` provides the pinned version; the floor is declared in `build.zig.zon`)
- The pinned WASM schema compiler for repository generation and compiler-dependent tests (`mise run bootstrap:capnp`)
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

On Windows, the `just` test recipes serialize build-runner jobs to avoid the
pinned Zig process-inheritance defect. Full-suite recipes compile binaries in
parallel first. The equivalent direct commands are:

```sh
mise exec -- zig build test-compile --summary all
mise exec -- zig build test -j1 --summary all
# Use -Doptimize=ReleaseSafe on both commands for the full safety-enabled suite.
```

Keep these as separate invocations so all compiler processes exit before tests
start. Tests retain their own threads, RPC concurrency, and deadlines. See
[Windows runner evidence](docs/windows-test-runner.md) for the failure mechanism
and the condition for removing this workaround.

## Toolchain Support

The exact Zig toolchain is pinned in `mise.toml` — the single specifier for
both CI and local development, always a tagged release (ziglang.org deletes
dev builds, so a dev pin eventually stops resolving). Read that file for the
current value; it is deliberately not repeated here so it cannot go stale.
`mise install` gets it; CI installs from the same file and asserts the
toolchain on PATH matches it. `build.zig.zon` carries a floor
(`minimum_zig_version`), not a second pin. Zig 0.16 and the 0.17-dev
snapshots are not supported targets for this branch; downstream consumers
should use a tagged Zig 0.17 release.

If you manage Zig with zvm, its PATH entry takes precedence over mise's shims —
use `mise exec -- zig ...` to match CI exactly.

Repository generation and package checks use the Cap'n Proto **2.0-dev WASM
compiler**, pinned by archive, module and manifest hashes in
[`tools/capnp-toolchain.json`](tools/capnp-toolchain.json). Run
`mise run bootstrap:capnp` once, then use `mise exec -- just gen` to regenerate
or `mise exec -- just check-generated` to check committed artifacts. The
bootstrap downloads the compiler-only package; the Python driver invokes the
pinned Wasmtime directly on Linux, macOS and Windows. `mise run check:capnp`
verifies the installed package and runtime. No native schema compiler is
selected through PATH for these commands.

Generation uses this checkout's `capnpc-zig`; package preflight uses the plugin
built from the extracted package. Ordinary library builds still use checked-in
bindings and require no compiler bootstrap. Native C++ interoperability tests
retain their own matching compiler, generator and libraries. See
[the toolchain contract](docs/capnp-wasm-toolchain.md) for commands, import
isolation and the intentional reflection-metadata changes from the former
1.5.0 compiler.

Linux, macOS, and Windows are all first-class targets and development
operating systems, gated per push in CI. The per-layer platform matrix
(including the few upstream-blocked features) lives in
[docs/stability.md](docs/stability.md#platform-support).

## Usage

### As a Library

Add `capnpc-zig` to your project and use the message serialization API directly:

<!-- verbatim-file: tests/docs/readme/library.zig -->
```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
const message = capnpc.message;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create a message builder
    var builder = message.MessageBuilder.init(allocator);
    defer builder.deinit();

    // Allocate a struct with 1 data word and 2 pointer words
    const struct_builder = try builder.allocateStruct(1, 2);

    // Write primitive fields
    struct_builder.writeU32(0, 42);
    struct_builder.writeU32(4, 100);

    // Write text fields
    try struct_builder.writeText(0, "Hello");
    try struct_builder.writeText(1, "World");

    // Serialize to bytes
    const bytes = try builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize (`.{}` keeps the default validation limits)
    var msg = try message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const root = try msg.getRootStruct();

    // Read fields (text slices point into `bytes`; nothing is copied)
    std.debug.assert(root.readU32(0) == 42);
    std.debug.assert(root.readU32(4) == 100);
    std.debug.assert(std.mem.eql(u8, try root.readText(0), "Hello"));
    std.debug.assert(std.mem.eql(u8, try root.readText(1), "World"));
}
```

### Generated Code Example

Take the `Person` struct from the example schema
[`examples/addressbook.capnp`](examples/addressbook.capnp):

<!-- verbatim: examples/addressbook.capnp -->
```capnp
struct Person {
  id @0 :UInt32;
  name @1 :Text;
  email @2 :Text;
  phones @3 :List(PhoneNumber);
  # Raw bytes — e.g. a tiny avatar thumbnail. Exercises the Data path.
  avatar @4 :Data;

  # Exactly one employment status is active at a time (unnamed union).
  union {
    unemployed @5 :Void;
    employer @6 :Text;
    school @7 :Text;
    selfEmployed @8 :Void;
  }

  struct PhoneNumber {
    number @0 :Text;
    type @1 :PhoneType;
  }

  enum PhoneType {
    mobile @0;
    home @1;
    work @2;
  }
}
```

Generate the schema with the plugin from your pinned package and import the
result as `addressbook`. The generated `Person.Builder` and `Person.Reader`
types give you named accessors:

<!-- verbatim-file: tests/docs/readme/generated.zig -->
```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");
// Generated from examples/addressbook.capnp; your build.zig names the module.
const addressbook = @import("addressbook");
const Person = addressbook.Person;

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    // Create a Person
    var msg_builder = capnpc.message.MessageBuilder.init(allocator);
    defer msg_builder.deinit();

    var person_builder = try Person.Builder.init(&msg_builder);
    try person_builder.setId(1);
    try person_builder.setName("Alice");
    try person_builder.setEmail("alice@example.com");

    // Serialize
    const bytes = try msg_builder.toBytes();
    defer allocator.free(bytes);

    // Deserialize
    var msg = try capnpc.message.Message.init(allocator, bytes, .{});
    defer msg.deinit();

    const person_reader = try Person.Reader.init(&msg);

    // Access fields
    std.debug.assert(try person_reader.getId() == 1);
    std.debug.assert(std.mem.eql(u8, try person_reader.getName(), "Alice"));
    std.debug.assert(std.mem.eql(u8, try person_reader.getEmail(), "alice@example.com"));
}
```

For a canonical `build.zig` codegen + generated-module wiring example, see `docs/build-integration.md`.

Generated Builders also support field getters, typed copy setters, clearing,
and `asReader()` with explicit borrowed-reader storage. Concrete generic list
and finite recursive applications have typed `brands()` views in full and
compact profiles. See [the generated API guide](docs/generated-api.md) for an
executable example, strict Text reads, lifetime rules, and remaining RPC limits.

### Reflection (Experimental)

Generated structs, groups, enums, and interfaces expose `capnpSchema`. The
module's `CAPNP_SCHEMA_REQUEST` contains the original binary schema nodes,
including the compiler-provided dependencies, defaults, annotations, and brands.
Load a registry once, resolve a type, and inspect or modify messages by field
name through `capnpc.reflection.DynamicStruct`. Full and core library modules
both export `reflection`.

See [the reflection guide](docs/reflection.md) for an executable example,
ownership rules, schema evolution, and the limits of this initial API.
`--no-reflection` omits this metadata; `--no-manifest` independently controls the
legacy JSON export-name manifest. Use matching generator and runtime revisions
when reflection is enabled.

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

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `canonical`, `reflection`, `rpc`, `io_backend`, `native` (a sans-IO connection and its C ABI, Experimental; [docs/native-abi.md](docs/native-abi.md))

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
│   ├── golden/                        # Golden codegen output (byte-exact, fmt clean)
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
- **Socket data I/O through `std.Io`**: connect, accept, read, and write on RPC sockets go through the `std.Io` you pass in, and backends are selected through one helper (below). Not every call does: on POSIX the TCP run loop waits for readiness in a raw `poll(2)` when wake, ticks, or an idle bound are enabled, the QUIC wake door also polls raw, and the wake channels and `TCP_NODELAY` use raw `socketpair`/`read`/`write`/`getsockname`/`setsockopt`. On an AF_UNIX socket (Linux and macOS) reads are a raw `poll` and `recvmsg` that takes any fds the peer attached, and two process-wide closer threads close those fds and the transport's own sockets with a raw `close`. Those calls block the OS thread whatever backend is selected. That suits `std.Io.Threaded`, the only backend that carries RPC today.

### Switchable Io Backend

The RPC runtime accepts a `std.Io` value at every entry point (`rpc.transport.tcp.Listener.init`, `rpc.transport.tcp.Connection.init`, `rpc.transport.tcp.Transport.init`). To centralise backend selection, the library exports `capnpc.io_backend`:

<!-- verbatim-file: tests/docs/readme/io_backend.zig -->
```zig
const std = @import("std");
const capnpc = @import("capnpc-zig");

pub fn main(init: std.process.Init) !void {
    var backend = try capnpc.io_backend.Backend.init(.process_init, init.gpa, init.io);
    defer backend.deinit();
    const io = backend.io();

    // Every RPC entry point takes `io`. Port 0 asks the OS for a free port.
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var listener = try capnpc.rpc.transport.tcp.Listener.init(init.gpa, io, address, .{});
    defer listener.close();
}
```

`Backend.init` accepts:

- `.process_init` -- reuse the `std.Io` provided by `std.process.Init`.
- `.threaded` -- explicitly construct a fresh `std.Io.Threaded` (useful for sizing your own thread pool or running multiple isolated I/O instances).
- `.evented` -- the selector for an owned `std.Io.Evented`. At Zig 0.17.0 no `std.Io.Evented` compiles, so `src/io_backend.zig` keeps `evented_available = false` and this returns `error.EventedBackendUnsupported` on every target (see below).

Bundled RPC executables (`example-rpc`, `e2e-zig-server`, `e2e-zig-client`) read the selection from a build option:

```bash
zig build example-rpc -Dio-backend=process_init   # default
zig build example-rpc -Dio-backend=threaded       # explicit Threaded
zig build example-rpc -Dio-backend=evented        # currently fails: EventedBackendUnsupported
```

`zig build -Dio-backend=evented check` compiles nothing evented: the selector
returns `error.EventedBackendUnsupported` without naming a backend (see
below). The gate is `just check-evented` (`zig build check-evented-canary`), an
expected-fail canary that references `std.Io.Evented` and stays green only
while that compile fails with the known std defect. When it goes red, re-check
`evented_available` in `src/io_backend.zig`.

**The Evented selector cannot yet carry RPC**, and the reason
is upstream rather than here. At the pinned toolchain (`0.17.0`),
`std.Io.Evented` resolves to `std.Io.Dispatch` on macOS and `std.Io.Uring` on
Linux, and neither compiles: both set `Io.VTable` fields
(`processReplacePath`, `processSpawnPath`) that the VTable dropped, and leave
two new ones (`inheritParentDir`, `inheritParentFile`) unset. So `.evented`
returns `error.EventedBackendUnsupported` on every target instead of
referencing one. Neither had a working socket vtable even before that: Uring
implements only `netBindIp` / `netClose` / `netShutdown`, Dispatch only
`netClose`, and every other entry — including `netListenIp`, `netAccept`, and
`netConnectIp` — is an `...Unavailable` stub. Since every RPC path is
socket-based, neither could carry a real connection. Treat this selector as
plumbing awaiting upstream, not as a supported transport; the selector itself
lives behind `src/io_backend.zig`, and
[`docs/upstream/handoff-zig-fork-evented-processreplacepath.md`](docs/upstream/handoff-zig-fork-evented-processreplacepath.md)
carries the std fix.

### Running the RPC Example

```bash
zig build example-rpc
```

### Unix-Domain Sockets (Experimental)

On Linux and macOS, `capnpc.rpc.transport.unix` runs the same RPC stack over a
socket file. `unix.listen` returns a `tcp.Listener`, so `ServerSession.accept`
serves it unchanged, and `unix.connect` returns a `*tcp.ClientSession`. Other
targets get `error.UnixSocketsUnsupported`.

<!-- verbatim: tests/docs/readme_snippets_test.zig -->
```zig
var listener = try capnpc.rpc.transport.unix.listen(gpa, io, "/run/myapp/rpc.sock", .{});
defer listener.close(); // removes the socket file, releases its lock
const session = try capnpc.rpc.transport.unix.connect(gpa, io, "/run/myapp/rpc.sock", .{});
defer session.deinit();
```

`listen` holds `<path>.lock` for the listener's life, sets the socket file to
mode 0600 before it accepts anything, refuses paths that do not fit `sun_path`
and abstract names, and replaces a stale socket file only with
`.reclaim_stale = true`. Keep the socket in a private (0700) directory. Every
AF_UNIX connection closes any file descriptors a peer attaches, unless both
ends turn on fd passing (`.fd_passing`), which ties fds to capabilities
(`Peer.setExportFd`, `Peer.importFd`). See
[docs/rpc-unix-sockets.md](docs/rpc-unix-sockets.md) for the full contract
and the threat table, and run the examples with `zig build example-rpc-unix`
and `zig build example-rpc-fd`.

The build option `-Dfd-passing=false` (Experimental; a consumer passes
`.@"fd-passing" = false` to `b.dependency`) compiles fd passing, the fd
closer threads and the AF_UNIX transport out, for an embedder that owns its
sockets. `capnpc-zig-core` also compiles for iOS as a static library
(`zig build check-ios`). See
[docs/build-integration.md](docs/build-integration.md#compiling-fd-passing-out--dfd-passing-experimental).

### QUIC Transport

The QUIC RPC transport is optional and excluded from normal builds. The
quic-zig dependency is listed in the package manifest for opt-in builds, but
`build.zig` resolves it only with `-Dquic=true`. Enable it when you need
`capnpc.rpc.transport.quic`:

```bash
zig build -Dquic=true check --summary all
zig build -Dquic=true test-rpc-quic --summary all
zig build -Dquic=true test-rpc-quic-evidence --summary all
```

QUIC defaults to baseline mode, which carries the same ordered RPC frame stream
as TCP over a QUIC connection. Native mode is an explicit opt-in on both peers
for QUIC-specific control/data stream routing. For mode selection, helper
module boundaries, and production hardening defaults, see
`docs/quic-transport.md`.

The native evidence step runs exactly four roots, rejects `SkipZigTest`, and
fails when QUIC is not enabled. Local macOS Debug and ReleaseSafe evidence is
61/61; Windows runtime acceptance remains a hosted gate after this capnp-zig
revision is pushed, so the passing Windows cross-compile is not presented as a
runtime claim.

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

- `init(allocator: Allocator, data: []const u8, options: ValidationOptions) !Message` - Parse and validate a message (`.{}` keeps the default limits)
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
zig build test-rpc-transport  # TCP/Unix/raw-frame transport
zig build test-rpc-unix       # AF_UNIX suites (fd drain, lingering close, listen/connect)
zig build test-rpc-peer       # Peer semantics
zig build test-rpc-integration # HostPeer/WorkerPool integration
zig build -Dquic=true test-rpc-quic # Optional QUIC transport
zig build test-rpc-l4         # Experimental Join leases/lifecycle

# Run specific focused suites
zig build test-message       # Message tests
zig build test-codegen       # Codegen tests
zig build test-schema-fidelity # Brand-aware validation/codegen closure
zig build -Dquic=true test-rpc-quic-evidence # Four-root no-skip QUIC evidence
zig build docs-smoke         # Docs/examples public API smoke checks
zig build test-docs-snippets # Compile documentation snippet fixtures
zig build -Dquic=true test-docs-snippets-quic # Optional QUIC docs snippets
zig build package-preflight # Filtered-package default/core/QUIC consumers
zig build check-ios          # capnpc-zig-core as iOS static libraries (no SDK)
zig build -Dfd-passing=false test # The suite with fd passing compiled out
zig build check-fd-passing-off-symbols # macOS host: no closer symbols with -Dfd-passing=false
zig build check-generated-shape # Frozen shape of generated code (docs/generated-shape.txt)
just check-release-drift vA.B.C X.Y.Z # Snapshot drift since tag vA.B.C, judged for release X.Y.Z
just release-preflight X.Y.Z # Complete local release preflight
just e2e                    # Cross-language interop harness
zig build e2e-l4-zig        # Experimental Zig↔Zig Join attacker/recovery flow
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

# Expected-fail canary: green while std.Io.Evented still fails to compile (Windows checks Linux)
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
