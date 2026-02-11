# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this project?

capnpc-zig is a pure Zig implementation of [Cap'n Proto](https://capnproto.org/) — a serialization framework and RPC system. It includes a compiler plugin (`capnpc-zig`), a message serialization library, and an in-progress RPC runtime built on libxev.

## Build & Test Commands

Requires **Zig 0.15.2** (use `mise install` to set up toolchain).

| Task | Command |
|---|---|
| Build | `zig build` or `just build` |
| Release build | `just release` |
| Run all tests | `zig build test --summary all` or `just test` |
| Format code | `zig fmt src/ tests/` or `just fmt` |
| Check (no link) | `zig build check` or `just check` |
| Run example | `just example` (requires `capnp` CLI) |

### Individual test suites

`zig build test-message`, `test-codegen`, `test-integration`, `test-interop`, `test-real-world`, `test-union`, `test-capnp-testdata`, `test-capnp-test-vendor`, `test-schema-validation`, `test-rpc`, `just e2e`

### Benchmarks

`zig build bench-ping-pong -- --iters 10000 --payload 1024`
`zig build bench-packed`, `zig build bench-unpacked`

### RPC example

`zig build example-rpc`

## Architecture

Four-layer design, each building on the previous:

**Wire Format** (`src/serialization/message.zig` + `src/serialization/message/*`, ~2000 LOC) — Core Cap'n Proto binary format: segment management, pointer encoding/decoding, struct/list/text/data serialization, packing, far pointers. Key types: `MessageBuilder`, `Message`, `StructBuilder`, `StructReader`.

**Schema** (`src/serialization/schema.zig`, `src/serialization/request_reader.zig`, `src/serialization/schema_validation.zig`) — Schema type definitions (Node, Field, Type, Value), CodeGeneratorRequest parsing from stdin, schema validation and canonicalization.

**Code Generation** (`src/capnpc-zig/`) — Generates idiomatic Zig Reader/Builder types from Cap'n Proto schemas. `generator.zig` is the main driver; `struct_gen.zig` generates field accessors; `types.zig` maps Cap'n Proto types to Zig types.

**RPC Runtime** (`src/rpc/`, IN PROGRESS) — Cap'n Proto RPC over TCP using libxev. Modules: `runtime.zig` (event loop), `connection.zig` (state machine), `framing.zig` (message framing), `transport_xev.zig` (async I/O), `protocol.zig` (RPC message types), `cap_table.zig` (capability export/import), `peer.zig` (call routing and bootstrap).

### Key data flows

**Code generation**: stdin (CodeGeneratorRequest) → `request_reader.parseCodeGeneratorRequest()` → `Generator.generateFile()` → `StructGenerator.generate()` → stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` → `StructBuilder.write*()` → `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` → `Message.getRootStruct()` → `StructReader.read*()` (zero-copy, reads directly from wire bytes)

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `rpc`

## Coding Conventions

- **Format**: Always use `zig fmt`; never hand-format.
- **Types**: `UpperCamelCase`. **Functions/variables**: `lowerCamelCase`. **Files**: `snake_case.zig`.
- **Tests**: Files named `*_test.zig` in `tests/`, using Zig built-in `test` blocks. Group by feature area.
- **Commits**: Concise imperative summaries, optionally scoped (e.g., `message: handle empty segments`).

## Dependencies & Vendored Code

- `libxev` — Event loop library, fetched via `build.zig.zon` URL+hash dependency (used by RPC runtime)
- `vendor/ext/go-capnp/` — Go Cap'n Proto reference (git submodule), used by the e2e Go backend and Cap'n Proto schema tooling
- `vendor/ext/capnp_test/` — Official Cap'n Proto test fixtures (git submodule)

## Current Status

Phases 1–5 complete (wire format, builder, codegen, interop, benchmarks). Phase 6 (RPC runtime + codegen) is in progress — see `PLAN.md` and `docs/rpc_runtime_design.md`.
