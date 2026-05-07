# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this project?

capnpc-zig is a pure Zig implementation of [Cap'n Proto](https://capnproto.org/) — a serialization framework and RPC system. It includes a compiler plugin (`capnpc-zig`), a message serialization library, and an RPC runtime using synchronous POSIX I/O with a concurrent read/write transport.

## Project Structure & Module Organization

- `src/` holds the Zig library and plugin entry point.
- `src/serialization/` contains wire-format, schema, and reader/validation modules.
- `src/capnpc-zig/` contains codegen utilities and generators.
- `src/rpc/level0`, `src/rpc/level1`, `src/rpc/level2`, and `src/rpc/level3` group RPC runtime modules by Cap'n Proto level.
- `src/rpc/level1/` contains promise and pipelining primitives shared by higher-level peer flows.
- `tests/serialization/` contains serialization-focused suites; `tests/rpc/level0..level3/` contain RPC suites by level.
- `tests/` also contains support assets; fixture schemas live in `tests/test_schemas/`.
- `build.zig` defines build/test steps; `Justfile` wraps common tasks.
- `zig-out/` and `.zig-cache/` are build artifacts.

## Build & Test Commands

Requires **Zig 0.16.0** (pinned in `mise.toml`; run `mise install` to set up the toolchain).

| Task | Command |
|---|---|
| Build | `zig build` or `just build` |
| Release build | `just release` |
| Run all tests | `zig build test --summary all` or `just test` |
| Format code | `zig fmt src/ tests/` or `just fmt` |
| Check (no link) | `zig build check` or `just check` |
| Run example | `just example` (requires `capnp` CLI) |
| Install plugin | `just install` (copies to `~/.local/bin/`) |

### Individual test suites

- `zig build test-message`, `test-codegen`, `test-integration`, `test-interop`, `test-real-world`, `test-union`, `test-capnp-testdata`, `test-capnp-test-vendor`, `test-schema-validation`, `test-rpc`, `just e2e`
- `just test-serialization` runs serialization-focused suites.
- `just test-rpc`, `just test-rpc-level0`, `just test-rpc-level1`, `just test-rpc-level2`, `just test-rpc-level3` run RPC suites by level.
- `zig build test-rpc-level0`, `test-rpc-level1`, `test-rpc-level2`, `test-rpc-level3` run cumulative RPC levels.

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

**RPC Runtime** (`src/rpc/`) — Cap'n Proto RPC over TCP. All socket I/O flows through `std.Io`, so the runtime is polymorphic over the concrete backend (`std.Io.Threaded` today, `std.Io.Evented` once it lands upstream). Modules: `runtime.zig` (listener/socket helpers), `connection.zig` (state machine), `framing.zig` (message framing), `transport.zig` (concurrent read/write I/O), `protocol.zig` (RPC message types), `cap_table.zig` (capability export/import), `peer.zig` (call routing and bootstrap).

### Key data flows

**Code generation**: stdin (CodeGeneratorRequest) → `request_reader.parseCodeGeneratorRequest()` → `Generator.generateFile()` → `StructGenerator.generate()` → stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` → `StructBuilder.write*()` → `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` → `Message.getRootStruct()` → `StructReader.read*()` (zero-copy, reads directly from wire bytes)

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `rpc`, `io_backend`

### Switchable Io Backend

The RPC runtime is polymorphic over `std.Io`. Centralised selection lives in `src/io_backend.zig` (`pub const io_backend` from `src/lib.zig`):

- `Backend.init(.process_init, gpa, init.io)` — reuse the `std.Io` provided by `std.process.Init` (currently `std.Io.Threaded`).
- `Backend.init(.threaded, gpa, _)` — explicitly construct a fresh `std.Io.Threaded`.
- `Backend.init(.evented, gpa, _)` — placeholder for `std.Io.Evented`; returns `error.EventedBackendNotImplemented` until Zig ships it.

RPC entry points (`examples/rpc_pingpong.zig`, `tests/e2e/zig/main_{server,client}.zig`) read the kind from the `-Dio-backend=process_init|threaded|evented` build option (default `process_init`) via the `io_backend_options` module wired up in `build.zig`.

## Coding Conventions

- **Format**: Always use `zig fmt`; never hand-format.
- **Indentation**: Zig defaults (4 spaces, no tabs).
- **Types**: `UpperCamelCase`. **Functions/variables**: `lowerCamelCase`. **Files**: `snake_case.zig`.
- **Tests**: Files named `*_test.zig` in `tests/`, using Zig built-in `test` blocks. Group by feature area.
- **Commits**: Concise imperative summaries, optionally scoped (e.g., `message: handle empty segments`).
- PRs should include a clear summary, the commands you ran, and any schema samples if codegen behavior changes.

## Dependencies & Vendored Code

- `vendor/ext/go-capnp/` — Go Cap'n Proto reference (git submodule), used by the e2e Go backend and Cap'n Proto schema tooling
- `vendor/ext/capnp_test/` — Official Cap'n Proto test fixtures (git submodule)

## Current Status

Phases 1–6 complete (wire format, builder, codegen, interop, benchmarks, RPC runtime + codegen). Phase 7 (production hardening) is in progress — see `docs/rpc_runtime_design.md`.

## Tooling & Configuration

- Target Zig `0.16.0`. `capnp`, `just`, and `mise` are optional but recommended for local workflows.

## Landing the Plane (Session Completion)

**When ending a code-changing work session that owns the current branch**, complete the steps below. Work is not complete until the owned changes are committed and pushed, but do not push unrelated user work from a dirty shared checkout.

**MANDATORY WORKFLOW:**

1. **Run quality gates** (if code changed) - Tests, linters, builds
2. **PUSH OWNED CHANGES TO REMOTE**:
   ```bash
   git status --short
   git pull --rebase
   git push
   git status  # MUST show "up to date with origin"
   ```
3. **Clean up** - Clear stashes, prune remote branches
4. **Verify** - All changes committed AND pushed
5. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until owned changes are pushed.
- Do not push when the worktree contains unrelated edits you do not own.
- If push fails, resolve owned branch issues and retry, or hand off the blocker clearly.
