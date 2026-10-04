# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is this project?

capnpc-zig is a pure Zig implementation of [Cap'n Proto](https://capnproto.org/) — a serialization framework and RPC system. It includes a compiler plugin (`capnpc-zig`), a message serialization library, and an RPC runtime using `std.Io` with a concurrent read/write transport.

## Project Structure & Module Organization

- `src/` holds the Zig library and plugin entry point.
- `src/serialization/` contains wire-format, schema, and reader/validation modules.
- `src/capnpc-zig/` contains codegen utilities and generators.
- `src/rpc/wire`, `src/rpc/caps`, `src/rpc/promises`, `src/rpc/transport`, `src/rpc/peer`, and `src/rpc/integration` group RPC runtime modules by domain.
- `src/rpc/promises/` contains promise and pipelining primitives shared by peer flows.
- `tests/serialization/` contains serialization-focused suites; `tests/rpc/` contains RPC suites by domain.
- `tests/` also contains support assets; fixture schemas live in `tests/test_schemas/`.
- `build.zig` defines build/test steps; `Justfile` wraps common tasks.
- `zig-out/` and `.zig-cache/` are build artifacts.

## Build & Test Commands

Requires **tagged Zig 0.17**. The exact toolchain is pinned in `mise.toml` — the single version specifier for this repo, used by both CI and local development (`build.zig.zon` carries a floor, not a second pin). Run `mise install` to get it; if you manage Zig with zvm, note its PATH entry wins over mise's shims, so use `mise exec -- zig ...` to match CI exactly.

| Task | Command |
|---|---|
| Build | `zig build` or `just build` |
| Release build | `just release` |
| Run all tests | `zig build test --summary all` or `just test` |
| Format code | `just fmt` (generated bindings and `tests/golden` are fmt clean as the plugin writes them; `just check-generated` enforces it) |
| Check (no link) | `zig build check` or `just check` |
| Evented canary (expected-fail until std fixes Evented) | `zig build check-evented-canary` or `just check-evented` |
| Docs/examples smoke | `zig build docs-smoke` or `just docs-smoke` |
| Docs snippet fixtures | `zig build test-docs-snippets` or `just test-docs-snippets` |
| Run example | `just example` |
| Bootstrap schema compiler | `mise run bootstrap:capnp` |
| Regenerate committed bindings | `mise exec -- just gen` |
| Install plugin | `just install` (copies to `~/.local/bin/`) |

### Individual test suites

- `zig build test-message`, `test-codegen`, `test-integration`, `test-interop`, `test-real-world`, `test-union`, `test-capnp-testdata`, `test-capnp-test-vendor`, `test-schema-validation`, `test-rpc`, `just e2e`
- `just test-serialization` runs serialization-focused suites.
- `just test-rpc`, `just test-rpc-wire`, `just test-rpc-caps`, `just test-rpc-promises`, `just test-rpc-transport`, `just test-rpc-peer`, `just test-rpc-integration`, and `just test-rpc-quic` run RPC suites by domain.
- `zig build test-rpc-wire`, `test-rpc-caps`, `test-rpc-promises`, `test-rpc-transport`, `test-rpc-peer`, `test-rpc-integration`, and `-Dquic=true test-rpc-quic` run focused RPC domain suites.

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

**RPC Runtime** (`src/rpc/`) — Cap'n Proto RPC over TCP with optional QUIC. Socket data I/O flows through `std.Io`, so the runtime is polymorphic over the concrete backend (`std.Io.Threaded` or the process-provided default); the TCP loop still waits in raw `poll(2)`. Public modules are domain-shaped: `wire`, `caps`, `promises`, `events`, `transport`, `peer`, `integration`, `generated`, and `testing`.

### Key data flows

**Code generation**: stdin (CodeGeneratorRequest) → `request_reader.parseCodeGeneratorRequest()` → `Generator.generateFile()` → `StructGenerator.generate()` → stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` → `StructBuilder.write*()` → `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` → `Message.getRootStruct()` → `StructReader.read*()` (zero-copy, reads directly from wire bytes)

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `canonical`, `rpc`, `io_backend`

### Switchable Io Backend

The RPC runtime is polymorphic over `std.Io`. Centralised selection lives in `src/io_backend.zig` (`pub const io_backend` from `src/lib.zig`):

- `Backend.init(.process_init, gpa, init.io)` — reuse the `std.Io` provided by `std.process.Init` (currently `std.Io.Threaded`).
- `Backend.init(.threaded, gpa, _)` — explicitly construct a fresh `std.Io.Threaded`.
- `Backend.init(.evented, gpa, _)` — returns `error.EventedBackendUnsupported` on every target at Zig 0.17.0, because no std evented backend compiles (`io_backend.evented_available = false`).

RPC entry points (`examples/rpc_pingpong.zig`, `tests/e2e/zig/main_{server,client}.zig`) read the kind from the `-Dio-backend=process_init|threaded|evented` build option (default `process_init`) via the `io_backend_options` module wired up in `build.zig`.

`just check-evented` (`zig build check-evented-canary`) passes only while `std.Io.Evented` still fails to compile with the known `processReplacePath` error. When it goes red (or Nightly posts a notice), std was fixed: re-check `evented_available` in `src/io_backend.zig`.

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

Phases 1–6 complete (wire format, builder, codegen, interop, benchmarks, RPC runtime + codegen). Production hardening is ongoing — `docs/stability.md` has the per-module stability matrix; `CHANGELOG.md` tracks current work.

## Tooling & Configuration

- Use the exact Zig and Wasmtime versions in `mise.toml` through `mise exec --`.
- Before running compiler-dependent tests or regeneration, run `mise run bootstrap:capnp`. Repository tooling uses the verified Cap'n Proto WASM compiler and this checkout's generator. When changing compiler invocation, generated fixtures or package preflight, read [docs/capnp-wasm-toolchain.md](docs/capnp-wasm-toolchain.md) for the binary-stream, import-isolation and native-oracle contracts.

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
