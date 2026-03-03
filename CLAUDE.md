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

Requires **Zig 0.16** (use `mise install` to set up toolchain).

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

**RPC Runtime** (`src/rpc/`) — Cap'n Proto RPC over TCP using synchronous POSIX I/O with concurrent read/write transport. Modules: `runtime.zig` (listener/socket helpers), `connection.zig` (state machine), `framing.zig` (message framing), `transport.zig` (concurrent read/write I/O), `protocol.zig` (RPC message types), `cap_table.zig` (capability export/import), `peer.zig` (call routing and bootstrap).

### Key data flows

**Code generation**: stdin (CodeGeneratorRequest) → `request_reader.parseCodeGeneratorRequest()` → `Generator.generateFile()` → `StructGenerator.generate()` → stdout (.zig files)

**Serialization**: `MessageBuilder.allocateStruct()` → `StructBuilder.write*()` → `MessageBuilder.toBytes()`

**Deserialization**: `Message.init(bytes)` → `Message.getRootStruct()` → `StructReader.read*()` (zero-copy, reads directly from wire bytes)

### Public API (`src/lib.zig`)

Exports: `message`, `schema`, `reader`, `codegen`, `request`, `schema_validation`, `rpc`

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

- Target Zig `0.16`. `capnp`, `just`, and `mise` are optional but recommended for local workflows.
- Use `bd` for task tracking (see below).

<!-- BEGIN BEADS INTEGRATION -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- Use bd for ALL task tracking
- Always use `--json` flag for programmatic use
- Link discovered work with `discovered-from` dependencies
- Check `bd ready` before asking "what should I work on?"
- Do NOT create markdown TODO lists
- Do NOT use external issue trackers
- Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

<!-- END BEADS INTEGRATION -->

## Landing the Plane (Session Completion)

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd sync
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
