# Repository Guidelines

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

## Build, Test, and Development Commands
- `just build` builds the project (`zig build`).
- `just release` builds optimized (`zig build -Doptimize=ReleaseSafe`).
- `just test` runs all tests with summary output (`zig build test --summary all`).
- `just test-serialization` runs serialization-focused suites.
- `just test-rpc`, `just test-rpc-level0`, `just test-rpc-level1`, `just test-rpc-level2`, `just test-rpc-level3` run RPC suites by level.
- `zig build test-message`, `zig build test-codegen`, `zig build test-integration`, `zig build test-real-world`, `zig build test-union` run focused suites.
- `zig build test-serialization` runs all non-RPC serialization suites.
- `zig build test-rpc-level0`, `zig build test-rpc-level1`, `zig build test-rpc-level2`, `zig build test-rpc-level3` run cumulative RPC levels.
- `zig build test-capnp-testdata` and `zig build test-capnp-test-vendor` run Cap’n Proto fixture suites.
- `just fmt` formats `src/` and `tests/` (`zig fmt`).
- `just check` compiles without linking (`zig build check`).
- `just example` runs Cap’n Proto compilation using the local plugin (requires `capnp` and `just build`).
- `just install` copies `zig-out/bin/capnpc-zig` to `~/.local/bin/`.
- `zig build bench-ping-pong -- --iters 10000 --payload 1024` runs the ping-pong benchmark.
- `zig build example-rpc` runs the RPC ping-pong example (`examples/rpc_pingpong.zig`).

## Coding Style & Naming Conventions
- Format with `zig fmt`; don’t hand-format.
- Indentation follows Zig defaults (4 spaces, no tabs).
- Types use `UpperCamelCase`, functions/vars use `lowerCamelCase`.
- Files are `snake_case.zig` (examples: `message.zig`, `integration_test.zig`).

## Testing Guidelines
- Tests use Zig’s built-in `test` blocks in `tests/**/*.zig`.
- Name new test files `*_test.zig` and group by area (`tests/serialization` or `tests/rpc/level0..level3`).
- Run `just test` before submitting changes; add targeted tests for new behavior.

## Commit & Pull Request Guidelines
- This repository has no commits yet, so history-based conventions aren't visible.
- Use concise, imperative commit summaries (optionally scoped), e.g. `message: handle empty segments`.
- PRs should include a clear summary, the commands you ran, and any schema samples if codegen behavior changes.

## Tooling & Configuration
- Target Zig `0.15.2`. `capnp`, `just`, and `mise` are optional but recommended for local workflows.
