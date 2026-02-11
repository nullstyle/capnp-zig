# Zig RPC Interop E2E

This directory is the canonical interoperability gate for `capnp-zig`.

The harness is intentionally Zig-centered:
- `zig client -> reference server`
- `reference client -> zig server`

It does **not** run reference-language-vs-reference-language matrix tests as a gate.

## Scenarios

The interoperability scenarios are game-domain RPC contracts:
- `game_world`
- `chat`
- `inventory`
- `matchmaking`

## Reference Backends

Current required backends:
- `cpp` (Cap'n Proto C++ reference stack)
- `go` (`capnproto.org/go/capnp/v3`)
- `python` (`pycapnp`)
- `rust` (`capnp-rpc`)

## Architecture

- Orchestration is implemented in `tools/e2e_runner.zig`.
- `tests/e2e/run_tests.sh` is a thin compatibility shim that delegates to the Zig runner.
- Backend-specific behavior lives in language-local `Justfile`s:
  - `tests/e2e/zig/Justfile`
  - `tests/e2e/go/Justfile`
  - `tests/e2e/cpp/Justfile`
  - `tests/e2e/python/Justfile`
  - `tests/e2e/rust/Justfile`
- Backend Dockerfiles are colocated with each backend:
  - `tests/e2e/go/Dockerfile`
  - `tests/e2e/cpp/Dockerfile`
  - `tests/e2e/python/Dockerfile`
  - `tests/e2e/rust/Dockerfile`

## Zig Hook Contract

By default the runner uses `tests/e2e/zig/Justfile` recipes:

- `client-hook host port schema backend`
- `server-hook host port schema`

Legacy override is still supported with environment variables:

```bash
export E2E_ZIG_CLIENT_CMD='zig build e2e-zig-client -- --host "$E2E_TARGET_HOST" --port "$E2E_TARGET_PORT" --schema "$E2E_SCHEMA"'
export E2E_ZIG_SERVER_CMD='zig build e2e-zig-server -- --host "$E2E_BIND_HOST" --port "$E2E_BIND_PORT" --schema "$E2E_SCHEMA"'
```

## Commands

```bash
export E2E_ZIG_GLOBAL_CACHE_DIR=.zig-global-cache
export ZIG_GLOBAL_CACHE_DIR="$E2E_ZIG_GLOBAL_CACHE_DIR"

# Build reference images
zig run tools/e2e_runner.zig -- --build-only

# Run full Zig interop e2e
zig run tools/e2e_runner.zig --

# Run only Python reference backend
zig run tools/e2e_runner.zig -- --backend=python

# Run only Rust reference backend
zig run tools/e2e_runner.zig -- --backend=rust

# Run e2e using already-built images
zig run tools/e2e_runner.zig -- --skip-build

# Scaffold mode while Zig hooks are not wired yet
zig run tools/e2e_runner.zig -- --allow-missing-hooks

# Or via just recipes
just --justfile tests/e2e/Justfile test
```

## Notes

- Go backend schema name uses `gameworld`; the harness maps `game_world -> gameworld` automatically.
- If Python backend behavior changes, rebuild images to avoid stale schema/parser state:
  `just --justfile tests/e2e/python/Justfile docker-build`.
- Dockerized reference clients are time-bounded (`E2E_TIMEOUT_SEC`, default `20`) to prevent stuck e2e runs.
- The Zig e2e runner also enforces a per-case wall timeout (20s) as a hard guard against hangs.
- Zig server e2e phase reserves an ephemeral local port per schema run to avoid stale `AddressInUse` collisions.
- Zig server e2e phase runs a fresh server per `(schema, backend)` case to avoid cross-backend state bleed.
- Output artifacts are written to `tests/e2e/.results/`.
