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

Protocol scenarios:
- `resolve_disembargo`: a promise resolved to a capability the caller hosts,
  and the Disembargo that follows.
- `l3_l4_interop`: Level-3 handoff (C++ only, Zig client only).
- `pass_back`: the client passes a capability it imported from the server
  back in `check()`'s params, and the server must receive its own capability
  (`receiverHosted`); `echo()` covers the same in results. The Zig client
  exports eight capabilities of its own first, so the import ids it passes
  back are also local export ids.
- `pipelined_params`: the client passes the result of an unanswered call as
  another call's param (`receiverAnswer`). The server must resolve it to its
  own capability, and a param pipelined on a failed call must fail with that
  call's exception.

`pass_back` and `pipelined_params` share one schema,
`schemas/cap_passing.capnp`: every backend serves the same `TokenHost` for
both, and the client picks the flow. Both also run Zig to Zig in
`zig build e2e-self` and `e2e-self-unix`.

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

## Unix-domain sockets

`--transport=unix` (`just e2e-unix`) runs the Zig e2e server and client against
the C++ reference over an AF_UNIX socket file, in both directions. The Zig
binaries take `--host unix:/path`; kj's `parseAddress` takes the same form, so
the C++ binaries need no change.

- Both peers run inside one `cpp-rpc` container, started with `--network none`.
  The runner cross-builds the Zig server and client as static musl binaries
  for the image's architecture (`docker image inspect -f '{{.Architecture}}'`:
  `amd64` gives `x86_64-linux-musl`, `arm64` gives `aarch64-linux-musl`) into
  `tests/e2e/.results/unix-zig/` and mounts them at `/zig`.
- Only the C++ backend runs. Go, Python and Rust record
  `SKIP(unix: reference harness TCP-only)`, and `l3_l4_interop` records a SKIP
  because its driver dials TCP only.
- A case whose client cannot connect prints no TAP and reads `FAIL`, never
  `SKIP`. The summary goes to `tests/e2e/.results/summary-unix.json`.

The Zig-to-Zig lane over a socket file needs no docker:
`zig build e2e-self-unix` (Linux and macOS).

## Notes

- Go backend schema name uses `gameworld`; the harness maps `game_world -> gameworld` automatically.
- If Python backend behavior changes, rebuild images to avoid stale schema/parser state:
  `just --justfile tests/e2e/python/Justfile docker-build`.
- Dockerized reference clients are time-bounded (`E2E_TIMEOUT_SEC`, default `20`) to prevent stuck e2e runs.
- The Zig e2e runner also enforces a per-case wall timeout (20s) as a hard guard against hangs.
- Zig server e2e phase reserves an ephemeral local port per schema run to avoid stale `AddressInUse` collisions.
- Zig server e2e phase runs a fresh server per `(schema, backend)` case to avoid cross-backend state bleed.
- Output artifacts are written to `tests/e2e/.results/`.
