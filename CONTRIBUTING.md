# Contributing to capnpc-zig

Thanks for considering a contribution. This document is the practical guide;
[CLAUDE.md](CLAUDE.md) carries the same conventions in agent-readable form,
and [docs/architecture.md](docs/architecture.md) explains how the codebase
fits together.

## Toolchain

- **Zig**: the exact development snapshot is pinned in
  [.github/workflows/ci.yml](.github/workflows/ci.yml) (`mlugg/setup-zig`)
  and the floor is declared in [build.zig.zon](build.zig.zon)
  (`minimum_zig_version`). This branch tracks Zig master; older 0.17-dev
  snapshots will not build.
- **Helper tools** (optional but recommended): `just`, `capnp`, and `mise`
  (`mise install` provides `just`, `go`, and friends; Zig itself is
  deliberately not managed by mise).

## Building and testing

| Task | Command |
|---|---|
| Build | `zig build` |
| All tests | `zig build test --summary all` (or `just test`) |
| Compile-only check | `zig build check` |
| Format | `just fmt` (never raw `zig fmt tests/` — it reformats excluded golden/generated files) |
| Docs/snippets gate | `zig build docs-smoke` |
| Self-interop e2e (no docker, all OSes) | `zig build e2e-self` (or `just e2e-self`) |
| Cross-language e2e | `just e2e` (needs docker, `capnp`, and `go`) |
| Soak harness | `zig build soak -- --seconds 5 --workers 4` |
| Fuzz targets | `zig build test-fuzz` (deterministic) / `--fuzz` (coverage-guided) |
| Windows test compile gate | `zig build check-test-compile -Dtarget=x86_64-windows` |

Run focused suites while iterating (`zig build test-message`,
`test-rpc-peer`, etc. — see CLAUDE.md for the full list), but make sure
`zig build test`, `zig build check`, and `just fmt-check` pass before
opening a PR. CI additionally runs hardening, ReleaseSafe, QUIC, WASM,
e2e, and benchmark-regression jobs.

## Developing on Windows

Windows is a first-class development OS (see the
[platform matrix](docs/stability.md#platform-support)). Quickstart:

- **Zig**: install the pinned master snapshot from
  [ci.yml](.github/workflows/ci.yml) — `zigup`/`scoop install zig-dev`
  or a manual download both work; the version must match exactly.
- **Shell**: install Git for Windows. `just` recipes assume a POSIX
  `sh`, which Git Bash provides (the Justfile pins `windows-shell` to
  it). Run `just` from Git Bash or any shell with `sh` on `PATH`.
- **Line endings**: nothing to configure — `.gitattributes` forces LF
  for text and protects binary fixtures regardless of `core.autocrlf`.
- **Everything except the docker e2e runs natively**: `just test`,
  `just e2e-self`, `just hardening`, `zig build check-api`,
  `zig build soak -- --seconds 5`.
- **Cross-language e2e**: needs Linux reference containers — install
  Docker Desktop (WSL2 backend) and `just e2e` works locally. Hosted
  Windows CI runners cannot do this; CI covers it on ubuntu and runs
  the self-interop e2e on Windows instead.
- **Build speed**: add a Microsoft Defender exclusion for the repo's
  `.zig-cache/` and `zig-out/` directories; real-time scanning of
  compiler outputs dominates build time otherwise.

## Conventions

- `zig fmt` is law; never hand-format.
- Types `UpperCamelCase`, functions/variables `lowerCamelCase`, files
  `snake_case.zig`.
- Tests live in `tests/` as `*_test.zig`, grouped by feature area
  (`tests/serialization/`, `tests/rpc/<domain>/`), and are registered in
  `build.zig`.
- Commits use concise imperative summaries, optionally scoped:
  `message: handle empty segments`.
- Public API changes must update the API snapshot (`zig build
  api-snapshot`) and respect the tiers in [docs/stability.md](docs/stability.md).
- New parser or protocol code handling untrusted bytes needs bounds-checked
  accessors and tests covering malformed input (see
  [docs/security-regression-matrix.md](docs/security-regression-matrix.md)).

## Pull requests

Include a clear summary, the commands you ran, and schema samples if codegen
behavior changes. PRs that change wire-format or RPC behavior should point
at the relevant section of the Cap'n Proto spec.

## Security issues

Do not open public issues for vulnerabilities — see [SECURITY.md](SECURITY.md).
