# Windows: recipes assume a POSIX shell. Git Bash (installed with Git for
# Windows) provides `sh`; pinning it here makes `just` use it explicitly
# instead of silently falling back to cmd/powershell semantics.
set windows-shell := ["sh", "-cu"]

# Build the plugin
build:
    zig build

# Build in release mode
release:
    zig build -Doptimize=ReleaseSafe

# Build optional QUIC-enabled targets
build-quic:
    zig build -Dquic=true --summary all

# Build WASM host target
wasm-build:
    zig build wasm-host --summary all

# Run tests
test:
    zig build test --summary all

# Run the RPC ping-pong example
example:
    zig build example-rpc

# Run static hardening gates
hardening:
    zig build hardening

# Run serialization-focused tests (message/codegen/schema/interop)
test-serialization:
    zig build test-serialization --summary all

# Run all RPC tests
test-rpc:
    zig build test-rpc --summary all

# Run resource-budget regression tests
test-resource-budgets:
    zig build test-resource-budgets --summary all

# Run OOM/failing-allocator regression tests
test-oom:
    zig build test-oom --summary all

# Run deterministic hardening fuzz/smoke coverage
test-fuzz-smoke:
    zig build test-fuzz-smoke --summary all

# Run documentation/example smoke coverage
docs-smoke:
    zig build docs-smoke --summary all

# Compile documentation snippet fixtures
test-docs-snippets:
    zig build test-docs-snippets --summary all

# Compile optional QUIC documentation snippet fixtures
test-docs-snippets-quic:
    zig build -Dquic=true test-docs-snippets-quic --summary all

# Run key hardening gates under ReleaseSafe
test-release-safe:
    zig build test-release-safe --summary all

# Run the FULL suite under ReleaseSafe — the mode CI's per-OS job uses.
#
# Not the same thing as `test-release-safe`, which is a ten-binary subset. This
# lane exists because it has now caught two defects nothing else could see: an
# OOM harness that reported a deterministic function as nondeterministic, and a
# dangling `ctx` pointer that segfaults on amd64 while a still-mapped stack page
# hides it on arm64. Neither Debug, ReleaseFast, nor the subset showed either.
# It is in `release-preflight` for that reason: locally green in every other
# mode is not evidence.
test-release-safe-full:
    zig build test -Doptimize=ReleaseSafe --summary all

# Run teardown-heavy RPC suites under ReleaseFast. This is a MEMORY-SAFETY lane,
# not a performance one: ReleaseFast is the only mode that leaves a freed
# pointer intact, so a use-after-free reached from a destructor shows up here
# and nowhere else.
test-release-fast:
    zig build test-release-fast --summary all

# Run raw-frame RPC security e2e tests
test-e2e-security:
    zig build test-e2e-security --summary all

# Run RPC wire framing/protocol tests
test-rpc-wire:
    zig build test-rpc-wire --summary all

# Run RPC capability table tests
test-rpc-caps:
    zig build test-rpc-caps --summary all

# Run RPC promise/pipelining tests
test-rpc-promises:
    zig build test-rpc-promises --summary all

# Run RPC TCP/raw-frame transport tests
test-rpc-transport:
    zig build test-rpc-transport --summary all

# Run RPC peer semantics tests
test-rpc-peer:
    zig build test-rpc-peer --summary all

# Run RPC integration tests
test-rpc-integration:
    zig build test-rpc-integration --summary all

# Run optional QUIC RPC transport tests
test-rpc-quic:
    zig build -Dquic=true test-rpc-quic --summary all

# Run benchmark regression checks
bench-check:
    zig build -Doptimize=ReleaseFast bench-check

# Run the RPC soak harness (chaos + deadline sessions over loopback TCP)
soak seconds="5" workers="4":
    zig build soak -- --seconds {{ seconds }} --workers {{ workers }}

# Build e2e reference images
e2e-build:
    just --justfile tests/e2e/Justfile build

# Run Zig interoperability e2e gate
e2e:
    just --justfile tests/e2e/Justfile test-zig

# Run e2e using the native Zig runner (no Deno dependency)
e2e-zig:
    just --justfile tests/e2e/Justfile test-zig

# Run the no-docker self-interop e2e (zig client vs zig server, all OSes)
e2e-self:
    zig build e2e-self --summary all

# Run e2e without rebuilding docker images
e2e-skip-build:
    just --justfile tests/e2e/Justfile test-skip-build

# Run the C++-first L3 handoff / L4 recon e2e lane
e2e-l3-cpp:
    just --justfile tests/e2e/Justfile test-l3-cpp

# Run the cross-impl L3 HOSTING lane: the C++ reference drives the recipient
# and introducer roles against a capnp-zig two-peer VatC host
e2e-l3-vatc:
    just --justfile tests/e2e/Justfile test-l3-vatc

# Run the Go L3 handoff recon/source-blocker gate
e2e-l3-go:
    just --justfile tests/e2e/Justfile test-l3-go

# Run the Experimental Zig L4 Join runtime expansion gate
e2e-l4-zig:
    just --justfile tests/e2e/Justfile test-l4-zig

# Run e2e harness without requiring Zig hooks (scaffolding mode)
e2e-scaffold:
    just --justfile tests/e2e/Justfile test-scaffold

# Run optional QUIC gates used by CI
ci-quic:
    just build-quic
    just check-quic
    just test-rpc-quic
    just test-docs-snippets-quic
    just test-quic-full

# The FULL suite against the QUIC library ROOT. `-Dquic=true` swaps the root to
# src/lib_quic.zig, and the targeted lanes above never compile the non-QUIC
# suites against it -- that gap hid a missing `canonical` export on the QUIC
# root through an entire release. Mirrors the CI job of the same shape.
test-quic-full:
    zig build -Dquic=true test --summary all

# CI gate (format, compile, docs, tests, QUIC, and interop e2e)
ci:
    just fmt-check
    just check
    just check-evented
    just check-selector
    zig build hardening
    zig build check-api
    zig build api-closure
    zig build test-fuzz-smoke --summary all
    zig build test-resource-budgets --summary all
    zig build test-oom --summary all
    zig build test-e2e-security --summary all
    zig build test-docs-snippets --summary all
    zig build docs-smoke --summary all
    zig build test-release-safe --summary all
    zig build test-release-fast --summary all
    just ci-quic
    just src/rpc/check-rpc
    just check-generated
    zig build test --summary all
    just e2e-self
    just e2e-zig
    just e2e-l3-vatc
    zig build example-rpc

# Regenerate every committed generated artifact and fail if any drifted from
# the current generator/public API. Guards against the recurring "changed the
# generator/API but forgot to regenerate the checked-in files" class that has
# turned CI red more than once. Needs the `capnp` CLI.
check-generated:
    zig build
    cd src/rpc && just gen-rpc
    cd tests/e2e/schemas && capnp compile -o{{justfile_directory()}}/zig-out/bin/capnpc-zig:{{justfile_directory()}}/tests/e2e/zig/generated game_types.capnp bootstrap.capnp game_world.capnp inventory.capnp chat.capnp matchmaking.capnp resolve_disembargo.capnp l3_l4_interop.capnp
    zig build api-snapshot
    just fmt
    # docs/api-snapshot-experimental.txt is deliberately NOT diffed here. It is
    # regenerated on every run by design ("drift here is expected and NEVER fails
    # the gate"), and it records target-dependent detail: `OwnerThreadId.value` is
    # `std.Thread.Id`, which renders u64 on macOS and u32 on Linux, so a committed
    # copy can never match on every OS. The Stable file MUST be target-stable and
    # stays in the diff — `zig build check-api` enforces that on all three tiers.
    git diff --exit-code -- src/rpc/gen tests/e2e/zig/generated docs/api-snapshot.txt || { echo "ERROR: committed generated artifacts are stale — run 'just check-generated' locally and commit the result"; exit 1; }

# Assert the Zig on PATH is the one mise.toml pins — the same check
# .github/actions/setup-zig makes, so a local gate proves the same thing CI's
# does. Deliberately compares PATH against `mise current zig` rather than
# holding a copy of the version: mise.toml stays the single specifier.
#
# This exists because the mismatch is easy to have and invisible without it:
# zvm installs its shim at ~/.zvm/bin/zig, which shadows mise's on PATH, so
# `just release-preflight` can gate a release on a toolchain CI never runs.
# `mise exec -- zig ...` (or putting `$(mise where zig)/bin` first) is the fix.
check-toolchain:
    #!/usr/bin/env bash
    set -euo pipefail
    want="$(mise current zig)"
    have="$(zig version)"
    if [ -z "$want" ]; then
      echo "ERROR: mise.toml pins no zig version ('mise current zig' returned empty)"; exit 1
    fi
    if [ "$want" != "$have" ]; then
      echo "ERROR: mise.toml pins ${want} but PATH resolves ${have}"
      echo "       run: PATH=\"\$(mise where zig)/bin:\$PATH\" just <recipe>"
      exit 1
    fi
    echo "zig ${have} matches the mise.toml pin"

# Complete local release preflight, including heavier CI build/regression jobs
release-preflight:
    just check-toolchain
    just ci
    just test-release-safe-full
    just wasm-build
    just bench-check
    just release

# Alias for the complete local release preflight
preflight: release-preflight

# Create and push an annotated release tag — but refuse when the commit being
# tagged does not already have a green CI run. v0.4.0 was tagged three minutes
# before its own push run went red on four jobs; this recipe is the preventive
# half of that lesson (.github/workflows/release.yml is the detective half).
# Usage: just release-tag 0.5.0 "one-line theme"
release-tag VERSION THEME="":
    just check-toolchain
    test -z "$(git status --porcelain)" || { echo "ERROR: worktree is dirty — commit or stash first"; exit 1; }
    test "$(cat build.zig.zon | sed -n 's/.*\.version = "\(.*\)".*/\1/p')" = "{{VERSION}}" || { echo "ERROR: build.zig.zon version does not match {{VERSION}} — run the RELEASING.md sweep first"; exit 1; }
    zig build docs-smoke
    gh api "repos/:owner/:repo/actions/runs?head_sha=$(git rev-parse HEAD)" --jq '[.workflow_runs[] | select(.name == "CI")] | if length == 0 then "NO_RUN" elif all(.conclusion == "success") then "GREEN" else "RED" end' | grep -qx GREEN || { echo "ERROR: HEAD has no green CI run — push and wait for CI before tagging (see RELEASING.md step 2)"; exit 1; }
    git tag -a "v{{VERSION}}" -m "$(test -n "{{THEME}}" && echo "v{{VERSION}} — {{THEME}}" || echo "v{{VERSION}}")"
    git push origin "v{{VERSION}}"
    @echo "Tagged v{{VERSION}}. Now do RELEASING.md step 6: real zig fetch, record the hash, create the GitHub Release."

# List CI workflow jobs as seen by `act`
act-list:
    act -l

# Run local CI-equivalent jobs with `act` (single runner profile, sequential)

# Excludes benchmark regression job by default since host/container timing is not comparable to CI baseline.
act-ci event="pull_request":
    act {{ event }} --matrix os:ubuntu-latest -j fmt-check
    act {{ event }} --matrix os:ubuntu-latest -j test
    act {{ event }} --matrix os:ubuntu-latest -j evented-check
    act {{ event }} --matrix os:ubuntu-latest -j quic-transport
    act {{ event }} --matrix os:ubuntu-latest -j docs-smoke
    act {{ event }} --matrix os:ubuntu-latest -j hardening
    act {{ event }} --matrix os:ubuntu-latest -j e2e-zig
    act {{ event }} --matrix os:ubuntu-latest -j wasm-build
    act {{ event }} --matrix os:ubuntu-latest -j release-build
    act {{ event }} --matrix os:ubuntu-latest -j release-safe-tests

# Run a single CI job locally with `act` (example: `just act-ci-job test`)
act-ci-job job event="pull_request" matrix="os:ubuntu-latest":
    act {{ event }} --matrix {{ matrix }} -j {{ job }}

# Run benchmark regression check locally under `act` (optional; often noisy on laptops/containers)
act-bench event="pull_request":
    act {{ event }} --matrix os:ubuntu-latest -j bench-check

# Install to a local bin path (defaults to ~/.local/bin)
install dest="${HOME}/.local/bin": release
    mkdir -p "{{ dest }}"
    cp zig-out/bin/capnpc-zig "{{ dest }}/capnpc-zig"

# Install to the first writable directory in PATH
install-path: release
    @set -eu
    @for dir in $(printf '%s' "$PATH" | tr ':' ' '); do \
        if [ -n "$dir" ] && [ -d "$dir" ] && [ -w "$dir" ]; then \
            cp zig-out/bin/capnpc-zig "$dir/capnpc-zig"; \
            echo "Installed capnpc-zig to $dir/capnpc-zig"; \
            exit 0; \
        fi; \
    done; \
    echo "No writable directory found in PATH. Use 'just install <dest>' instead."; \
    exit 1

# Clean build artifacts
clean:
    rm -rf zig-out .zig-cache

# Format code
fmt:
    zig fmt --exclude examples/kvstore/gen --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor --exclude tests/e2e/zig/generated --exclude tests/golden --exclude src/rpc/gen src/ tests/ bench/ tools/ examples/

# Check formatting with the same paths CI uses
fmt-check:
    zig fmt --check --exclude examples/kvstore/gen --exclude examples/kvstore/zig-pkg --exclude examples/kvstore/vendor --exclude tests/e2e/zig/generated --exclude tests/golden --exclude src/rpc/gen src/ tests/ bench/ tools/ examples/

# Check for errors without building
check:
    zig build check

# Check RPC entry points against the explicit Evented Io backend where supported
check-evented:
    zig build -Dio-backend=evented check --summary all

# Execute the RPC e2e over an explicitly selected Io backend. This is the lane
# with teeth: `-Dio-backend` is a []const u8 compared at RUNTIME by
# io_backend.parseKind, so `check` alone analyses all three arms in every
# configuration and a compile check proves nothing about selection. `.threaded`
# is the only selector that can carry RPC today -- std.Io.Evented has no working
# socket vtable upstream at the pinned toolchain (docs/stability.md).
check-selector:
    zig build -Dio-backend=threaded e2e-self --summary all

# Check optional QUIC-enabled build graph
check-quic:
    zig build -Dquic=true check --summary all

# Generate API documentation
docs:
    zig build docs
